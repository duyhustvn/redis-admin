// Package replication tracks per-replica lag trends and advises on resync health.
package replication

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/duydinhle/redis-sentinel-admin/internal/config"
	"github.com/duydinhle/redis-sentinel-admin/internal/sentinel"
	"go.uber.org/zap"
)

// LagSample is one data point in a replica's lag ring buffer.
type LagSample struct {
	LagBytes int64     `json:"lag_bytes"`
	At       time.Time `json:"at"`
}

// ReplicaLag describes the replication state and trend for a single replica.
type ReplicaLag struct {
	NodeAddr       string      `json:"node_addr"`
	MasterOffset   int64       `json:"master_offset"`
	ReplicaOffset  int64       `json:"replica_offset"`
	LagBytes       int64       `json:"lag_bytes"`
	LagTrend       []LagSample `json:"lag_trend"`       // up to 10 most recent samples
	IsCaughtUp     bool        `json:"is_caught_up"`    // lag < 1 MiB
	PromotionScore float64     `json:"promotion_score"` // 0–100, higher = better failover candidate
}

// Service exposes replication diagnostics.
type Service interface {
	GetReplicationLag(ctx context.Context) ([]ReplicaLag, error)
	GetResyncStats(ctx context.Context) ([]ResyncReport, error)
}

const (
	lagRingSize    = 10
	caughtUpThresh = 1 << 20 // 1 MiB
)

// ReplicationService implements Service.
type ReplicationService struct {
	cfg         *config.Config
	sentinelSvc sentinel.Service
	logger      *zap.Logger

	mu      sync.Mutex
	samples map[string][]LagSample // ring buffer keyed by replica addr
}

// New creates a ReplicationService.
func New(cfg *config.Config, svc sentinel.Service, logger *zap.Logger) *ReplicationService {
	return &ReplicationService{
		cfg:         cfg,
		sentinelSvc: svc,
		logger:      logger,
		samples:     make(map[string][]LagSample),
	}
}

// GetReplicationLag fetches INFO replication from the master and every replica,
// computes per-replica lag, and updates the ring buffer.
func (s *ReplicationService) GetReplicationLag(ctx context.Context) ([]ReplicaLag, error) {
	addrs, err := s.sentinelSvc.GetNodeAddresses(ctx)
	if err != nil {
		return nil, fmt.Errorf("get replication lag: %w", err)
	}

	masterOffset, err := s.fetchMasterOffset(ctx, addrs.Master)
	if err != nil {
		return nil, fmt.Errorf("master offset: %w", err)
	}

	var all []ReplicaLag
	var errs []error
	for _, addr := range addrs.Replicas {
		lag, err := s.fetchReplicaLag(ctx, addr, masterOffset)
		if err != nil {
			s.logger.Warn("replica lag fetch failed", zap.String("replica", addr), zap.Error(err))
			errs = append(errs, fmt.Errorf("replica %s: %w", addr, err))
			continue
		}
		s.recordSample(addr, lag.LagBytes)
		lag.LagTrend = s.getTrend(addr)
		lag.PromotionScore = promotionScore(lag.LagBytes)
		all = append(all, lag)
	}
	return all, errors.Join(errs...)
}

// fetchMasterOffset gọi INFO replication trên master để lấy vị trí replication log hiện tại.
//
// Redis command: INFO replication
//
// Field extracted:
//   - master_repl_offset → offset hiện tại của replication backlog trên master.
//     Đây là số byte đã được ghi vào replication stream; replica dùng giá trị này
//     để tính độ trễ (lag = master_repl_offset - slave_repl_offset).
func (s *ReplicationService) fetchMasterOffset(ctx context.Context, addr string) (int64, error) {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	client := sentinel.NewDirectClient(addr, s.cfg.RedisPassword)
	defer client.Close()

	raw, err := client.Info(tctx, "replication").Result()
	if err != nil {
		return 0, fmt.Errorf("INFO replication on master %s: %w", addr, sentinel.ErrNodeUnreachable)
	}
	kv := parseInfo(raw)
	offset, _ := strconv.ParseInt(kv["master_repl_offset"], 10, 64)
	return offset, nil
}

// fetchReplicaLag gọi INFO replication trên replica và tính độ trễ so với master.
//
// Redis command: INFO replication
//
// Field extracted:
//   - slave_repl_offset → offset mà replica đã nhận và áp dụng từ replication stream.
//
// Tính toán:
//   lagBytes = masterOffset - slave_repl_offset
//   Giá trị dương = replica đang bị trễ; âm được clamp về 0 (xảy ra khi replica
//   vừa bắt kịp ngay trước lần đọc master_repl_offset).
func (s *ReplicationService) fetchReplicaLag(ctx context.Context, addr string, masterOffset int64) (ReplicaLag, error) {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	client := sentinel.NewDirectClient(addr, s.cfg.RedisPassword)
	defer client.Close()

	raw, err := client.Info(tctx, "replication").Result()
	if err != nil {
		return ReplicaLag{}, fmt.Errorf("INFO replication on replica %s: %w", addr, sentinel.ErrNodeUnreachable)
	}
	kv := parseInfo(raw)

	replicaOffset, _ := strconv.ParseInt(kv["slave_repl_offset"], 10, 64)
	lagBytes := masterOffset - replicaOffset
	if lagBytes < 0 {
		lagBytes = 0
	}

	return ReplicaLag{
		NodeAddr:      addr,
		MasterOffset:  masterOffset,
		ReplicaOffset: replicaOffset,
		LagBytes:      lagBytes,
		IsCaughtUp:    lagBytes < caughtUpThresh,
	}, nil
}

func (s *ReplicationService) recordSample(addr string, lagBytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ring := s.samples[addr]
	ring = append(ring, LagSample{LagBytes: lagBytes, At: time.Now()})
	if len(ring) > lagRingSize {
		ring = ring[len(ring)-lagRingSize:]
	}
	s.samples[addr] = ring
}

func (s *ReplicationService) getTrend(addr string) []LagSample {
	s.mu.Lock()
	defer s.mu.Unlock()
	src := s.samples[addr]
	out := make([]LagSample, len(src))
	copy(out, src)
	return out
}

// promotionScore converts lag to a 0-100 score (100 = zero lag = best candidate).
func promotionScore(lagBytes int64) float64 {
	const maxLag = 100 * 1 << 20 // 100 MiB cap
	if lagBytes <= 0 {
		return 100
	}
	if lagBytes >= maxLag {
		return 0
	}
	return 100 * (1 - float64(lagBytes)/float64(maxLag))
}

// parseInfo parses key:value\r\n lines returned by Redis INFO.
func parseInfo(raw string) map[string]string {
	out := make(map[string]string)
	for _, line := range splitLines(raw) {
		k, v, ok := cutColon(line)
		if !ok {
			continue
		}
		out[k] = v
	}
	return out
}
