// Package keys provides big-key scanning, hot-key detection, and TTL health analysis.
package keys

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/duydinhle/redis-sentinel-admin/internal/config"
	"github.com/duydinhle/redis-sentinel-admin/internal/sentinel"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// KeyReport describes a single key found during a big-key scan.
type KeyReport struct {
	Key        string `json:"key"`
	Type       string `json:"type"`
	SizeBytes  int64  `json:"size_bytes"`
	NodeAddr   string `json:"node_addr"`
	Namespace  string `json:"namespace"` // prefix before first ':'
	TTLSeconds int64  `json:"ttl_seconds"`
}

// NamespaceStat aggregates big-key findings per key namespace prefix.
type NamespaceStat struct {
	Namespace string  `json:"namespace"`
	KeyCount  int64   `json:"key_count"`
	TotalBytes int64  `json:"total_bytes"`
	AvgBytes  float64 `json:"avg_bytes"`
	MaxBytes  int64   `json:"max_bytes"`
}

// KeysService exposes key-level intelligence operations.
type KeysService interface {
	ScanBigKeys(ctx context.Context, thresholdBytes int64, onKey func(KeyReport)) ([]NamespaceStat, error)
	GetHotkeys(ctx context.Context, topN int) ([]HotKeyReport, error)
	GetTTLReport(ctx context.Context) ([]NamespaceTTLReport, error)
}

// Service implements KeysService.
type Service struct {
	cfg         *config.Config
	sentinelSvc sentinel.Service
	logger      *zap.Logger
}

// New creates a Service.
func New(cfg *config.Config, svc sentinel.Service, logger *zap.Logger) *Service {
	return &Service{cfg: cfg, sentinelSvc: svc, logger: logger}
}

const scanBatchSize = 200

// ScanBigKeys performs a non-blocking SCAN across replica nodes (master if none).
// Each key larger than thresholdBytes triggers onKey. Returns per-namespace aggregates.
func (s *Service) ScanBigKeys(ctx context.Context, thresholdBytes int64, onKey func(KeyReport)) ([]NamespaceStat, error) {
	addrs, err := s.sentinelSvc.GetNodeAddresses(ctx)
	if err != nil {
		return nil, fmt.Errorf("scan bigkeys: %w", err)
	}

	// Prefer replicas to avoid load on master.
	targets := addrs.Replicas
	if len(targets) == 0 {
		targets = []string{addrs.Master}
	}

	ns := make(map[string]*NamespaceStat)

	for _, addr := range targets {
		if err := s.scanNode(ctx, addr, thresholdBytes, onKey, ns); err != nil {
			s.logger.Warn("bigkey scan partial failure", zap.String("node", addr), zap.Error(err))
		}
	}

	stats := make([]NamespaceStat, 0, len(ns))
	for _, st := range ns {
		if st.KeyCount > 0 {
			st.AvgBytes = float64(st.TotalBytes) / float64(st.KeyCount)
		}
		stats = append(stats, *st)
	}
	return stats, nil
}

func (s *Service) scanNode(ctx context.Context, addr string, thresholdBytes int64, onKey func(KeyReport), ns map[string]*NamespaceStat) error {
	client := sentinel.NewDirectClient(addr, s.cfg.RedisPassword)
	defer client.Close()

	var cursor uint64
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		scanCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		keys, next, err := client.Scan(scanCtx, cursor, "*", scanBatchSize).Result()
		cancel()
		if err != nil {
			return fmt.Errorf("SCAN on %s: %w", addr, err)
		}

		for _, key := range keys {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			s.inspectKey(ctx, client, addr, key, thresholdBytes, onKey, ns)
		}

		cursor = next
		if cursor == 0 {
			break
		}

		// Brief pause between batches to avoid starving Redis.
		time.Sleep(time.Millisecond)
	}
	return nil
}

func (s *Service) inspectKey(ctx context.Context, client *redis.Client, addr, key string, thresholdBytes int64, onKey func(KeyReport), ns map[string]*NamespaceStat) {
	memCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	// SAMPLES 0 = estimate based on encoding header only, fastest mode.
	sizeBytes, err := client.MemoryUsage(memCtx, key, 0).Result()
	cancel()
	if err != nil || sizeBytes < thresholdBytes {
		return
	}

	typeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	keyType, _ := client.Type(typeCtx, key).Result()
	cancel()

	ttlCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	ttlDur, _ := client.TTL(ttlCtx, key).Result()
	cancel()

	namespace := keyNamespace(key)
	report := KeyReport{
		Key:        key,
		Type:       keyType,
		SizeBytes:  sizeBytes,
		NodeAddr:   addr,
		Namespace:  namespace,
		TTLSeconds: int64(ttlDur.Seconds()),
	}
	if onKey != nil {
		onKey(report)
	}

	st, ok := ns[namespace]
	if !ok {
		st = &NamespaceStat{Namespace: namespace}
		ns[namespace] = st
	}
	st.KeyCount++
	st.TotalBytes += sizeBytes
	if sizeBytes > st.MaxBytes {
		st.MaxBytes = sizeBytes
	}
}

// keyNamespace returns the prefix before the first ':' (e.g. "user" from "user:123").
func keyNamespace(key string) string {
	if idx := strings.IndexByte(key, ':'); idx > 0 {
		return key[:idx]
	}
	return key
}
