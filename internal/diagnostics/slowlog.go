// Package diagnostics provides slowlog aggregation, pipeline analysis, and
// memory health checks across all Redis cluster nodes.
package diagnostics

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/duydinhle/redis-sentinel-admin/internal/config"
	"github.com/duydinhle/redis-sentinel-admin/internal/sentinel"
	"go.uber.org/zap"
)

// SlowlogEntry is a single SLOWLOG record attributed to a specific cluster node.
type SlowlogEntry struct {
	NodeAddr   string    `json:"node_addr"`
	ID         int64     `json:"id"`
	Timestamp  time.Time `json:"timestamp"`
	DurationUs int64     `json:"duration_us"` // execution time in microseconds
	Args       []string  `json:"args"`
	ClientAddr string    `json:"client_addr,omitempty"`
	ClientName string    `json:"client_name,omitempty"`
}

// evictedSample stores a single evicted_keys snapshot for delta rate calculation.
type evictedSample struct {
	count int64
	at    time.Time
}

// Service exposes diagnostics operations across the cluster.
type Service interface {
	GetSlowlog(ctx context.Context, limit int) ([]SlowlogEntry, error)
	GetPipelineStats(ctx context.Context) ([]PipelineReport, error)
	GetMemory(ctx context.Context) ([]MemoryReport, error)
	GetStaleLocks(ctx context.Context, pattern string, staleThresholdSec int64) ([]LockReport, error)
}

// DiagnosticsService implements Service.
type DiagnosticsService struct {
	cfg         *config.Config
	sentinelSvc sentinel.Service
	logger      *zap.Logger

	// State for eviction rate calculation (guarded by mu).
	mu          sync.Mutex
	lastEvicted map[string]evictedSample
}

// New creates a DiagnosticsService.
func New(cfg *config.Config, svc sentinel.Service, logger *zap.Logger) *DiagnosticsService {
	return &DiagnosticsService{
		cfg:         cfg,
		sentinelSvc: svc,
		logger:      logger,
		lastEvicted: make(map[string]evictedSample),
	}
}

// GetSlowlog pulls SLOWLOG GET from every node, merges all entries, sorts by
// duration descending, and returns the top limit entries.
func (s *DiagnosticsService) GetSlowlog(ctx context.Context, limit int) ([]SlowlogEntry, error) {
	addrs, err := s.sentinelSvc.GetNodeAddresses(ctx)
	if err != nil {
		return nil, fmt.Errorf("get slowlog: %w", err)
	}

	nodes := append([]string{addrs.Master}, addrs.Replicas...)

	var all []SlowlogEntry
	var errs []error
	for _, addr := range nodes {
		entries, err := s.fetchSlowlog(ctx, addr, 128)
		if err != nil {
			s.logger.Warn("slowlog fetch failed", zap.String("node", addr), zap.Error(err))
			errs = append(errs, fmt.Errorf("node %s: %w", addr, err))
			continue
		}
		all = append(all, entries...)
	}

	// Sort by duration descending (slowest first).
	sort.Slice(all, func(i, j int) bool {
		return all[i].DurationUs > all[j].DurationUs
	})
	if len(all) > limit {
		all = all[:limit]
	}
	return all, errors.Join(errs...)
}

// fetchSlowlog lấy tối đa count bản ghi từ slow query log của một node.
//
// Redis command: SLOWLOG GET <count>
//
// Redis ghi vào slowlog mỗi lệnh có thời gian thực thi vượt ngưỡng slowlog-log-slower-than
// (mặc định 10 000 µs). Mỗi bản ghi trả về:
//   - ID         → số thứ tự monotonic trong log của node đó
//   - Time       → thời điểm lệnh bắt đầu chạy (Unix timestamp)
//   - Duration   → thời gian thực thi (microseconds) — đây là trường sắp xếp chính
//   - Args       → các token của lệnh gốc (đã bị cắt ngắn nếu quá dài)
//   - ClientAddr → IP:port của client gửi lệnh
//   - ClientName → tên client (nếu client đã gọi CLIENT SETNAME)
//
// Lý do fetch 128 rồi mới cắt ở GetSlowlog: mỗi node có log riêng, nên cần lấy
// đủ nhiều từ mỗi node trước khi merge + sort toàn cluster.
func (s *DiagnosticsService) fetchSlowlog(ctx context.Context, addr string, count int64) ([]SlowlogEntry, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	client := sentinel.NewDirectClient(addr, s.cfg.RedisPassword)
	defer client.Close()

	entries, err := client.SlowLogGet(ctx, count).Result()
	if err != nil {
		return nil, fmt.Errorf("SLOWLOG GET on %s: %w", addr, sentinel.ErrNodeUnreachable)
	}

	var result []SlowlogEntry
	for _, e := range entries {
		result = append(result, SlowlogEntry{
			NodeAddr:   addr,
			ID:         e.ID,
			Timestamp:  e.Time,
			DurationUs: e.Duration.Microseconds(),
			Args:       e.Args,
			ClientAddr: e.ClientAddr,
			ClientName: e.ClientName,
		})
	}
	return result, nil
}
