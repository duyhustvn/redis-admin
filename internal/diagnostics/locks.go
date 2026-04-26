package diagnostics

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/duydinhle/redis-sentinel-admin/internal/sentinel"
	"go.uber.org/zap"
)

// LockReport describes a key that may be a stale distributed lock.
type LockReport struct {
	Key        string `json:"key"`
	TTLSeconds int64  `json:"ttl_seconds"` // -1 = no TTL set
	SizeBytes  int64  `json:"size_bytes"`
	NodeAddr   string `json:"node_addr"`
	IsStale    bool   `json:"is_stale"` // true: no TTL or TTL > stale threshold
}

// GetStaleLocks scans keys matching pattern and returns those whose TTL is
// absent or exceeds staleThresholdSec. A threshold of 0 flags only keys with
// no TTL at all.
func (s *DiagnosticsService) GetStaleLocks(ctx context.Context, pattern string, staleThresholdSec int64) ([]LockReport, error) {
	addrs, err := s.sentinelSvc.GetNodeAddresses(ctx)
	if err != nil {
		return nil, fmt.Errorf("get stale locks: %w", err)
	}

	// Scan replicas when available; fall back to master.
	targets := addrs.Replicas
	if len(targets) == 0 {
		targets = []string{addrs.Master}
	}

	var all []LockReport
	var errs []error
	seen := make(map[string]struct{}) // deduplicate across replicas

	for _, addr := range targets {
		locks, err := s.scanLocks(ctx, addr, pattern, staleThresholdSec, seen)
		if err != nil {
			s.logger.Warn("lock scan partial failure", zap.String("node", addr), zap.Error(err))
			errs = append(errs, fmt.Errorf("node %s: %w", addr, err))
			continue
		}
		all = append(all, locks...)
	}
	return all, errors.Join(errs...)
}

func (s *DiagnosticsService) scanLocks(
	ctx context.Context,
	addr, pattern string,
	staleThresholdSec int64,
	seen map[string]struct{},
) ([]LockReport, error) {
	client := sentinel.NewDirectClient(addr, s.cfg.RedisPassword)
	defer client.Close()

	const batchSize = 200
	var result []LockReport
	var cursor uint64

	for {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}

		scanCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		keys, next, err := client.Scan(scanCtx, cursor, pattern, batchSize).Result()
		cancel()
		if err != nil {
			return result, fmt.Errorf("SCAN on %s: %w", addr, err)
		}

		for _, key := range keys {
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}

			ttlCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			ttlDur, err := client.TTL(ttlCtx, key).Result()
			cancel()
			if err != nil {
				continue
			}
			ttlSec := int64(ttlDur.Seconds())

			isStale := ttlSec == -1 ||
				(staleThresholdSec > 0 && ttlSec > staleThresholdSec)
			if !isStale {
				continue
			}

			memCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			sizeBytes, _ := client.MemoryUsage(memCtx, key, 0).Result()
			cancel()

			result = append(result, LockReport{
				Key:        key,
				TTLSeconds: ttlSec,
				SizeBytes:  sizeBytes,
				NodeAddr:   addr,
				IsStale:    true,
			})
		}

		cursor = next
		if cursor == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	return result, nil
}
