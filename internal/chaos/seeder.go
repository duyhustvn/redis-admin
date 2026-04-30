// Package chaos provides tools for testing Redis cluster resilience:
// dummy data seeding, pattern-scoped flushing, and forced failover.
package chaos

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/duydinhle/redis-sentinel-admin/internal/config"
	"github.com/duydinhle/redis-sentinel-admin/internal/sentinel"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	k8sclient "k8s.io/client-go/kubernetes"
)

// SeedResult summarises a completed seed operation.
type SeedResult struct {
	KeysCreated int64  `json:"keys_created"`
	Prefix      string `json:"prefix"`
	KeyType     string `json:"key_type"`
	ElapsedMs   int64  `json:"elapsed_ms"`
}

// FlushResult summarises a completed pattern-scoped flush.
type FlushResult struct {
	KeysDeleted int64  `json:"keys_deleted"`
	Pattern     string `json:"pattern"`
	ElapsedMs   int64  `json:"elapsed_ms"`
}

// Service exposes chaos operations.
type Service interface {
	Seed(ctx context.Context, prefix string, count, valueSize int, keyType string, ttlSec int64) (*SeedResult, error)
	Flush(ctx context.Context, pattern string) (*FlushResult, error)
	TriggerChaosFailover(ctx context.Context, mode, podNamespace, podName string) (*ChaosFailoverResult, error)
}

// ChaosService implements Service.
type ChaosService struct {
	cfg         *config.Config
	sentinelSvc sentinel.Service
	k8sClient   k8sclient.Interface // nil when K8s unavailable
	logger      *zap.Logger
}

// New creates a ChaosService. k8sClient may be nil; pod-delete mode will be
// unavailable in that case.
func New(cfg *config.Config, svc sentinel.Service, k8sClient k8sclient.Interface, logger *zap.Logger) *ChaosService {
	return &ChaosService{
		cfg:         cfg,
		sentinelSvc: svc,
		k8sClient:   k8sClient,
		logger:      logger,
	}
}

const seedPipelineBatch = 100

// Seed tạo count key giả trên master để phục vụ chaos testing và load simulation.
//
// Redis commands (qua Pipeline, flush mỗi 100 lệnh):
//   string: SET  <prefix><i> <value>
//   hash:   HSET <prefix><i> field <value>
//   list:   RPUSH <prefix><i> <value>
//   set:    SADD  <prefix><i> <value>
//   zset:   ZADD  <prefix><i> <score> <member>
//   + EXPIRE <key> <ttlSec>  (nếu ttlSec > 0, append ngay sau lệnh write)
//
// Pipeline giảm số round-trip từ N xuống ceil(N/100) — thay vì gửi từng lệnh
// một, gom thành batch rồi gửi cùng lúc và đọc toàn bộ response trong một lần.
// Supported types: string (default), hash, list, set, zset.
// ttlSec == 0 means no expiry.
func (s *ChaosService) Seed(ctx context.Context, prefix string, count, valueSize int, keyType string, ttlSec int64) (*SeedResult, error) {
	if prefix == "" {
		prefix = "test:"
	}
	if count <= 0 {
		count = 100
	}
	if valueSize <= 0 {
		valueSize = 64
	}
	if keyType == "" {
		keyType = "string"
	}

	start := time.Now()
	client := sentinel.NewMasterClient(s.cfg)
	defer client.Close()

	var created int64
	pipe := client.Pipeline()

	flush := func() error {
		if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
			return fmt.Errorf("pipeline exec: %w", err)
		}
		pipe = client.Pipeline()
		return nil
	}

	for i := 0; i < count; i++ {
		if ctx.Err() != nil {
			break
		}
		key := fmt.Sprintf("%s%d", prefix, i)
		val := randomValue(valueSize)
		ttl := time.Duration(ttlSec) * time.Second

		switch keyType {
		case "hash":
			pipe.HSet(ctx, key, "field", val)
		case "list":
			pipe.RPush(ctx, key, val)
		case "set":
			pipe.SAdd(ctx, key, val)
		case "zset":
			pipe.ZAdd(ctx, key, redis.Z{Score: float64(i), Member: val})
		default: // "string"
			pipe.Set(ctx, key, val, 0)
		}
		if ttlSec > 0 {
			pipe.Expire(ctx, key, ttl)
		}
		created++

		if int(created)%seedPipelineBatch == 0 {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}

	s.logger.Info("chaos seed complete",
		zap.Int64("keys", created),
		zap.String("prefix", prefix),
		zap.String("type", keyType),
	)
	return &SeedResult{
		KeysCreated: created,
		Prefix:      prefix,
		KeyType:     keyType,
		ElapsedMs:   time.Since(start).Milliseconds(),
	}, nil
}

// Flush xóa toàn bộ key khớp pattern trên master bằng SCAN + DEL.
//
// Redis commands:
//  1. SCAN cursor MATCH <pattern> COUNT 200 → tìm key theo batch
//  2. DEL key1 key2 ... keyN → xóa cả batch trong một lần gọi (variadic DEL)
//
// Không dùng FLUSHALL/FLUSHDB để tránh xóa key ngoài pattern.
// Variadic DEL giảm round-trip so với gọi DEL từng key một — toàn bộ batch
// được xóa trong một request/response cycle.
func (s *ChaosService) Flush(ctx context.Context, pattern string) (*FlushResult, error) {
	if pattern == "" {
		pattern = "test:*"
	}
	start := time.Now()
	client := sentinel.NewMasterClient(s.cfg)
	defer client.Close()

	var deleted int64
	var cursor uint64
	for {
		if ctx.Err() != nil {
			break
		}
		scanCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		keys, next, err := client.Scan(scanCtx, cursor, pattern, 200).Result()
		cancel()
		if err != nil {
			return nil, fmt.Errorf("SCAN %s: %w", pattern, err)
		}

		if len(keys) > 0 {
			delCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			n, err := client.Del(delCtx, keys...).Result()
			cancel()
			if err != nil {
				s.logger.Warn("DEL partial failure", zap.Error(err))
			}
			deleted += n
		}

		cursor = next
		if cursor == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	s.logger.Info("chaos flush complete",
		zap.Int64("keys_deleted", deleted),
		zap.String("pattern", pattern),
	)
	return &FlushResult{
		KeysDeleted: deleted,
		Pattern:     pattern,
		ElapsedMs:   time.Since(start).Milliseconds(),
	}, nil
}

// randomValue returns a hex-encoded random byte slice of approximately size bytes.
func randomValue(size int) string {
	if size <= 0 {
		size = 16
	}
	buf := make([]byte, (size+1)/2)
	_, _ = rand.Read(buf)
	return hex.EncodeToString(buf)[:size]
}
