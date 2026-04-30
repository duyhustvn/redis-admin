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

// GetStaleLocks quét keyspace theo pattern để tìm distributed lock bị stale —
// tức là lock không có TTL (tồn tại vĩnh viễn) hoặc TTL vượt ngưỡng cho phép.
//
// Tham số:
//   - pattern: SCAN pattern để lọc key (vd: "lock:*", "mutex:*")
//   - staleThresholdSec: ngưỡng TTL tính bằng giây
//       = 0  → chỉ flag key không có TTL (-1)
//       > 0  → flag cả key không có TTL lẫn key có TTL > ngưỡng
//
// Luồng xử lý:
//  1. Ưu tiên replica; fallback master nếu không có replica.
//  2. seen map deduplicate key xuất hiện ở nhiều replica (key đã replicated).
//  3. Lỗi từng node → skip node đó, tiếp tục node còn lại (kết quả partial).
func (s *DiagnosticsService) GetStaleLocks(ctx context.Context, pattern string, staleThresholdSec int64) ([]LockReport, error) {
	addrs, err := s.sentinelSvc.GetNodeAddresses(ctx)
	if err != nil {
		return nil, fmt.Errorf("get stale locks: %w", err)
	}

	targets := addrs.Replicas
	if len(targets) == 0 {
		targets = []string{addrs.Master}
	}

	var all []LockReport
	var errs []error
	seen := make(map[string]struct{}) // tránh báo cáo trùng key trên nhiều replica

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

// scanLocks duyệt keyspace theo pattern để tìm lock stale, dùng 3 lệnh Redis:
//
// ── Lệnh 1: SCAN cursor MATCH <pattern> COUNT 200 ───────────────────────────
//
//	Lọc key theo pattern ngay tại Redis thay vì lấy tất cả rồi filter ở Go.
//	Lưu ý: SCAN với MATCH vẫn phải duyệt toàn bộ keyspace nội bộ —
//	COUNT chỉ là gợi ý batch size, không phải số key match trả về.
//
// ── Lệnh 2: TTL <key> ────────────────────────────────────────────────────────
//
//	Trả về time.Duration, chuyển sang giây:
//	  -1s → key không có TTL (vĩnh viễn) → IsStale = true
//	  -2s → key không tồn tại (bị evict giữa SCAN và TTL) → skip
//	   Ns → còn N giây; so với staleThresholdSec để quyết định có stale không
//
//	Gọi TTL trước MEMORY USAGE — nếu key không stale thì không cần đo kích thước.
//
// ── Lệnh 3: MEMORY USAGE <key> SAMPLES 0 ────────────────────────────────────
//
//	Chỉ gọi khi key đã xác nhận là stale.
//	Kết quả dùng để ước tính tổng bộ nhớ bị chiếm bởi lock không bao giờ được giải phóng.
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
			// Bỏ qua key đã xử lý từ replica trước (key được replicated sang mọi replica).
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

			// ttlSec == -2: key bị evict giữa SCAN và TTL, bỏ qua.
			isStale := ttlSec == -1 ||
				(staleThresholdSec > 0 && ttlSec > staleThresholdSec)
			if !isStale {
				continue
			}

			// Chỉ đo kích thước sau khi xác nhận stale để giảm round-trip.
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
