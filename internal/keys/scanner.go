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
	Namespace  string  `json:"namespace"`
	KeyCount   int64   `json:"key_count"`
	TotalBytes int64   `json:"total_bytes"`
	AvgBytes   float64 `json:"avg_bytes"`
	MaxBytes   int64   `json:"max_bytes"`
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

// ScanBigKeys quét toàn bộ keyspace để tìm key vượt ngưỡng kích thước, gom kết quả
// theo namespace (prefix trước dấu ':'), và gọi callback onKey cho mỗi key tìm được.
//
// Luồng xử lý:
//  1. GetNodeAddresses → lấy danh sách node từ Sentinel.
//  2. Ưu tiên chạy trên replica để không tải master; fallback về master nếu không có replica.
//  3. Với mỗi node gọi scanNode → lặp SCAN → inspectKey từng key.
//  4. Gom thống kê per-namespace (KeyCount, TotalBytes, AvgBytes, MaxBytes).
//
// onKey callback được gọi ngay khi tìm thấy key đủ lớn — dùng cho SSE streaming
// để client nhận kết quả liên tục thay vì chờ scan xong toàn bộ.
func (s *Service) ScanBigKeys(ctx context.Context, thresholdBytes int64, onKey func(KeyReport)) ([]NamespaceStat, error) {
	addrs, err := s.sentinelSvc.GetNodeAddresses(ctx)
	if err != nil {
		return nil, fmt.Errorf("scan bigkeys: %w", err)
	}

	// Ưu tiên replica để không block master đang xử lý write.
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

// scanNode duyệt toàn bộ keyspace của một node bằng SCAN theo từng batch.
//
// Redis command: SCAN cursor MATCH * COUNT 200
//
// Output mỗi lần gọi SCAN:
//   - []string keys: các key trong batch hiện tại (số lượng xấp xỉ COUNT, không chính xác)
//   - uint64 next:   cursor cho lần gọi tiếp theo; bằng 0 nghĩa là đã duyệt hết
//
// SCAN không block Redis (khác KEYS *) — mỗi lần gọi xử lý một phần nhỏ của keyspace
// rồi trả về cursor để tiếp tục. sleep 1ms giữa các batch để tránh chiếm CPU Redis.
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

		// Nhường CPU Redis giữa mỗi batch để không ảnh hưởng latency của client thật.
		time.Sleep(time.Millisecond)
	}
	return nil
}

// inspectKey kiểm tra một key: đo kích thước, nếu vượt ngưỡng thì lấy thêm type + TTL
// và đưa vào kết quả. Thiết kế "check trước, gọi thêm sau" giảm số round-trip đáng kể
// vì phần lớn key trong thực tế nhỏ hơn ngưỡng.
//
// Redis commands (theo thứ tự, có điều kiện):
//
//  1. MEMORY USAGE <key> SAMPLES 0
//     → trả về int64 bytes (bao gồm cả overhead encoding của Redis)
//     → SAMPLES 0: ước tính dựa trên encoding header thay vì sample toàn bộ value
//        — nhanh hơn nhiều, đủ chính xác để so ngưỡng
//     → lỗi hoặc sizeBytes < thresholdBytes → return ngay, bỏ qua TYPE + TTL
//
//  2. TYPE <key>   (chỉ gọi khi key vượt ngưỡng)
//     → trả về "string" | "hash" | "list" | "set" | "zset" | "stream" | "none"
//
//  3. TTL <key>    (chỉ gọi khi key vượt ngưỡng)
//     → trả về time.Duration; chuyển sang giây
//     → -1s = key không có TTL (tồn tại vĩnh viễn)
//     → -2s = key không tồn tại (đã bị evict giữa lúc SCAN và TTL)
func (s *Service) inspectKey(ctx context.Context, client *redis.Client, addr, key string, thresholdBytes int64, onKey func(KeyReport), ns map[string]*NamespaceStat) {
	memCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	sizeBytes, err := client.MemoryUsage(memCtx, key, 0).Result()
	cancel()
	// Bỏ qua key nhỏ hơn ngưỡng để tránh gọi TYPE + TTL không cần thiết.
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

	// Cập nhật thống kê per-namespace để trả về aggregates.
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
