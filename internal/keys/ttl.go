package keys

import (
	"context"
	"fmt"
	"time"

	"github.com/duydinhle/redis-sentinel-admin/internal/sentinel"
	"go.uber.org/zap"
)

// NamespaceTTLReport summarises TTL health for a single key namespace prefix.
type NamespaceTTLReport struct {
	Namespace  string  `json:"namespace"`
	TotalKeys  int64   `json:"total_keys"`
	NoTTLKeys  int64   `json:"no_ttl_keys"`
	NoTTLPct   float64 `json:"no_ttl_pct"`   // percentage of keys with no TTL
	NoTTLAlert bool    `json:"no_ttl_alert"` // true when >50% of keys have no TTL
}

type nsStat struct {
	total int64
	noTTL int64
}

// GetTTLReport scans all keys and produces a per-namespace TTL health summary.
func (s *Service) GetTTLReport(ctx context.Context) ([]NamespaceTTLReport, error) {
	addrs, err := s.sentinelSvc.GetNodeAddresses(ctx)
	if err != nil {
		return nil, fmt.Errorf("get ttl report: %w", err)
	}

	// Use replicas when available to avoid master load.
	targets := addrs.Replicas
	if len(targets) == 0 {
		targets = []string{addrs.Master}
	}

	ns := make(map[string]*nsStat)

	for _, addr := range targets {
		if err := s.collectTTLStats(ctx, addr, ns); err != nil {
			s.logger.Warn("TTL scan partial failure", zap.String("node", addr), zap.Error(err))
		}
	}

	reports := make([]NamespaceTTLReport, 0, len(ns))
	for name, st := range ns {
		r := NamespaceTTLReport{
			Namespace: name,
			TotalKeys: st.total,
			NoTTLKeys: st.noTTL,
		}
		if st.total > 0 {
			r.NoTTLPct = float64(st.noTTL) / float64(st.total) * 100
		}
		r.NoTTLAlert = r.NoTTLPct > 50
		reports = append(reports, r)
	}
	return reports, nil
}

// collectTTLStats quét toàn bộ keyspace và kiểm tra TTL từng key để tính tỷ lệ
// key không có TTL (persistent) per namespace.
//
// Redis commands (2 lệnh per key):
//
//  1. SCAN cursor MATCH * COUNT 200
//     → duyệt keyspace theo batch, không block Redis (xem scanNode để biết thêm).
//
//  2. TTL <key>  → trả về time.Duration:
//     -1s = key tồn tại nhưng không có expiry  → tính vào noTTL count
//     -2s = key không tồn tại (bị evict giữa SCAN và TTL)  → bỏ qua
//     Ns  = còn N giây sống  → tính vào total nhưng không vào noTTL
//
// Lý do chỉ dùng TTL (không PTTL): độ chính xác millisecond không cần thiết
// cho phân tích sức khoẻ TTL ở cấp namespace.
func (s *Service) collectTTLStats(ctx context.Context, addr string, ns map[string]*nsStat) error {
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

			ttlCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			ttlDur, err := client.TTL(ttlCtx, key).Result()
			cancel()
			if err != nil {
				continue
			}

			name := keyNamespace(key)
			st, ok := ns[name]
			if !ok {
				st = &nsStat{}
				ns[name] = st
			}
			st.total++
			// -1s = key tồn tại vĩnh viễn (no expiry).
			if ttlDur.Seconds() == -1 {
				st.noTTL++
			}
		}

		cursor = next
		if cursor == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	return nil
}
