package keys

import (
	"container/heap"
	"context"
	"fmt"
	"time"

	"github.com/duydinhle/redis-sentinel-admin/internal/sentinel"
	"go.uber.org/zap"
)

// HotKeyReport describes a single hot key detected via LFU access frequency.
type HotKeyReport struct {
	Key       string `json:"key"`
	Type      string `json:"type"`
	Frequency int64  `json:"frequency"` // OBJECT FREQ value
	NodeAddr  string `json:"node_addr"`
	Namespace string `json:"namespace"`
}

// GetHotkeys scans all nodes for keys with the highest LFU access frequency.
// Requires maxmemory-policy to be an *-lfu variant; returns an error if not.
func (s *Service) GetHotkeys(ctx context.Context, topN int) ([]HotKeyReport, error) {
	addrs, err := s.sentinelSvc.GetNodeAddresses(ctx)
	if err != nil {
		return nil, fmt.Errorf("get hotkeys: %w", err)
	}

	targets := addrs.Replicas
	if len(targets) == 0 {
		targets = []string{addrs.Master}
	}

	h := &hotHeap{}
	heap.Init(h)

	for _, addr := range targets {
		if err := s.collectHotkeys(ctx, addr, topN, h); err != nil {
			s.logger.Warn("hotkey collection failed", zap.String("node", addr), zap.Error(err))
		}
	}

	result := make([]HotKeyReport, h.Len())
	for i := h.Len() - 1; i >= 0; i-- {
		result[i] = heap.Pop(h).(HotKeyReport)
	}
	return result, nil
}

// collectHotkeys quét toàn bộ keyspace của một node để tìm top-N key có tần suất truy cập cao nhất.
//
// Luồng xử lý gồm 3 lệnh Redis:
//
// 1. CONFIG GET maxmemory-policy
//   - Xác nhận node đang dùng LFU eviction policy (allkeys-lfu hoặc volatile-lfu).
//   - Nếu không phải LFU, OBJECT FREQ sẽ luôn trả về 0 nên hàm trả lỗi sớm.
//
// 2. SCAN cursor MATCH * COUNT scanBatchSize  (lặp cho đến cursor = 0)
//   - Duyệt keyspace theo từng batch (không block Redis như KEYS *).
//   - Mỗi lần trả về một slice key và cursor tiếp theo; cursor = 0 báo hết vòng.
//
// 3. OBJECT FREQ <key>  (cho từng key trong batch)
//   - Trả về LFU access frequency counter (0–255, logarithmic scale).
//   - Giá trị này do Redis cập nhật theo mỗi lần key được đọc/ghi.
//   - Dùng min-heap (hotHeap) để duy trì top-N hiệu quả: nếu freq mới lớn hơn
//     phần tử nhỏ nhất trong heap thì thay thế, giữ heap đúng kích thước topN.
//
// 4. TYPE <key>  (cho từng key lọt vào heap)
//   - Lấy kiểu dữ liệu (string/hash/list/set/zset/stream) để đưa vào report.
func (s *Service) collectHotkeys(ctx context.Context, addr string, topN int, h *hotHeap) error {
	client := sentinel.NewDirectClient(addr, s.cfg.RedisPassword)
	defer client.Close()

	// Bước 1: kiểm tra LFU policy trước khi scan để tránh scan vô ích.
	cfgCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	vals, err := client.ConfigGet(cfgCtx, "maxmemory-policy").Result()
	cancel()
	if err != nil {
		return fmt.Errorf("CONFIG GET maxmemory-policy on %s: %w", addr, err)
	}
	policy := ""
	if len(vals) >= 2 {
		policy, _ = vals["maxmemory-policy"]
	}
	if policy == "" || (len(policy) < 3 || policy[len(policy)-3:] != "lfu") {
		return fmt.Errorf("node %s: maxmemory-policy %q is not an LFU policy — OBJECT FREQ unavailable", addr, policy)
	}

	// Bước 2: SCAN toàn keyspace theo batch để không block Redis.
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

			// Bước 3: OBJECT FREQ lấy LFU counter của key.
			freqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			freq, err := client.ObjectFreq(freqCtx, key).Result()
			cancel()
			if err != nil {
				continue
			}

			// Bước 4: TYPE chỉ gọi khi key đủ điều kiện vào heap, giảm số round-trip.
			typeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			keyType, _ := client.Type(typeCtx, key).Result()
			cancel()

			entry := HotKeyReport{
				Key:       key,
				Type:      keyType,
				Frequency: freq,
				NodeAddr:  addr,
				Namespace: keyNamespace(key),
			}

			// Duy trì min-heap kích thước topN: thay thế phần tử nhỏ nhất nếu freq mới lớn hơn.
			if h.Len() < topN {
				heap.Push(h, entry)
			} else if h.Len() > 0 && freq > (*h)[0].Frequency {
				heap.Pop(h)
				heap.Push(h, entry)
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

// hotHeap is a min-heap of HotKeyReport ordered by Frequency (lowest at root).
// This lets us efficiently maintain the top-N hottest keys.
type hotHeap []HotKeyReport

func (h hotHeap) Len() int            { return len(h) }
func (h hotHeap) Less(i, j int) bool  { return h[i].Frequency < h[j].Frequency }
func (h hotHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *hotHeap) Push(x interface{}) { *h = append(*h, x.(HotKeyReport)) }
func (h *hotHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
