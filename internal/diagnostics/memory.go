package diagnostics

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/duydinhle/redis-sentinel-admin/internal/sentinel"
	"go.uber.org/zap"
)

// MemoryReport aggregates memory health indicators for a single cluster node.
type MemoryReport struct {
	NodeAddr            string  `json:"node_addr"`
	Role                string  `json:"role"`
	UsedMemoryBytes     int64   `json:"used_memory_bytes"`
	UsedMemoryHuman     string  `json:"used_memory_human"`
	MaxMemoryBytes      int64   `json:"max_memory_bytes"`
	FragRatio           float64 `json:"frag_ratio"`
	FragAlert           bool    `json:"frag_alert"`           // true when frag_ratio > 1.5
	EvictionPolicy      string  `json:"eviction_policy"`
	EvictedKeysTotal    int64   `json:"evicted_keys_total"`
	EvictedKeysPerSec   float64 `json:"evicted_keys_per_sec"` // delta since last call
	RDBEnabled          bool    `json:"rdb_enabled"`
	RDBLastSaveTime     int64   `json:"rdb_last_save_time"`
	RDBChangesSinceSave int64   `json:"rdb_changes_since_save"`
	RDBLastBgsaveStatus string  `json:"rdb_last_bgsave_status"`
	RDBAlert            bool    `json:"rdb_alert"` // true when last bgsave failed
	AOFEnabled          bool    `json:"aof_enabled"`
	AOFLastRewriteStatus string `json:"aof_last_rewrite_status,omitempty"`
	AOFAlert            bool    `json:"aof_alert"` // true when last aof rewrite failed
}

// GetMemory collects memory, persistence, and eviction stats from every node.
func (s *DiagnosticsService) GetMemory(ctx context.Context) ([]MemoryReport, error) {
	addrs, err := s.sentinelSvc.GetNodeAddresses(ctx)
	if err != nil {
		return nil, fmt.Errorf("get memory: %w", err)
	}

	type nodeRole struct {
		addr string
		role string
	}
	nodes := []nodeRole{{addrs.Master, "master"}}
	for _, r := range addrs.Replicas {
		nodes = append(nodes, nodeRole{r, "replica"})
	}

	var all []MemoryReport
	var errs []error
	for _, n := range nodes {
		report, err := s.fetchMemory(ctx, n.addr, n.role)
		if err != nil {
			s.logger.Warn("memory fetch failed", zap.String("node", n.addr), zap.Error(err))
			errs = append(errs, fmt.Errorf("node %s: %w", n.addr, err))
			continue
		}
		all = append(all, report)
	}
	return all, errors.Join(errs...)
}

func (s *DiagnosticsService) fetchMemory(ctx context.Context, addr, role string) (MemoryReport, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	client := sentinel.NewDirectClient(addr, s.cfg.RedisPassword)
	defer client.Close()

	raw, err := client.Info(ctx, "memory", "stats", "persistence", "server").Result()
	if err != nil {
		return MemoryReport{}, fmt.Errorf("INFO on %s: %w", addr, sentinel.ErrNodeUnreachable)
	}

	kv := parseInfoAll(raw)

	report := MemoryReport{
		NodeAddr:            addr,
		Role:                role,
		UsedMemoryHuman:     kv["used_memory_human"],
		EvictionPolicy:      kv["maxmemory_policy"],
		RDBLastBgsaveStatus: kv["rdb_last_bgsave_status"],
		AOFLastRewriteStatus: kv["aof_last_bgrewrite_status"],
	}

	report.UsedMemoryBytes, _ = strconv.ParseInt(kv["used_memory"], 10, 64)
	report.MaxMemoryBytes, _ = strconv.ParseInt(kv["maxmemory"], 10, 64)
	report.FragRatio, _ = strconv.ParseFloat(kv["mem_fragmentation_ratio"], 64)
	report.FragAlert = report.FragRatio > 1.5

	report.EvictedKeysTotal, _ = strconv.ParseInt(kv["evicted_keys"], 10, 64)
	report.EvictedKeysPerSec = s.evictionRate(addr, report.EvictedKeysTotal)

	rdbSave, _ := strconv.ParseInt(kv["rdb_saves"], 10, 64)
	report.RDBEnabled = rdbSave >= 0 && kv["rdb_last_bgsave_status"] != ""
	report.RDBLastSaveTime, _ = strconv.ParseInt(kv["rdb_last_save_time"], 10, 64)
	report.RDBChangesSinceSave, _ = strconv.ParseInt(kv["rdb_changes_since_last_save"], 10, 64)
	report.RDBAlert = kv["rdb_last_bgsave_status"] == "err"

	aofEnabled, _ := strconv.ParseInt(kv["aof_enabled"], 10, 64)
	report.AOFEnabled = aofEnabled == 1
	report.AOFAlert = kv["aof_last_bgrewrite_status"] == "err"

	return report, nil
}

// evictionRate computes evicted_keys/sec since the last call for this node.
func (s *DiagnosticsService) evictionRate(addr string, current int64) float64 {
	now := time.Now()
	s.mu.Lock()
	prev, ok := s.lastEvicted[addr]
	s.lastEvicted[addr] = evictedSample{count: current, at: now}
	s.mu.Unlock()

	if !ok || current < prev.count {
		return 0
	}
	elapsed := now.Sub(prev.at).Seconds()
	if elapsed <= 0 {
		return 0
	}
	return float64(current-prev.count) / elapsed
}

// parseInfoAll parses all sections of an INFO response into a flat key→value map.
func parseInfoAll(raw string) map[string]string {
	out := make(map[string]string)
	for _, line := range strings.Split(raw, "\r\n") {
		k, v, ok := strings.Cut(line, ":")
		if !ok || strings.HasPrefix(k, "#") {
			continue
		}
		out[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	return out
}
