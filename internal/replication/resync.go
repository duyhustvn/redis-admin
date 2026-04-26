package replication

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

// ResyncReport summarises replication backlog health for a single node.
type ResyncReport struct {
	NodeAddr          string `json:"node_addr"`
	Role              string `json:"role"`
	TotalResyncs      int64  `json:"total_resyncs"`
	BacklogSize       int64  `json:"backlog_size_bytes"`
	BacklogActiveSize int64  `json:"backlog_active_size_bytes"`
	BacklogAlert      bool   `json:"backlog_alert"`          // active > 90% of backlog size
	Advisory          string `json:"advisory,omitempty"`
}

// GetResyncStats fetches INFO replication from master and replicas, returning
// per-node full-resync counts and backlog utilisation.
func (s *ReplicationService) GetResyncStats(ctx context.Context) ([]ResyncReport, error) {
	addrs, err := s.sentinelSvc.GetNodeAddresses(ctx)
	if err != nil {
		return nil, fmt.Errorf("get resync stats: %w", err)
	}

	type nodeRole struct {
		addr string
		role string
	}
	nodes := []nodeRole{{addrs.Master, "master"}}
	for _, r := range addrs.Replicas {
		nodes = append(nodes, nodeRole{r, "replica"})
	}

	var all []ResyncReport
	var errs []error
	for _, n := range nodes {
		r, err := s.fetchResync(ctx, n.addr, n.role)
		if err != nil {
			s.logger.Warn("resync fetch failed", zap.String("node", n.addr), zap.Error(err))
			errs = append(errs, fmt.Errorf("node %s: %w", n.addr, err))
			continue
		}
		all = append(all, r)
	}
	return all, errors.Join(errs...)
}

func (s *ReplicationService) fetchResync(ctx context.Context, addr, role string) (ResyncReport, error) {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	client := sentinel.NewDirectClient(addr, s.cfg.RedisPassword)
	defer client.Close()

	raw, err := client.Info(tctx, "replication").Result()
	if err != nil {
		return ResyncReport{}, fmt.Errorf("INFO replication on %s: %w", addr, sentinel.ErrNodeUnreachable)
	}

	kv := parseInfo(raw)
	r := ResyncReport{
		NodeAddr: addr,
		Role:     role,
	}

	// total_resyncs_processed is exposed since Redis 6.2.
	r.TotalResyncs, _ = strconv.ParseInt(kv["total_resyncs_processed"], 10, 64)
	r.BacklogSize, _ = strconv.ParseInt(kv["repl_backlog_size"], 10, 64)
	r.BacklogActiveSize, _ = strconv.ParseInt(kv["repl_backlog_active"], 10, 64)

	if r.BacklogSize > 0 && r.BacklogActiveSize > 0 {
		pct := float64(r.BacklogActiveSize) / float64(r.BacklogSize)
		if pct > 0.90 {
			r.BacklogAlert = true
			r.Advisory = fmt.Sprintf(
				"Backlog %.0f%% full (%.1f MiB / %.1f MiB). "+
					"Consider increasing repl-backlog-size to avoid forced full resyncs.",
				pct*100,
				float64(r.BacklogActiveSize)/(1<<20),
				float64(r.BacklogSize)/(1<<20),
			)
		}
	}
	return r, nil
}

// splitLines splits an INFO response on \r\n, skipping section headers and empty lines.
func splitLines(raw string) []string {
	var out []string
	for _, line := range strings.Split(raw, "\r\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		out = append(out, line)
	}
	return out
}

// cutColon splits "key:value" and returns (key, value, true).
func cutColon(s string) (string, string, bool) {
	idx := strings.IndexByte(s, ':')
	if idx < 0 {
		return "", "", false
	}
	return strings.TrimSpace(s[:idx]), strings.TrimSpace(s[idx+1:]), true
}
