package operations

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/duydinhle/redis-sentinel-admin/internal/sentinel"
	"go.uber.org/zap"
)

// FailoverResult describes the outcome (or dry-run preview) of a graceful failover.
type FailoverResult struct {
	DryRun          bool     `json:"dry_run"`
	PreChecks       []string `json:"pre_checks"`
	SelectedReplica string   `json:"selected_replica"`
	OldMaster       string   `json:"old_master"`
	NewMaster       string   `json:"new_master,omitempty"`
	ElapsedMs       int64    `json:"elapsed_ms"`
}

const (
	maxAcceptableLagBytes = 50 * 1 << 20 // 50 MiB
	failoverWaitTimeout   = 45 * time.Second
	failoverPollInterval  = 1 * time.Second
)

// Failover thực hiện (hoặc mô phỏng) graceful failover với pre-flight checks.
//
// Luồng các lệnh Redis theo thứ tự:
//  1. SENTINEL CKQUORUM <masterName>   → kiểm tra quorum (gửi đến sentinel port 26379)
//  2. INFO replication (x2 per replica) → tính lag (master_repl_offset - slave_repl_offset)
//     để chọn replica có lag thấp nhất làm ứng viên failover
//  3. SENTINEL FAILOVER <masterName>   → kích hoạt failover (chỉ khi dryRun=false)
//     gửi qua sc.Do() vì go-redis không có wrapper cho lệnh này
//  4. Sentinel.GetMasterAddrByName (poll 1s/lần, tối đa 45s) → đợi master mới xuất hiện
func (s *OperationsService) Failover(ctx context.Context, dryRun bool) (*FailoverResult, error) {
	start := time.Now()
	result := &FailoverResult{DryRun: dryRun}

	addrs, err := s.sentinelSvc.GetNodeAddresses(ctx)
	if err != nil {
		return nil, fmt.Errorf("failover: resolve nodes: %w", err)
	}
	result.OldMaster = addrs.Master

	// ── Pre-flight checks ────────────────────────────────────────────────────

	quorumOK := false
	for _, sAddr := range s.cfg.SentinelAddrs {
		sc := sentinel.NewSentinelManagementClient(sAddr, s.cfg.SentinelPassword)
		tctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		res, err := sc.CkQuorum(tctx, s.cfg.MasterName).Result()
		cancel()
		sc.Close()
		if err == nil && strings.HasPrefix(res, "OK") {
			result.PreChecks = append(result.PreChecks, fmt.Sprintf("quorum OK: %s", res))
			quorumOK = true
			break
		}
		result.PreChecks = append(result.PreChecks, fmt.Sprintf("quorum check failed on %s: %v", sAddr, err))
	}
	if !quorumOK {
		return result, fmt.Errorf("failover: quorum not met")
	}

	// Per-replica lag check — build candidate list.
	type candidate struct {
		addr     string
		lagBytes int64
	}
	var candidates []candidate
	for _, rAddr := range addrs.Replicas {
		lag, err := s.fetchReplicaLag(ctx, addrs.Master, rAddr)
		if err != nil {
			result.PreChecks = append(result.PreChecks,
				fmt.Sprintf("replica %s: lag unavailable (%v)", rAddr, err))
			continue
		}
		note := "OK"
		if lag > maxAcceptableLagBytes {
			note = fmt.Sprintf("WARNING lag=%.1f MiB exceeds 50 MiB threshold", float64(lag)/(1<<20))
		}
		result.PreChecks = append(result.PreChecks,
			fmt.Sprintf("replica %s lag=%.1f KiB: %s", rAddr, float64(lag)/1024, note))
		candidates = append(candidates, candidate{rAddr, lag})
	}

	if len(candidates) == 0 {
		return result, fmt.Errorf("failover: no eligible replicas found")
	}

	// Pick lowest-lag replica.
	best := candidates[0]
	for _, c := range candidates[1:] {
		if c.lagBytes < best.lagBytes {
			best = c
		}
	}
	result.SelectedReplica = best.addr
	result.PreChecks = append(result.PreChecks,
		fmt.Sprintf("selected replica: %s (lag %.1f KiB)", best.addr, float64(best.lagBytes)/1024))

	if dryRun {
		result.ElapsedMs = time.Since(start).Milliseconds()
		return result, nil
	}

	// ── Execute failover via SENTINEL FAILOVER ───────────────────────────────

	sentinelAddr, err := s.reachableSentinelAddr(ctx)
	if err != nil {
		return result, fmt.Errorf("failover: no reachable sentinel: %w", err)
	}

	// Use NewDirectClient to send raw SENTINEL FAILOVER command.
	sc := sentinel.NewDirectClient(sentinelAddr, s.cfg.SentinelPassword)
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err = sc.Do(tctx, "SENTINEL", "failover", s.cfg.MasterName).Err()
	cancel()
	sc.Close()
	if err != nil {
		return result, fmt.Errorf("SENTINEL FAILOVER: %w", err)
	}

	s.logger.Info("sentinel failover triggered",
		zap.String("old_master", addrs.Master),
		zap.String("candidate", best.addr),
		zap.String("sentinel", sentinelAddr),
	)

	// ── Wait for new master ──────────────────────────────────────────────────

	newMaster, err := s.waitForNewMaster(ctx, addrs.Master)
	if err != nil {
		return result, fmt.Errorf("failover: waiting for new master: %w", err)
	}
	result.NewMaster = newMaster
	result.ElapsedMs = time.Since(start).Milliseconds()

	s.logger.Info("failover complete",
		zap.String("old_master", addrs.Master),
		zap.String("new_master", newMaster),
		zap.Int64("elapsed_ms", result.ElapsedMs),
	)
	s.notify.notifyFailover(addrs.Master, newMaster)
	return result, nil
}

func (s *OperationsService) waitForNewMaster(ctx context.Context, oldMaster string) (string, error) {
	deadline := time.Now().Add(failoverWaitTimeout)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		addrs, err := s.sentinelSvc.GetNodeAddresses(ctx)
		if err == nil && addrs.Master != "" && addrs.Master != oldMaster {
			return addrs.Master, nil
		}
		time.Sleep(failoverPollInterval)
	}
	return "", fmt.Errorf("timed out waiting %v for new master to appear", failoverWaitTimeout)
}

func (s *OperationsService) fetchReplicaLag(ctx context.Context, masterAddr, replicaAddr string) (int64, error) {
	masterOffset, err := s.fetchReplOffset(ctx, masterAddr, "master_repl_offset")
	if err != nil {
		return 0, fmt.Errorf("master offset: %w", err)
	}
	replicaOffset, err := s.fetchReplOffset(ctx, replicaAddr, "slave_repl_offset")
	if err != nil {
		return 0, fmt.Errorf("replica offset: %w", err)
	}
	lag := masterOffset - replicaOffset
	if lag < 0 {
		lag = 0
	}
	return lag, nil
}

// fetchReplOffset đọc một offset field từ INFO replication của một node.
//
// Redis command: INFO replication
//
// field nhận một trong hai giá trị:
//   - "master_repl_offset" → offset hiện tại của master (gọi trên master)
//   - "slave_repl_offset"  → offset mà replica đã áp dụng (gọi trên replica)
//
// Dùng chung cho cả master lẫn replica thay vì tạo hai hàm riêng.
func (s *OperationsService) fetchReplOffset(ctx context.Context, addr, field string) (int64, error) {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	client := sentinel.NewDirectClient(addr, s.cfg.RedisPassword)
	defer client.Close()
	raw, err := client.Info(tctx, "replication").Result()
	if err != nil {
		return 0, fmt.Errorf("INFO replication on %s: %w", addr, sentinel.ErrNodeUnreachable)
	}
	v, _ := strconv.ParseInt(parseInfoKV(raw)[field], 10, 64)
	return v, nil
}

func (s *OperationsService) reachableSentinelAddr(ctx context.Context) (string, error) {
	for _, addr := range s.cfg.SentinelAddrs {
		sc := sentinel.NewDirectClient(addr, s.cfg.SentinelPassword)
		tctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := sc.Ping(tctx).Err()
		cancel()
		sc.Close()
		if err == nil {
			return addr, nil
		}
	}
	return "", sentinel.ErrNodeUnreachable
}

// parseInfoKV parses key:value\r\n INFO output into a map.
func parseInfoKV(raw string) map[string]string {
	out := make(map[string]string)
	for _, line := range strings.Split(raw, "\r\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if idx := strings.IndexByte(line, ':'); idx >= 0 {
			out[strings.TrimSpace(line[:idx])] = strings.TrimSpace(line[idx+1:])
		}
	}
	return out
}
