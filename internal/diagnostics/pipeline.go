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

// PipelineReport summarises MULTI/EXEC and pipeline health for one node.
type PipelineReport struct {
	NodeAddr            string `json:"node_addr"`
	EXECAbortCount      int64  `json:"exec_abort_count"`       // EXECABORT errors since node start
	RejectedCalls       int64  `json:"rejected_calls"`         // commands rejected (errors/limits)
	MaxInputBufferBytes int64  `json:"max_input_buffer_bytes"` // client_recent_max_input_buffer
	OversizedPipelines  int    `json:"oversized_pipelines"`    // clients with qbuf > 1 MiB
}

const oversizedPipelineThreshold = 1 * 1024 * 1024 // 1 MiB

// GetPipelineStats thu thập số liệu pipeline và transaction từ tất cả node trong cluster.
//
// Luồng xử lý:
//  1. GetNodeAddresses → lấy master + replicas từ Sentinel.
//  2. Với mỗi node gọi fetchPipelineReport (4 lệnh Redis).
//  3. Lỗi từng node không dừng toàn bộ — kết quả partial được trả về kèm errors.Join.
//
// Lý do chạy trên cả master lẫn replica: EXECABORT và rejected_calls xảy ra nhiều nhất
// trên master (nơi nhận write), còn oversized pipeline có thể xuất hiện ở cả hai phía.
func (s *DiagnosticsService) GetPipelineStats(ctx context.Context) ([]PipelineReport, error) {
	addrs, err := s.sentinelSvc.GetNodeAddresses(ctx)
	if err != nil {
		return nil, fmt.Errorf("get pipeline stats: %w", err)
	}

	nodes := append([]string{addrs.Master}, addrs.Replicas...)

	var results []PipelineReport
	var errs []error
	for _, addr := range nodes {
		report, err := s.fetchPipelineReport(ctx, addr)
		if err != nil {
			s.logger.Warn("pipeline stats fetch failed", zap.String("node", addr), zap.Error(err))
			errs = append(errs, fmt.Errorf("node %s: %w", addr, err))
			continue
		}
		results = append(results, report)
	}
	return results, errors.Join(errs...)
}

// fetchPipelineReport gọi 4 lệnh Redis để thu thập đầy đủ số liệu pipeline và transaction.
// Gọi tuần tự (không pipeline) để xử lý lỗi độc lập — INFO stats là bắt buộc,
// ba lệnh còn lại fail thì log warn/debug và tiếp tục với giá trị zero.
//
// ── Lệnh 1: INFO stats ──────────────────────────────────────────────────────
//
//	Redis command: INFO stats
//	Field: rejected_calls → số lệnh bị Redis từ chối (vd: server đạt maxmemory,
//	       lệnh không hợp lệ trong context hiện tại).
//	       Giá trị cộng dồn từ lúc node khởi động.
//
// ── Lệnh 2: INFO clients ────────────────────────────────────────────────────
//
//	Redis command: INFO clients
//	Field: client_recent_max_input_buffer → kích thước input buffer lớn nhất (bytes)
//	       của bất kỳ client nào trong khoảng thời gian gần đây.
//	       Buffer lớn → client đang gửi pipeline rất lớn trong một lần.
//
// ── Lệnh 3: INFO errorstats ─────────────────────────────────────────────────
//
//	Redis command: INFO errorstats
//	Output format:
//	  errorstat_EXECABORT:count=5,first_seen=...,last_seen=...
//	  errorstat_ERR:count=2,...
//	Chỉ đếm EXECABORT — lỗi này xảy ra khi MULTI/EXEC bị hủy do WATCH key bị
//	modified bởi client khác (optimistic locking failure). Tăng nhiều → race condition.
//	extractCount() parse "count=N" từ phần sau dấu ':'.
//
// ── Lệnh 4: CLIENT LIST ─────────────────────────────────────────────────────
//
//	Redis command: CLIENT LIST
//	Field: qbuf → số byte đang chờ trong input buffer của từng connection.
//	Đếm số client có qbuf > 1 MiB (OversizedPipelines) — dấu hiệu client đang
//	gửi batch lệnh quá lớn, có thể gây áp lực bộ nhớ và tăng latency toàn node.
func (s *DiagnosticsService) fetchPipelineReport(ctx context.Context, addr string) (PipelineReport, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	client := sentinel.NewDirectClient(addr, s.cfg.RedisPassword)
	defer client.Close()

	report := PipelineReport{NodeAddr: addr}

	statsRaw, err := client.Info(ctx, "stats").Result()
	if err != nil {
		return PipelineReport{}, fmt.Errorf("INFO stats on %s: %w", addr, sentinel.ErrNodeUnreachable)
	}
	statsFields := parseInfoSection(statsRaw)
	report.RejectedCalls, _ = strconv.ParseInt(statsFields["rejected_calls"], 10, 64)

	clientsRaw, err := client.Info(ctx, "clients").Result()
	if err != nil {
		s.logger.Warn("INFO clients failed", zap.String("node", addr), zap.Error(err))
	} else {
		clientsFields := parseInfoSection(clientsRaw)
		report.MaxInputBufferBytes, _ = strconv.ParseInt(clientsFields["client_recent_max_input_buffer"], 10, 64)
	}

	errorRaw, err := client.Info(ctx, "errorstats").Result()
	if err != nil {
		s.logger.Debug("INFO errorstats failed", zap.String("node", addr), zap.Error(err))
	} else {
		// Mỗi dòng: "errorstat_<TYPE>:count=N,first_seen=...,last_seen=..."
		for _, line := range strings.Split(errorRaw, "\r\n") {
			if strings.HasPrefix(line, "errorstat_EXECABORT:") {
				report.EXECAbortCount = extractCount(line)
			}
		}
	}

	clientListRaw, err := client.ClientList(ctx).Result()
	if err != nil {
		s.logger.Debug("CLIENT LIST failed", zap.String("node", addr), zap.Error(err))
	} else {
		for _, line := range strings.Split(clientListRaw, "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			fields := parseKVLine(line)
			if qbuf, _ := strconv.ParseInt(fields["qbuf"], 10, 64); qbuf > oversizedPipelineThreshold {
				report.OversizedPipelines++
			}
		}
	}

	return report, nil
}

// parseKVLine parses a space-separated "key=value" line.
// Duplicate of monitor.go's parseKVLine — kept local to avoid circular package deps.
func parseKVLine(line string) map[string]string {
	fields := make(map[string]string)
	for _, token := range strings.Fields(line) {
		idx := strings.IndexByte(token, '=')
		if idx < 0 {
			continue
		}
		fields[token[:idx]] = token[idx+1:]
	}
	return fields
}

// parseInfoSection parses the key:value\r\n format returned by Redis INFO.
func parseInfoSection(raw string) map[string]string {
	result := make(map[string]string)
	for _, line := range strings.Split(raw, "\r\n") {
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			result[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}
	return result
}

// extractCount parses "errorstat_EXECABORT:count=N,..." and returns N.
func extractCount(line string) int64 {
	colonIdx := strings.IndexByte(line, ':')
	if colonIdx < 0 {
		return 0
	}
	for _, kv := range strings.Split(line[colonIdx+1:], ",") {
		if strings.HasPrefix(kv, "count=") {
			n, _ := strconv.ParseInt(strings.TrimPrefix(kv, "count="), 10, 64)
			return n
		}
	}
	return 0
}
