package connection

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/duydinhle/redis-sentinel-admin/internal/config"
	"github.com/duydinhle/redis-sentinel-admin/internal/k8s"
	"github.com/duydinhle/redis-sentinel-admin/internal/sentinel"
	"go.uber.org/zap"
)

// ClientInfo aggregates all connections from the same source IP into one record.
type ClientInfo struct {
	SourceAddr   string `json:"source_addr"`
	PodName      string `json:"pod_name,omitempty"`
	Namespace    string `json:"namespace,omitempty"`
	Deployment   string `json:"deployment,omitempty"`
	ConnCount    int    `json:"conn_count"`
	MaxIdleSec   int64  `json:"max_idle_sec"`
	MaxQBufBytes int64  `json:"max_qbuf_bytes"`
}

// NodeConnections holds aggregated connection data for one Redis node.
type NodeConnections struct {
	NodeAddr      string             `json:"node_addr"`
	Role          string             `json:"role"`
	Total         int                `json:"total"`
	UniqueClients int                `json:"unique_clients"`
	Clients       []ClientInfo       `json:"clients"`
	Suspicious    []SuspiciousClient `json:"suspicious,omitempty"`
}

type SuspiciousClient struct {
	SourceAddr string `json:"source_addr"`
	PodName    string `json:"pod_name,omitempty"`
	Namespace  string `json:"namespace,omitempty"`
	Deployment string `json:"deployment,omitempty"`
	Reason     string `json:"reason"`
	Severity   string `json:"severity"` // warning | critical
}

type AnalysisSummary struct {
	TotalConnections int `json:"total_connections"`
	UniqueClients    int `json:"unique_clients"`
	SuspiciousCount  int `json:"suspicious_count"`
	CriticalCount    int `json:"critical_count"`
}

type AnalysisResponse struct {
	Summary          AnalysisSummary    `json:"summary"`
	TopByConnections []ClientInfo       `json:"top_by_connections"`
	TopByIdle        []ClientInfo       `json:"top_by_idle"`
	Suspicious       []SuspiciousClient `json:"suspicious"`
	Recommendations  []string           `json:"recommendations"`
}

// Service exposes connection monitoring and distribution operations.
type Service interface {
	GetConnections(ctx context.Context) ([]NodeConnections, error)
	GetDistribution(ctx context.Context) ([]ReplicaDistribution, error)
	AnalyzeConnections(ctx context.Context) (*AnalysisResponse, error)
}

// ConnectionService implements Service.
type ConnectionService struct {
	cfg         *config.Config
	sentinelSvc sentinel.Service
	podCache    *k8s.PodCache
	logger      *zap.Logger
}

// New creates a ConnectionService.
func New(
	cfg *config.Config,
	svc sentinel.Service,
	cache *k8s.PodCache,
	logger *zap.Logger,
) *ConnectionService {
	return &ConnectionService{
		cfg:         cfg,
		sentinelSvc: svc,
		podCache:    cache,
		logger:      logger,
	}
}

// GetConnections lấy danh sách connection từ tất cả node trong cluster (master + replicas),
// gom nhóm theo source IP, và đánh dấu các client bất thường.
//
// Luồng xử lý:
//  1. GetNodeAddresses → hỏi Sentinel để lấy địa chỉ master và toàn bộ replica.
//  2. Với mỗi node gọi fetchNodeConnections (CLIENT LIST) để lấy raw connection list.
//  3. Kết quả mỗi node được trả về độc lập dưới dạng NodeConnections — caller
//     (AnalyzeConnections) sẽ merge các node lại thành view toàn cluster.
//
// Lỗi từng node không dừng toàn bộ hàm — node nào lỗi thì bị skip và lỗi được
// gom vào errors.Join để trả về cùng kết quả partial.
func (s *ConnectionService) GetConnections(ctx context.Context) ([]NodeConnections, error) {
	addrs, err := s.sentinelSvc.GetNodeAddresses(ctx)
	if err != nil {
		return nil, fmt.Errorf("get connections: %w", err)
	}

	type nodeSpec struct{ addr, role string }
	nodes := []nodeSpec{{addrs.Master, "master"}}
	for _, r := range addrs.Replicas {
		nodes = append(nodes, nodeSpec{r, "replica"})
	}

	var results []NodeConnections
	var errs []error
	for _, n := range nodes {
		nc, err := s.fetchNodeConnections(ctx, n.addr, n.role)
		if err != nil {
			s.logger.Warn("failed to fetch connections",
				zap.String("node", n.addr),
				zap.Error(err),
			)
			errs = append(errs, fmt.Errorf("node %s: %w", n.addr, err))
			continue
		}
		results = append(results, nc)
	}
	return results, errors.Join(errs...)
}

// fetchNodeConnections chạy CLIENT LIST trên một node, gom nhóm theo source IP,
// bổ sung pod metadata từ K8s cache, rồi phát hiện connection bất thường.
//
// Redis command: CLIENT LIST
//
// Output format — mỗi dòng là một connection đang mở, các field cách nhau bằng dấu cách:
//
//	id=123 addr=10.0.0.5:54321 laddr=10.0.0.1:6379 fd=8 name= age=0 idle=120
//	cmd=get flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=32768 ...
//
// Các field được sử dụng:
//   - addr    → "IP:port" của client; lấy phần IP để gom nhóm (nhiều conn cùng pod → cùng IP)
//   - flags   → bỏ qua dòng có flag "S" (slave/replica link) và "M" (master link) —
//               đây là kết nối nội bộ Redis, không phải client ứng dụng
//   - idle    → số giây không có lệnh nào; giữ giá trị lớn nhất trong group (MaxIdleSec)
//   - qbuf   → số byte đang chờ trong input buffer; giữ giá trị lớn nhất (MaxQBufBytes)
//
// Sau khi gom nhóm:
//   - enrichWithPodInfo tra cứu K8s PodCache bằng source IP để điền PodName/Namespace/Deployment.
//
// Phát hiện suspicious (áp dụng theo thứ tự ưu tiên, chỉ gán một rule cho mỗi client):
//   - critical: ConnCount >= 5 VÀ MaxIdleSec > 86400 (1 ngày) → rò rỉ connection nghiêm trọng
//   - warning:  ConnCount >= 3 VÀ MaxIdleSec > 300  (5 phút) → có khả năng rò rỉ pool
//   - warning:  MaxIdleSec > 3600 (1 giờ) đơn thuần      → zombie connection
func (s *ConnectionService) fetchNodeConnections(ctx context.Context, addr, role string) (NodeConnections, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	client := sentinel.NewDirectClient(addr, s.cfg.RedisPassword)
	defer client.Close()

	raw, err := client.ClientList(ctx).Result()
	if err != nil {
		return NodeConnections{}, fmt.Errorf("CLIENT LIST on %s: %w", addr, err)
	}

	// Gom nhóm các connection theo source IP.
	byIP := make(map[string]*ClientInfo)
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := parseKVLine(line)

		// Flag "S" = slave/replica link — kết nối nội bộ Redis, không phải client ứng dụng.
		if strings.Contains(fields["flags"], "S") {
			continue
		}

		srcAddr := fields["addr"]
		if srcAddr == "" {
			continue
		}
		ip := sourceIP(srcAddr)
		ci, ok := byIP[ip]
		if !ok {
			ci = &ClientInfo{SourceAddr: ip}
			byIP[ip] = ci
		}
		ci.ConnCount++
		if idle, _ := strconv.ParseInt(fields["idle"], 10, 64); idle > ci.MaxIdleSec {
			ci.MaxIdleSec = idle
		}
		if qbuf, _ := strconv.ParseInt(fields["qbuf"], 10, 64); qbuf > ci.MaxQBufBytes {
			ci.MaxQBufBytes = qbuf
		}
	}

	totalConnections := 0
	for _, ci := range byIP {
		totalConnections += ci.ConnCount
	}

	nc := NodeConnections{NodeAddr: addr, Role: role, Total: totalConnections, UniqueClients: len(byIP)}
	for _, ci := range byIP {
		// Tra K8s cache để gắn tên Pod/Namespace/Deployment vào từng nhóm IP.
		enrichWithPodInfo(ci, s.podCache)
		nc.Clients = append(nc.Clients, *ci)
	}

	// Phát hiện suspicious — kiểm tra theo thứ tự ưu tiên (critical trước warning).
	for _, ci := range nc.Clients {

		// Rule critical: nhiều connection lâu ngày không đóng → rò rỉ nghiêm trọng.
		if ci.ConnCount >= 5 && ci.MaxIdleSec > 86400 {
			nc.Suspicious = append(nc.Suspicious, SuspiciousClient{
				SourceAddr: ci.SourceAddr,
				PodName:    ci.PodName,
				Namespace:  ci.Namespace,
				Deployment: ci.Deployment,
				Reason:     "high connections with very long idle (>1d)",
				Severity:   "critical",
			})
			continue
		}

		// Rule warning: pool mở nhiều connection nhưng không dùng → có thể rò rỉ pool.
		if ci.ConnCount >= 3 && ci.MaxIdleSec > 300 {
			nc.Suspicious = append(nc.Suspicious, SuspiciousClient{
				SourceAddr: ci.SourceAddr,
				PodName:    ci.PodName,
				Namespace:  ci.Namespace,
				Deployment: ci.Deployment,
				Reason:     "multiple connections with idle >5m",
				Severity:   "warning",
			})
			continue
		}

		// Rule warning: connection đơn lẻ idle quá lâu → zombie, nên bị TCP keepalive đóng.
		if ci.MaxIdleSec > 3600 {
			nc.Suspicious = append(nc.Suspicious, SuspiciousClient{
				SourceAddr: ci.SourceAddr,
				PodName:    ci.PodName,
				Namespace:  ci.Namespace,
				Deployment: ci.Deployment,
				Reason:     "idle >1h",
				Severity:   "warning",
			})
		}
	}
	return nc, nil
}

// parseKVLine parses a space-separated "key=value" line (CLIENT LIST format).
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

func (s *ConnectionService) AnalyzeConnections(ctx context.Context) (*AnalysisResponse, error) {
	nodes, err := s.GetConnections(ctx)
	if err != nil {
		return nil, err
	}

	// Flatten toàn bộ clients trong cluster.
	var allSuspicious []SuspiciousClient

	// Aggregate theo source IP trên toàn cluster.
	clientMap := make(map[string]*ClientInfo)

	for _, n := range nodes {
		for _, c := range n.Clients {

			existing, ok := clientMap[c.SourceAddr]
			if !ok {
				tmp := c
				clientMap[c.SourceAddr] = &tmp
				continue
			}

			// Merge connections từ nhiều Redis nodes.
			existing.ConnCount += c.ConnCount

			if c.MaxIdleSec > existing.MaxIdleSec {
				existing.MaxIdleSec = c.MaxIdleSec
			}

			if c.MaxQBufBytes > existing.MaxQBufBytes {
				existing.MaxQBufBytes = c.MaxQBufBytes
			}
		}

		// Merge suspicious results từ từng node.
		allSuspicious = append(allSuspicious, n.Suspicious...)
	}

	// Convert map -> slice
	var allClients []ClientInfo
	for _, c := range clientMap {
		allClients = append(allClients, *c)
	}

	// ===== Top by connections =====
	connCopy := append([]ClientInfo(nil), allClients...)

	sort.Slice(connCopy, func(i, j int) bool {
		return connCopy[i].ConnCount > connCopy[j].ConnCount
	})

	topConn := topN(connCopy, 10)

	// ===== Top by idle =====
	idleCopy := append([]ClientInfo(nil), allClients...)

	sort.Slice(idleCopy, func(i, j int) bool {
		return idleCopy[i].MaxIdleSec > idleCopy[j].MaxIdleSec
	})

	topIdle := topN(idleCopy, 10)

	// Các pod kiểu:
	// - redis-cmd
	// - redis-commander
	// thường giữ connection lâu nhưng không phải leak thật.
	filteredSuspicious := make([]SuspiciousClient, 0, len(allSuspicious))

	for _, s := range allSuspicious {
		switch s.Deployment {
		case "redis-cmd", "redis-commander":
			// Skip operational/admin tools.
			continue
		}
		filteredSuspicious = append(filteredSuspicious, s)
	}

	// Summary
	totalConnections := 0
	for _, c := range allClients {
		totalConnections += c.ConnCount
	}

	criticalCount := 0
	for _, s := range filteredSuspicious {
		if s.Severity == "critical" {
			criticalCount++
		}
	}

	summary := AnalysisSummary{
		TotalConnections: totalConnections,
		UniqueClients:    len(allClients),
		SuspiciousCount:  len(filteredSuspicious),
		CriticalCount:    criticalCount,
	}

	// Recommendations
	recommendations := buildRecommendations(allClients, filteredSuspicious)

	return &AnalysisResponse{
		Summary:          summary,
		TopByConnections: topConn,
		TopByIdle:        topIdle,
		Suspicious:       filteredSuspicious,
		Recommendations:  recommendations,
	}, nil
}

func topN(clients []ClientInfo, n int) []ClientInfo {
	if len(clients) < n {
		return clients
	}
	return clients[:n]
}

func buildRecommendations(clients []ClientInfo, suspicious []SuspiciousClient) []string {
	var recs []string

	for _, s := range suspicious {
		if s.Severity == "critical" {
			recs = append(recs, "Critical: some clients have connection leak (high conn + long idle)")
			break
		}
	}

	for _, c := range clients {
		if c.MaxIdleSec > 3600 {
			recs = append(recs, "Some connections idle >1h → check Redis pool config")
			break
		}
	}

	for _, c := range clients {
		if c.ConnCount > 10 {
			recs = append(recs, "Some clients open too many connections → check pooling")
			break
		}
	}

	if len(recs) == 0 {
		recs = append(recs, "No obvious issues detected")
	}

	return recs
}
