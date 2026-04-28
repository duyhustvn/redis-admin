package connection

import (
	"context"
	"errors"
	"fmt"
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
	Reason     string `json:"reason"`
	Severity   string `json:"severity"` // warning | critical
}

// Service exposes connection monitoring and distribution operations.
type Service interface {
	GetConnections(ctx context.Context) ([]NodeConnections, error)
	GetDistribution(ctx context.Context) ([]ReplicaDistribution, error)
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

// GetConnections fetches CLIENT LIST from every node, groups by source IP,
// enriches with pod metadata
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

// fetchNodeConnections runs CLIENT LIST on addr, aggregates by source IP
func (s *ConnectionService) fetchNodeConnections(ctx context.Context, addr, role string) (NodeConnections, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	client := sentinel.NewDirectClient(addr, s.cfg.RedisPassword)
	defer client.Close()

	raw, err := client.ClientList(ctx).Result()
	if err != nil {
		return NodeConnections{}, fmt.Errorf("CLIENT LIST on %s: %w", addr, err)
	}

	// Aggregate raw connections by source IP.
	byIP := make(map[string]*ClientInfo)
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := parseKVLine(line)

		// bỏ qua connection nội bộ Redis như replication/sentinel/internal link.
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
		enrichWithPodInfo(ci, s.podCache)
		nc.Clients = append(nc.Clients, *ci)
	}

	// Detect suspicious clients (connection leak / abnormal)
	for _, ci := range nc.Clients {

		// Rule 3: severe leak conn_count >= 5 AND max_idle_sec > 1 ngày
		if ci.ConnCount >= 5 && ci.MaxIdleSec > 86400 {
			nc.Suspicious = append(nc.Suspicious, SuspiciousClient{
				SourceAddr: ci.SourceAddr,
				Reason:     "high connections with very long idle (>1d)",
				Severity:   "critical",
			})
			continue
		}

		// Rule 1: possible leak conn_count >= 3 AND max_idle_sec > 300s (5 phút)
		if ci.ConnCount >= 3 && ci.MaxIdleSec > 300 {
			nc.Suspicious = append(nc.Suspicious, SuspiciousClient{
				SourceAddr: ci.SourceAddr,
				Reason:     "multiple connections with idle >5m",
				Severity:   "warning",
			})
			continue
		}

		// Rule 2: zombie max_idle_sec > 3600 (1 giờ)
		if ci.MaxIdleSec > 3600 {
			nc.Suspicious = append(nc.Suspicious, SuspiciousClient{
				SourceAddr: ci.SourceAddr,
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
