package sentinel

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/duydinhle/redis-sentinel-admin/internal/config"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// NodeInfo describes a single Redis or Sentinel node.
type NodeInfo struct {
	Addr             string `json:"addr"`
	Role             string `json:"role"` // "master" | "replica" | "sentinel"
	IsHealthy        bool   `json:"is_healthy"`
	ConnectedClients int64  `json:"connected_clients"`
	UptimeSeconds    int64  `json:"uptime_seconds"`
}

// TopologySnapshot captures the full cluster state at a point in time.
type TopologySnapshot struct {
	Master     NodeInfo   `json:"master"`
	Replicas   []NodeInfo `json:"replicas"`
	Sentinels  []NodeInfo `json:"sentinels"`
	QuorumOK   bool       `json:"quorum_ok"`
	CapturedAt time.Time  `json:"captured_at"`
}

// NodeAddresses holds the current master and replica addresses resolved from Sentinel.
type NodeAddresses struct {
	Master   string
	Replicas []string
}

// Service exposes sentinel topology operations used by the API layer.
type Service interface {
	GetTopology(ctx context.Context) (*TopologySnapshot, error)
	// GetNodeAddresses is a lightweight call that returns master + replica addrs
	// without fetching full INFO for each node.
	GetNodeAddresses(ctx context.Context) (*NodeAddresses, error)
	IsReady(ctx context.Context) error
}

// TopologyService implements Service using Redis Sentinel.
type TopologyService struct {
	cfg    *config.Config
	logger *zap.Logger
	quorum *QuorumChecker
}

// NewTopologyService creates a TopologyService.
func NewTopologyService(cfg *config.Config, logger *zap.Logger) *TopologyService {
	return &TopologyService{
		cfg:    cfg,
		logger: logger,
		quorum: NewQuorumChecker(cfg, logger),
	}
}

// GetTopology queries the first reachable Sentinel for master, replica, and
// sentinel node lists, enriches each with INFO data, then validates quorum.
func (s *TopologyService) GetTopology(ctx context.Context) (*TopologySnapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	sc, sentinelAddr, err := s.reachableSentinel(ctx)
	if err != nil {
		return nil, fmt.Errorf("get topology: %w", ErrNodeUnreachable)
	}
	defer sc.Close()

	snap := &TopologySnapshot{
		CapturedAt: time.Now().UTC(),
	}

	// Master address
	masterParts, err := sc.GetMasterAddrByName(ctx, s.cfg.MasterName).Result()
	if err != nil || len(masterParts) < 2 {
		return nil, fmt.Errorf("get master from sentinel %s: %w", sentinelAddr, ErrNoMaster)
	}
	masterAddr := masterParts[0] + ":" + masterParts[1]
	snap.Master = s.fetchNodeInfo(ctx, masterAddr, "master")

	// Replicas
	replicaMaps, err := sc.Replicas(ctx, s.cfg.MasterName).Result()
	if err != nil {
		s.logger.Warn("failed to list replicas", zap.Error(err))
	}
	for _, rm := range replicaMaps {
		ip, port := rm["ip"], rm["port"]
		if ip == "" || port == "" {
			continue
		}
		addr := ip + ":" + port
		flags := rm["flags"]
		node := s.fetchNodeInfo(ctx, addr, "replica")
		if sentinelFlagsUnhealthy(flags) {
			node.IsHealthy = false
		}
		snap.Replicas = append(snap.Replicas, node)
	}

	// Peer sentinels (the one we're connected to is not included in this list)
	sentinelMaps, err := sc.Sentinels(ctx, s.cfg.MasterName).Result()
	if err != nil {
		s.logger.Warn("failed to list sentinels", zap.Error(err))
	}
	for _, sm := range sentinelMaps {
		ip, port := sm["ip"], sm["port"]
		if ip == "" || port == "" {
			continue
		}
		addr := ip + ":" + port
		flags := sm["flags"]
		snap.Sentinels = append(snap.Sentinels, NodeInfo{
			Addr:      addr,
			Role:      "sentinel",
			IsHealthy: !sentinelFlagsUnhealthy(flags),
		})
	}
	// Add the sentinel we connected to (it omits itself from its own list).
	snap.Sentinels = append(snap.Sentinels, NodeInfo{
		Addr:      sentinelAddr,
		Role:      "sentinel",
		IsHealthy: true,
	})

	snap.QuorumOK = s.quorum.Check(ctx)

	return snap, nil
}

// IsReady pings the master via the failover client; used by the readiness probe.
func (s *TopologyService) IsReady(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	client := NewMasterClient(s.cfg)
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("ping master: %w", err)
	}
	return nil
}

// reachableSentinel returns a SentinelClient for the first sentinel that responds
// to PING, plus that sentinel's address.
func (s *TopologyService) reachableSentinel(ctx context.Context) (*redis.SentinelClient, string, error) {
	for _, addr := range s.cfg.SentinelAddrs {
		sc := NewSentinelManagementClient(addr, s.cfg.SentinelPassword)
		if err := sc.Ping(ctx).Err(); err != nil {
			sc.Close()
			continue
		}
		return sc, addr, nil
	}
	return nil, "", ErrNodeUnreachable
}

// fetchNodeInfo connects directly to a node and parses INFO clients + server.
//
// Redis command: INFO clients server
//
// Fields extracted:
//   - connected_clients  (section: clients) → NodeInfo.ConnectedClients
//   - uptime_in_seconds  (section: server)  → NodeInfo.UptimeSeconds
//
// On failure it returns a NodeInfo with IsHealthy=false rather than propagating
// the error — topology should still return even if individual nodes are unreachable.
func (s *TopologyService) fetchNodeInfo(ctx context.Context, addr, role string) NodeInfo {
	node := NodeInfo{Addr: addr, Role: role, IsHealthy: false}

	client := NewDirectClient(addr, s.cfg.RedisPassword)
	defer client.Close()

	// INFO clients server trả về text dạng "key:value\r\n" theo từng section.
	// parseInfoSection gộp tất cả section thành một map phẳng để dễ tra cứu.
	raw, err := client.Info(ctx, "clients", "server").Result()
	if err != nil {
		s.logger.Warn("INFO failed on node",
			zap.String("addr", addr),
			zap.String("role", role),
			zap.Error(err),
		)
		return node
	}

	fields := parseInfoSection(raw)
	node.IsHealthy = true
	if v, ok := fields["connected_clients"]; ok {
		node.ConnectedClients, _ = strconv.ParseInt(v, 10, 64)
	}
	if v, ok := fields["uptime_in_seconds"]; ok {
		node.UptimeSeconds, _ = strconv.ParseInt(v, 10, 64)
	}
	return node
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

// GetNodeAddresses resolves the current master and replica addresses from Sentinel
// without fetching INFO for each node. Cheaper than GetTopology.
func (s *TopologyService) GetNodeAddresses(ctx context.Context) (*NodeAddresses, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	sc, sentinelAddr, err := s.reachableSentinel(ctx)
	if err != nil {
		return nil, fmt.Errorf("get node addresses: %w", ErrNodeUnreachable)
	}
	defer sc.Close()

	masterParts, err := sc.GetMasterAddrByName(ctx, s.cfg.MasterName).Result()
	if err != nil || len(masterParts) < 2 {
		return nil, fmt.Errorf("get master from sentinel %s: %w", sentinelAddr, ErrNoMaster)
	}

	addrs := &NodeAddresses{Master: masterParts[0] + ":" + masterParts[1]}

	replicaMaps, err := sc.Replicas(ctx, s.cfg.MasterName).Result()
	if err != nil {
		s.logger.Warn("failed to list replicas for node addresses", zap.Error(err))
	}
	for _, rm := range replicaMaps {
		ip, port := rm["ip"], rm["port"]
		if ip == "" || port == "" {
			continue
		}
		addrs.Replicas = append(addrs.Replicas, ip+":"+port)
	}
	return addrs, nil
}

// sentinelFlagsUnhealthy returns true when sentinel flags indicate a node is down.
func sentinelFlagsUnhealthy(flags string) bool {
	return strings.Contains(flags, "s_down") ||
		strings.Contains(flags, "o_down") ||
		strings.Contains(flags, "disconnected")
}
