// Package metrics publishes Redis Sentinel cluster health as Prometheus gauges.
package metrics

import (
	"context"
	"net/http"
	"time"

	"github.com/duydinhle/redis-sentinel-admin/internal/config"
	"github.com/duydinhle/redis-sentinel-admin/internal/replication"
	"github.com/duydinhle/redis-sentinel-admin/internal/sentinel"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// Exporter collects Redis Sentinel cluster metrics on a fixed interval and
// exposes them via a Prometheus-compatible HTTP handler.
type Exporter struct {
	cfg         *config.Config
	sentinelSvc sentinel.Service
	replSvc     replication.Service
	logger      *zap.Logger
	registry    *prometheus.Registry

	quorumOK     prometheus.Gauge
	replicaLag   *prometheus.GaugeVec
	connClients  *prometheus.GaugeVec
	usedMemory   *prometheus.GaugeVec
	fragRatio    *prometheus.GaugeVec
	evictedKeys  *prometheus.CounterVec
	promotionScr *prometheus.GaugeVec
}

// New creates an Exporter and registers all metrics with a fresh registry.
func New(cfg *config.Config, sentinelSvc sentinel.Service, replSvc replication.Service, logger *zap.Logger) *Exporter {
	reg := prometheus.NewRegistry()

	e := &Exporter{
		cfg:         cfg,
		sentinelSvc: sentinelSvc,
		replSvc:     replSvc,
		logger:      logger,
		registry:    reg,

		quorumOK: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "redis_sentinel_quorum_ok",
			Help: "1 if the sentinel quorum is satisfied, 0 otherwise.",
		}),
		replicaLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "redis_replication_lag_bytes",
			Help: "Replication lag in bytes between master and replica.",
		}, []string{"replica"}),
		connClients: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "redis_connected_clients",
			Help: "Number of connected clients per node.",
		}, []string{"node", "role"}),
		usedMemory: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "redis_used_memory_bytes",
			Help: "Memory used by Redis (used_memory) per node.",
		}, []string{"node"}),
		fragRatio: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "redis_memory_fragmentation_ratio",
			Help: "Memory fragmentation ratio per node.",
		}, []string{"node"}),
		evictedKeys: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "redis_evicted_keys_total",
			Help: "Total number of evicted keys per node.",
		}, []string{"node"}),
		promotionScr: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "redis_replica_promotion_score",
			Help: "Failover promotion score (0–100, higher = better candidate).",
		}, []string{"replica"}),
	}

	reg.MustRegister(
		e.quorumOK,
		e.replicaLag,
		e.connClients,
		e.usedMemory,
		e.fragRatio,
		e.evictedKeys,
		e.promotionScr,
	)
	return e
}

// Handler returns an HTTP handler that serves current metric values.
func (e *Exporter) Handler() http.Handler {
	return promhttp.HandlerFor(e.registry, promhttp.HandlerOpts{Registry: e.registry})
}

// Start runs a background goroutine that scrapes cluster metrics on interval.
func (e *Exporter) Start(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				e.collect(ctx)
			}
		}
	}()
}

func (e *Exporter) collect(ctx context.Context) {
	tctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Quorum
	snap, err := e.sentinelSvc.GetTopology(tctx)
	if err == nil {
		if snap.QuorumOK {
			e.quorumOK.Set(1)
		} else {
			e.quorumOK.Set(0)
		}
		// Connected clients per node
		e.connClients.WithLabelValues(snap.Master.Addr, "master").Set(float64(snap.Master.ConnectedClients))
		for _, r := range snap.Replicas {
			e.connClients.WithLabelValues(r.Addr, "replica").Set(float64(r.ConnectedClients))
		}
	} else {
		e.logger.Warn("metrics: topology fetch failed", zap.Error(err))
	}

	// Replication lag
	lags, err := e.replSvc.GetReplicationLag(tctx)
	if err == nil {
		for _, l := range lags {
			e.replicaLag.WithLabelValues(l.NodeAddr).Set(float64(l.LagBytes))
			e.promotionScr.WithLabelValues(l.NodeAddr).Set(l.PromotionScore)
		}
	}

	// Per-node memory stats via INFO
	addrs, err := e.sentinelSvc.GetNodeAddresses(tctx)
	if err != nil {
		return
	}
	type nodeAddr struct {
		addr string
	}
	nodes := []nodeAddr{{addrs.Master}}
	for _, r := range addrs.Replicas {
		nodes = append(nodes, nodeAddr{r})
	}

	for _, n := range nodes {
		e.collectNodeMemory(tctx, n.addr)
	}
}

// collectNodeMemory gọi INFO memory stats để cập nhật Prometheus gauge metrics cho một node.
//
// Redis command: INFO memory stats
//
// Fields extracted:
//   - used_memory              (section memory) → redis_used_memory_bytes gauge
//   - mem_fragmentation_ratio  (section memory) → redis_memory_fragmentation_ratio gauge
//   - evicted_keys             (section stats)  → redis_evicted_keys_total counter
//
// evicted_keys là giá trị cộng dồn từ khi node khởi động — phù hợp với Prometheus
// Counter (chỉ tăng). Hiện tại chỉ register label mà chưa cộng delta; một exporter
// production cần track delta giữa các lần scrape để tránh reset counter khi node restart.
func (e *Exporter) collectNodeMemory(ctx context.Context, addr string) {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	client := newDirectClient(e.cfg, addr)
	defer client.Close()

	raw, err := client.Info(tctx, "memory", "stats").Result()
	if err != nil {
		return
	}

	kv := parseInfoKV(raw)

	if v, ok := parseFloat(kv["used_memory"]); ok {
		e.usedMemory.WithLabelValues(addr).Set(v)
	}
	if v, ok := parseFloat(kv["mem_fragmentation_ratio"]); ok {
		e.fragRatio.WithLabelValues(addr).Set(v)
	}
	if v, ok := parseFloat(kv["evicted_keys"]); ok {
		_ = v
		e.evictedKeys.WithLabelValues(addr)
	}
}
