# Project Structure

```
redis-sentinel-admin/
├── CLAUDE.md                        # Claude Code project guide (you are here)
├── README.md
├── config.example.yaml
├── go.mod
├── go.sum
├── Dockerfile
├── k8s/
│   ├── deployment.yaml              # K8s Deployment manifest
│   ├── service.yaml                 # ClusterIP service :8080
│   ├── configmap.yaml               # config.yaml as ConfigMap
│   ├── rbac.yaml                    # ServiceAccount + ClusterRole for pod watching
│   └── ingress.yaml                 # Optional: internal ingress
│
├── cmd/
│   └── rsa-server/
│       └── main.go                  # Entrypoint: start HTTP server + background workers
│
├── internal/
│   │
│   ├── config/
│   │   ├── config.go                # Config struct + viper loader
│   │   └── config_test.go
│   │
│   ├── sentinel/
│   │   ├── client.go                # Sentinel connection factory
│   │   ├── topology.go              # Topology polling + NodeInfo mapping
│   │   ├── pubsub.go                # Sentinel Pub/Sub event listener + flap detector
│   │   ├── quorum.go                # SENTINEL CKQUORUM validator
│   │   └── *_test.go
│   │
│   ├── replication/
│   │   ├── tracker.go               # Replication offset trend (ring buffer per replica)
│   │   ├── resync.go                # Full resync counter + backlog-size advisor
│   │   └── *_test.go
│   │
│   ├── keys/
│   │   ├── scanner.go               # Non-blocking big key SCAN (async, results via channel)
│   │   ├── hotkey.go                # LFU-based hot key detector (OBJECT FREQ)
│   │   ├── ttl.go                   # TTL health report per namespace prefix
│   │   └── *_test.go
│   │
│   ├── connection/
│   │   ├── monitor.go               # INFO CLIENTS poller
│   │   ├── mapper.go                # IP → Pod/Namespace/Deployment mapper
│   │   ├── distribution.go          # Read/Write load % per replica (INFO COMMANDSTATS)
│   │   └── *_test.go
│   │
│   ├── diagnostics/
│   │   ├── slowlog.go               # Slowlog puller + cross-node aggregator + ranker
│   │   ├── pipeline.go              # MULTI/EXEC abort + oversized pipeline detector
│   │   ├── memory.go                # Fragmentation ratio + eviction rate + RDB/AOF health
│   │   └── *_test.go
│   │
│   ├── k8s/
│   │   ├── informer.go              # Pod informer + IP→Pod cache (RWMutex)
│   │   ├── throttle.go              # CPU throttle detector via metrics-server API
│   │   └── *_test.go
│   │
│   ├── operations/
│   │   ├── failover.go              # Graceful failover: lag-check → drain → promote → rollback
│   │   ├── configdiff.go            # CONFIG GET * cross-node diff
│   │   ├── audit.go                 # Config change audit log (in-memory ring + optional file)
│   │   ├── notify.go                # Webhook notifier (Slack / generic webhook)
│   │   └── *_test.go
│   │
│   ├── chaos/
│   │   ├── seeder.go                # Dummy data generator (configurable size/type/prefix)
│   │   └── trigger.go               # Chaos failover trigger (SENTINEL FAILOVER or pod delete)
│   │
│   ├── metrics/
│   │   └── exporter.go              # Prometheus metrics exporter (:9090/metrics)
│   │
│   └── api/
│       ├── server.go                # Gin engine setup, middleware, graceful shutdown
│       ├── routes.go                # Route registration (all /api/v1/... grouped here)
│       ├── response.go              # Unified JSON response helpers: OK(), Err(), SSE()
│       └── handlers/
│           ├── topology.go          # GET /api/v1/topology
│           ├── replication.go       # GET /api/v1/replication/lag
│           │                        # GET /api/v1/replication/resync-stats
│           ├── connections.go       # GET /api/v1/connections
│           │                        # GET /api/v1/connections/distribution
│           ├── slowlog.go           # GET /api/v1/diagnostics/slowlog
│           │                        # GET /api/v1/diagnostics/pipeline
│           ├── keys.go              # GET /api/v1/keys/hotkeys
│           │                        # GET /api/v1/keys/bigkeys  (SSE stream)
│           │                        # GET /api/v1/keys/ttl-report
│           ├── memory.go            # GET /api/v1/diagnostics/memory
│           ├── config.go            # GET /api/v1/config/diff
│           │                        # GET /api/v1/config/audit
│           │                        # POST /api/v1/config/set
│           ├── operations.go        # POST /api/v1/ops/failover
│           │                        # POST /api/v1/ops/chaos/seed
│           │                        # POST /api/v1/ops/chaos/failover
│           ├── events.go            # GET /api/v1/events/stream  (SSE)
│           └── health.go            # GET /healthz, GET /readyz
│
├── docs/
│   ├── STRUCTURE.md                 # This file
│   ├── PHASES.md                    # Development phases + progress
│   └── api-reference.md             # Full API endpoint documentation
│
├── skills/
│   ├── redis-patterns.md            # Redis/Sentinel API patterns, safe vs unsafe
│   ├── k8s-integration.md           # client-go informer, IP→Pod mapping, RBAC
│   ├── api-patterns.md              # Gin conventions, response format, SSE
│   └── error-handling.md            # Error wrapping, typed errors, retry
│
├── integration/
│   ├── sentinel_test.go             # Integration tests (real Redis Sentinel)
│   └── docker-compose.yml           # Local Sentinel cluster: 1 master, 2 replicas, 3 sentinels
│
└── scripts/
    ├── setup-sentinel.sh            # Bootstrap local Sentinel cluster
    └── gen-load.sh                  # Quick load generation for manual testing
```

## Package Dependency Rules

```
cmd/rsa-server/ → internal/api, internal/config (bootstrap only)

internal/api/         → internal/sentinel, internal/replication, internal/keys,
                        internal/connection, internal/diagnostics, internal/operations,
                        internal/k8s, internal/chaos, internal/metrics

internal/operations/  → internal/sentinel, internal/replication, internal/k8s
internal/connection/  → internal/k8s, internal/sentinel
internal/keys/        → internal/sentinel
internal/diagnostics/ → internal/sentinel
internal/replication/ → internal/sentinel
internal/metrics/     → internal/* (reads from all modules)
internal/chaos/       → internal/sentinel, internal/k8s

# FORBIDDEN:
# internal/sentinel/    must NOT import any other internal package
# internal/k8s/         must NOT import internal/sentinel/
# internal/config/      must NOT import any internal package
```

## Key Types

```go
// internal/sentinel/topology.go
type TopologySnapshot struct {
    Master     NodeInfo
    Replicas   []NodeInfo
    Sentinels  []NodeInfo
    QuorumOK   bool
    CapturedAt time.Time
}

type NodeInfo struct {
    Addr             string
    Role             string // "master" | "replica" | "sentinel"
    IsHealthy        bool
    ConnectedClients int64
    UptimeSeconds    int64
}

// internal/keys/scanner.go
type KeyReport struct {
    Key       string
    Namespace string        // prefix before first ":"
    SizeBytes int64
    TTL       time.Duration // -1 = no TTL
    Type      string        // string | hash | list | set | zset
}

// internal/connection/mapper.go
type ClientInfo struct {
    Addr       string
    PodName    string
    Namespace  string
    Deployment string
    ConnCount  int
}

// internal/api/response.go
type APIResponse struct {
    Data  interface{} `json:"data"`
    Error *APIError   `json:"error"`
}

type APIError struct {
    Code    string `json:"code"`
    Message string `json:"message"`
}
```

## API Endpoint Summary

| Method | Path | Description |
|---|---|---|
| GET | `/healthz` | Liveness probe |
| GET | `/readyz` | Readiness probe (checks Redis reachable) |
| GET | `/api/v1/topology` | Full cluster topology snapshot |
| GET | `/api/v1/replication/lag` | Per-replica lag in bytes |
| GET | `/api/v1/replication/resync-stats` | Full resync count + advisor |
| GET | `/api/v1/connections` | Per-node client connection counts |
| GET | `/api/v1/connections/distribution` | Read/write % per replica |
| GET | `/api/v1/diagnostics/slowlog` | Top slowlog entries across cluster |
| GET | `/api/v1/diagnostics/pipeline` | MULTI/EXEC abort stats |
| GET | `/api/v1/diagnostics/memory` | Frag ratio, eviction rate, RDB/AOF |
| GET | `/api/v1/keys/hotkeys?top=N` | Top N hot keys (LFU) |
| GET | `/api/v1/keys/bigkeys?threshold_bytes=N` | Big key scan results (SSE) |
| GET | `/api/v1/keys/ttl-report` | TTL health per namespace |
| GET | `/api/v1/config/diff` | Cross-node config diff |
| GET | `/api/v1/config/audit` | Recent config change log |
| POST | `/api/v1/config/set` | Set config on node (requires confirm) |
| POST | `/api/v1/ops/failover` | Graceful failover (requires confirm) |
| POST | `/api/v1/ops/chaos/seed` | Seed dummy data |
| POST | `/api/v1/ops/chaos/failover` | Chaos failover trigger |
| GET | `/api/v1/events/stream` | SSE stream of Sentinel events |
| GET | `/metrics` | Prometheus metrics |
