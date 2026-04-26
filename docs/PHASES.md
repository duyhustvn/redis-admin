# Development Phases

Status legend: ✅ Done | 🚧 In Progress | ⬜ Not Started

---

## Phase 1 — Core Foundation
> Goal: Service boots, connects to Sentinel, exposes basic topology and events via API. Swagger UI accessible.

| # | Feature | Package | Endpoint | Status |
|---|---|---|---|---|
| 1.1 | Sentinel connection factory | `internal/sentinel/client.go` | — | ✅ |
| 1.2 | Live topology polling (Master/Replica/Sentinel state) | `internal/sentinel/topology.go` | `GET /api/v1/topology` | ✅ |
| 1.3 | Sentinel Pub/Sub event listener (`+sdown`, `-sdown`, `+odown`, `+failover-*`) | `internal/sentinel/pubsub.go` | — | ✅ |
| 1.4 | Network flapping detector (rapid sdown/up cycles → alert) | `internal/sentinel/pubsub.go` | — | ✅ |
| 1.5 | Sentinel quorum validator (`SENTINEL CKQUORUM`) | `internal/sentinel/quorum.go` | included in topology | ✅ |
| 1.6 | K8s Pod informer + IP→Pod cache | `internal/k8s/informer.go` | — | ✅ |
| 1.7 | Echo HTTP server + graceful shutdown | `internal/api/server.go` | — | ✅ |
| 1.8 | Unified response format (`APIResponse`) | `internal/api/response.go` | — | ✅ |
| 1.9 | Global error handler (Echo custom) | `internal/api/server.go` | — | ✅ |
| 1.10 | Health + readiness probes | `internal/api/handlers/health.go` | `GET /healthz` `GET /readyz` | ✅ |
| 1.11 | SSE event stream for Sentinel events | `internal/api/handlers/events.go` | `GET /api/v1/events/stream` | ✅ |
| 1.12 | Config loader (viper + env) | `internal/config/config.go` | — | ✅ |
| 1.13 | Swaggo setup: `@title` block in main.go + `swag init` working | `cmd/rsa-server/main.go` | `GET /swagger/*` | ✅ |
| 1.14 | Dockerfile + K8s manifests (Deployment, Service, RBAC) | `k8s/` | — | ⬜ |

**Completion criteria**: `curl /api/v1/topology` returns live cluster state. `curl -N /api/v1/events/stream` streams Sentinel events. Swagger UI at `/swagger/index.html` shows topology endpoint. Service deployable in K8s.

---

## Phase 2 — K8s-Native Diagnostics
> Goal: Pinpoint connection leaks and load imbalance down to pod level.

| # | Feature | Package | Endpoint | Status |
|---|---|---|---|---|
| 2.1 | `CLIENT LIST` poller per node | `internal/connection/monitor.go` | `GET /api/v1/connections` | ✅ |
| 2.2 | IP → Pod/Deployment/Namespace mapper | `internal/connection/mapper.go` | included in connections | ✅ |
| 2.3 | Read/Write distribution per replica (`INFO COMMANDSTATS`) | `internal/connection/distribution.go` | `GET /api/v1/connections/distribution` | ✅ |
| 2.4 | Alert flag when replica handles >80% reads | `internal/connection/distribution.go` | included in distribution | ✅ |
| 2.5 | Slowlog puller + cross-node aggregator + ranker | `internal/diagnostics/slowlog.go` | `GET /api/v1/diagnostics/slowlog` | ✅ |
| 2.6 | `MULTI/EXEC` abort detector + oversized pipeline alert | `internal/diagnostics/pipeline.go` | `GET /api/v1/diagnostics/pipeline` | ✅ |
| 2.7 | K8s CPU throttle detector (CFS quota via metrics-server) | `internal/k8s/throttle.go` | included in connections | ✅ |

**Completion criteria**: `curl /api/v1/connections` shows per-pod breakdown. `curl /api/v1/diagnostics/slowlog` returns top-10 slow commands ranked across cluster. All endpoints visible in Swagger UI.

---

## Phase 3 — Key-level Intelligence
> Goal: Surface the hot/big keys and memory issues standard monitoring misses.

| # | Feature | Package | Endpoint | Status |
|---|---|---|---|---|
| 3.1 | Non-blocking big key scanner (`SCAN` + `MEMORY USAGE`, SSE stream) | `internal/keys/scanner.go` | `GET /api/v1/keys/bigkeys` | ✅ |
| 3.2 | Results grouped by namespace prefix (`user:*`, `session:*`) | `internal/keys/scanner.go` | included in bigkeys | ✅ |
| 3.3 | Hot key detector via `OBJECT FREQ` (requires LFU policy) | `internal/keys/hotkey.go` | `GET /api/v1/keys/hotkeys` | ✅ |
| 3.4 | TTL health report — % keys without TTL per namespace | `internal/keys/ttl.go` | `GET /api/v1/keys/ttl-report` | ✅ |
| 3.5 | Memory fragmentation ratio alert (`mem_fragmentation_ratio` > 1.5) | `internal/diagnostics/memory.go` | `GET /api/v1/diagnostics/memory` | ✅ |
| 3.6 | Eviction rate monitor (`evicted_keys` delta/sec) | `internal/diagnostics/memory.go` | included in memory | ✅ |
| 3.7 | RDB/AOF persistence health check | `internal/diagnostics/memory.go` | included in memory | ✅ |

**Completion criteria**: `curl /api/v1/keys/hotkeys?top=20` returns ranked hot keys. Big key SSE scan streams results without noticeably impacting Redis. All documented in Swagger.

---

## Phase 4 — Operational Resiliency
> Goal: Safe cluster operations with audit trail and notifications.

| # | Feature | Package | Endpoint | Status |
|---|---|---|---|---|
| 4.1 | Replication offset trend tracker (ring buffer per replica) | `internal/replication/tracker.go` | `GET /api/v1/replication/lag` | ✅ |
| 4.2 | Full resync counter + `repl-backlog-size` advisor | `internal/replication/resync.go` | `GET /api/v1/replication/resync-stats` | ✅ |
| 4.3 | Replica promotion readiness (suggest best candidate pre-failover) | `internal/replication/tracker.go` | included in failover dry-run | ✅ |
| 4.4 | Cross-node config diff (`CONFIG GET *` across all nodes) | `internal/operations/configdiff.go` | `GET /api/v1/config/diff` | ✅ |
| 4.5 | Config change audit log (ring buffer + optional file sink) | `internal/operations/audit.go` | `GET /api/v1/config/audit` | ✅ |
| 4.6 | Config set with audit (requires `confirm: true`) | `internal/operations/audit.go` | `POST /api/v1/config/set` | ✅ |
| 4.7 | Graceful failover: lag-check → replica-select → drain → promote → verify → rollback | `internal/operations/failover.go` | `POST /api/v1/ops/failover` | ✅ |
| 4.8 | Webhook notification on failover events (Slack / generic POST) | `internal/operations/notify.go` | config-driven | ✅ |
| 4.9 | Stale distributed lock detector (by key pattern + TTL check) | `internal/diagnostics/locks.go` | `GET /api/v1/diagnostics/locks` | ✅ |
| 4.10 | Prometheus metrics exporter | `internal/metrics/exporter.go` | `GET /metrics` | ✅ |

**Completion criteria**: `POST /api/v1/ops/failover` with `dry_run: true` shows pre-flight checks. Config diff works. All mutation endpoints require `confirm: true`. Swagger UI shows request body schema for all POST endpoints.

---

## Phase 5 — Testing & Chaos
> Can run incrementally alongside other phases.

| # | Feature | Package | Endpoint | Status |
|---|---|---|---|---|
| 5.1 | Dummy data seeder (configurable key count / type / prefix / size) | `internal/chaos/seeder.go` | `POST /api/v1/ops/chaos/seed` | ✅ |
| 5.2 | Safe cluster flusher (requires `confirm: true`) | `internal/chaos/seeder.go` | `POST /api/v1/ops/chaos/flush` | ✅ |
| 5.3 | Chaos failover trigger (`SENTINEL FAILOVER` or pod delete) | `internal/chaos/trigger.go` | `POST /api/v1/ops/chaos/failover` | ✅ |
| 5.4 | Integration test suite (docker-compose: 1 master + 2 replicas + 3 sentinels) | `integration/sentinel_test.go` | — | ✅ |
| 5.5 | Final `swag init` pass — verify all endpoints documented, no missing annotations | `docs/swagger/` | `GET /swagger/*` | ✅ |

**Completion criteria**: All endpoints appear in Swagger UI with correct request/response schemas. Integration tests pass against local cluster.

---

## Deferred / Won't Do

| Feature | Reason |
|---|---|
| TUI (Bubble Tea) | Not practical — requires terminal access to the server. REST API + Swagger UI covers all use cases. |
| gRPC | Overkill for current scope |
| Web UI frontend | Out of scope; consumers can build on top of the REST API |
| Multi-cluster management | Run one instance per cluster |

---

## Current Focus

**Start here → Phase 1**: `internal/sentinel/client.go` → `topology.go` → `pubsub.go` → `internal/api/server.go` → swaggo setup

Get Phase 1 complete and deployable — with Swagger UI working — before touching any diagnostic feature.
