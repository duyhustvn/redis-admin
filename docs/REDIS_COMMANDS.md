# Redis Commands — Tài liệu các lệnh và luồng xử lý dữ liệu

Tài liệu này mô tả từng module trong dự án: lệnh Redis nào được gọi, dữ liệu trả về có dạng gì, và service xử lý dữ liệu đó như thế nào để tạo ra output cuối cùng.

---

## Mục lục

1. [Topology — INFO clients server](#1-topology--info-clients-server)
2. [Replication Lag — INFO replication](#2-replication--info-replication)
3. [Memory — INFO memory stats persistence server](#3-memory--info-memory-stats-persistence-server)
4. [Connection Monitor — CLIENT LIST](#4-connection-monitor--client-list)
5. [Command Distribution — INFO commandstats](#5-command-distribution--info-commandstats)
6. [Slowlog — SLOWLOG GET](#6-slowlog--slowlog-get)
7. [Pipeline Stats — INFO stats / clients / errorstats + CLIENT LIST](#7-pipeline-stats--info-stats--clients--errorstats--client-list)
8. [Big Key Scanner — SCAN + MEMORY USAGE + TYPE + TTL](#8-big-key-scanner--scan--memory-usage--type--ttl)
9. [Hot Key Detection — CONFIG GET + SCAN + OBJECT FREQ + TYPE](#9-hot-key-detection--config-get--scan--object-freq--type)
10. [Stale Lock Scanner — SCAN + TTL + MEMORY USAGE](#10-stale-lock-scanner--scan--ttl--memory-usage)
11. [Quorum Check — SENTINEL CKQUORUM](#11-quorum-check--sentinel-ckquorum)
12. [Resync Stats — INFO replication (backlog)](#12-resync-stats--info-replication-backlog)
13. [TTL Report — SCAN + TTL](#13-ttl-report--scan--ttl)
14. [Sentinel Pub/Sub — PSUBSCRIBE *](#14-sentinel-pubsub--psubscribe-)
15. [Config Diff & Config Set — CONFIG GET / CONFIG SET](#15-config-diff--config-set--config-get--config-set)
16. [Failover — SENTINEL CKQUORUM + SENTINEL FAILOVER + INFO replication](#16-failover--sentinel-ckquorum--sentinel-failover--info-replication)
17. [Chaos — Pipeline + SCAN + DEL + SENTINEL FAILOVER](#17-chaos--pipeline--scan--del--sentinel-failover)
18. [Metrics Exporter — INFO memory stats](#18-metrics-exporter--info-memory-stats)

---

## 1. Topology — INFO clients server

**File:** `internal/sentinel/topology.go`  
**Hàm:** `fetchNodeInfo(ctx, addr, role)`

### Lệnh Redis

```
INFO clients server
```

Gọi trực tiếp đến từng node (master và replica) bằng `NewDirectClient`, **không** qua Failover client. Dùng hai section trong một lần gọi để giảm round-trip.

### Raw output từ Redis

```
# Clients
connected_clients:42
cluster_connections:0
maxclients:10000
client_recent_max_input_buffer:20512
blocked_clients:0
...

# Server
redis_version:7.0.11
uptime_in_seconds:86400
hz:10
...
```

### Trường được trích xuất

| Redis field | Kiểu | Ánh xạ sang |
|---|---|---|
| `connected_clients` | int | `NodeInfo.ConnectedClients` |
| `uptime_in_seconds` | int | `NodeInfo.UptimeSeconds` |

### Xử lý dữ liệu

```
raw string
  → parseInfoSection()  // tách "key:value\r\n", bỏ dòng "#" và dòng trống
  → map[string]string   // map phẳng, không phân biệt section
  → strconv.ParseInt()  // parse từng trường cần dùng
```

`parseInfoSection` gộp toàn bộ section thành một map phẳng — không cần phân biệt key `connected_clients` thuộc section `clients` hay `server`.

### Output struct

```go
type NodeInfo struct {
    Addr             string `json:"addr"`
    Role             string `json:"role"`       // "master" | "replica" | "sentinel"
    IsHealthy        bool   `json:"is_healthy"`
    ConnectedClients int64  `json:"connected_clients"`
    UptimeSeconds    int64  `json:"uptime_seconds"`
}
```

Nếu INFO thất bại (node không reachable), hàm trả về `NodeInfo{IsHealthy: false}` thay vì propagate lỗi — đảm bảo topology snapshot vẫn được trả về dù có node chết.

---

## 2. Replication — INFO replication

**File:** `internal/replication/tracker.go`  
**Hàm:** `fetchMasterOffset(ctx, addr)` và `fetchReplicaLag(ctx, addr, masterOffset)`

### Lệnh Redis

```
INFO replication
```

Gọi riêng lẻ cho master và từng replica.

### Raw output từ Redis (master)

```
# Replication
role:master
connected_slaves:2
slave0:ip=10.0.0.2,port=6379,state=online,offset=102400,lag=0
slave1:ip=10.0.0.3,port=6379,state=online,offset=101900,lag=1
master_failover_state:no-failover
master_replid:abc123...
master_repl_offset:102400
repl_backlog_active:1
repl_backlog_size:1048576
```

### Raw output từ Redis (replica)

```
# Replication
role:slave
master_host:10.0.0.1
master_port:6379
master_link_status:up
master_last_io_seconds_ago:1
slave_repl_offset:101900
slave_priority:100
```

### Trường được trích xuất

| Node | Redis field | Ánh xạ sang |
|---|---|---|
| master | `master_repl_offset` | offset chuẩn để tính lag |
| replica | `slave_repl_offset` | `ReplicaLag.ReplicaOffset` |

### Xử lý dữ liệu và tính lag

```
masterOffset  = master_repl_offset  (từ INFO master)
replicaOffset = slave_repl_offset   (từ INFO replica)

lagBytes = masterOffset - replicaOffset
  → clamp về 0 nếu âm (replica vừa bắt kịp ngay trước khi đọc master)

IsCaughtUp = lagBytes < 1 MiB (1 << 20)

PromotionScore = 100 * (1 - lagBytes / 100MiB)
  → 100 = lag = 0, ứng viên failover tốt nhất
  → 0   = lag ≥ 100 MiB, không nên promote
```

Ring buffer lưu 10 mẫu `LagSample` gần nhất cho mỗi replica để hiển thị trend:

```go
type LagSample struct {
    LagBytes int64     `json:"lag_bytes"`
    At       time.Time `json:"at"`
}
```

### Output struct

```go
type ReplicaLag struct {
    NodeAddr       string      `json:"node_addr"`
    MasterOffset   int64       `json:"master_offset"`
    ReplicaOffset  int64       `json:"replica_offset"`
    LagBytes       int64       `json:"lag_bytes"`
    LagTrend       []LagSample `json:"lag_trend"`       // tối đa 10 mẫu
    IsCaughtUp     bool        `json:"is_caught_up"`
    PromotionScore float64     `json:"promotion_score"` // 0–100
}
```

---

## 3. Memory — INFO memory stats persistence server

**File:** `internal/diagnostics/memory.go`  
**Hàm:** `fetchMemory(ctx, addr, role)`

### Lệnh Redis

```
INFO memory stats persistence server
```

Gộp 4 section trong một lần gọi để giảm round-trip.

### Raw output từ Redis (các dòng liên quan)

```
# Memory
used_memory:1073741824
used_memory_human:1.00G
maxmemory:2147483648
mem_fragmentation_ratio:1.23

# Stats
evicted_keys:500
maxmemory_policy:allkeys-lru

# Persistence
rdb_saves:10
rdb_last_save_time:1714435200
rdb_changes_since_last_save:1200
rdb_last_bgsave_status:ok
aof_enabled:1
aof_last_bgrewrite_status:ok
```

### Trường được trích xuất theo section

**Section `memory`:**

| Redis field | Ánh xạ sang | Ghi chú |
|---|---|---|
| `used_memory` | `UsedMemoryBytes` | byte thực tế đang dùng |
| `used_memory_human` | `UsedMemoryHuman` | dạng "1.00G" để hiển thị |
| `maxmemory` | `MaxMemoryBytes` | 0 = không giới hạn |
| `mem_fragmentation_ratio` | `FragRatio` | >1.5 → `FragAlert=true` |

**Section `stats`:**

| Redis field | Ánh xạ sang | Ghi chú |
|---|---|---|
| `evicted_keys` | `EvictedKeysTotal` | tổng cộng dồn từ khi node khởi động |
| `maxmemory_policy` | `EvictionPolicy` | vd: `allkeys-lru`, `volatile-lfu` |

**Section `persistence`:**

| Redis field | Ánh xạ sang | Ghi chú |
|---|---|---|
| `rdb_last_bgsave_status` | `RDBLastBgsaveStatus` | `"ok"` hoặc `"err"` |
| `rdb_last_save_time` | `RDBLastSaveTime` | Unix timestamp |
| `rdb_changes_since_last_save` | `RDBChangesSinceSave` | số write chưa được persist |
| `aof_enabled` | `AOFEnabled` | `1` = bật |
| `aof_last_bgrewrite_status` | `AOFLastRewriteStatus` | `"ok"` hoặc `"err"` |

### Xử lý đặc biệt — Eviction rate

`EvictedKeysTotal` là giá trị cộng dồn, không phản ánh được đột biến. Service tính **delta per second**:

```
evictedRate = (currentTotal - prevTotal) / elapsedSeconds
```

Trạng thái `prevTotal` và timestamp được lưu trong map `lastEvicted` (guarded by mutex) giữa các lần gọi.

### Output struct

```go
type MemoryReport struct {
    NodeAddr             string  `json:"node_addr"`
    Role                 string  `json:"role"`
    UsedMemoryBytes      int64   `json:"used_memory_bytes"`
    UsedMemoryHuman      string  `json:"used_memory_human"`
    MaxMemoryBytes       int64   `json:"max_memory_bytes"`
    FragRatio            float64 `json:"frag_ratio"`
    FragAlert            bool    `json:"frag_alert"`
    EvictionPolicy       string  `json:"eviction_policy"`
    EvictedKeysTotal     int64   `json:"evicted_keys_total"`
    EvictedKeysPerSec    float64 `json:"evicted_keys_per_sec"`
    RDBEnabled           bool    `json:"rdb_enabled"`
    RDBLastSaveTime      int64   `json:"rdb_last_save_time"`
    RDBChangesSinceSave  int64   `json:"rdb_changes_since_save"`
    RDBLastBgsaveStatus  string  `json:"rdb_last_bgsave_status"`
    RDBAlert             bool    `json:"rdb_alert"`
    AOFEnabled           bool    `json:"aof_enabled"`
    AOFLastRewriteStatus string  `json:"aof_last_rewrite_status,omitempty"`
    AOFAlert             bool    `json:"aof_alert"`
}
```

---

## 4. Connection Monitor — CLIENT LIST

**File:** `internal/connection/monitor.go`  
**Hàm:** `GetConnections(ctx)` → `fetchNodeConnections(ctx, addr, role)`

### Lệnh Redis

```
CLIENT LIST
```

Gọi trực tiếp trên từng node (master + tất cả replica).

### Raw output từ Redis

Mỗi dòng là một connection đang mở, các field cách nhau bằng dấu cách:

```
id=101 addr=10.0.0.5:54321 laddr=10.0.0.1:6379 fd=8 name=app-worker age=0
  idle=120 flags=N db=0 sub=0 psub=0 multi=-1 watch=0
  qbuf=0 qbuf-free=32768 argv-mem=10 multi-mem=0 tot-mem=61466
  rbs=16384 rbp=16384 obl=0 oll=0 omem=0 events=r cmd=get ...

id=102 addr=10.0.0.1:6380 laddr=10.0.0.1:6379 fd=9 name= age=0
  idle=0 flags=S db=0 ...   ← đây là slave/replication link, bị bỏ qua
```

### Trường được trích xuất

| Redis field | Dùng để |
|---|---|
| `addr` | Lấy phần IP để gom nhóm (nhiều conn từ cùng pod → cùng IP) |
| `flags` | Bỏ qua dòng có `"S"` (slave link nội bộ) |
| `idle` | Số giây không hoạt động; giữ giá trị `max` trong nhóm IP |
| `qbuf` | Input buffer đang chờ (bytes); giữ giá trị `max` trong nhóm IP |

### Xử lý dữ liệu

```
raw string (toàn bộ CLIENT LIST)
  → split "\n" → từng dòng
  → parseKVLine()  // tách "key=value key=value ..."
  → lọc flag "S"   // bỏ replication/sentinel link
  → gom nhóm theo IP (sourceIP strips port)
  → mỗi nhóm IP → một ClientInfo (ConnCount, MaxIdleSec, MaxQBufBytes)
  → enrichWithPodInfo() → tra K8s PodCache bằng IP
     → điền PodName / Namespace / Deployment
```

### Phát hiện Suspicious clients

Sau khi gom nhóm, service áp dụng 3 rule theo thứ tự ưu tiên (chỉ một rule/client):

| Rule | Điều kiện | Severity | Ý nghĩa |
|---|---|---|---|
| Critical leak | `ConnCount ≥ 5` **và** `MaxIdleSec > 86400` | `critical` | Rò rỉ nghiêm trọng, nhiều conn mở hơn 1 ngày |
| Pool leak | `ConnCount ≥ 3` **và** `MaxIdleSec > 300` | `warning` | Pool mở connection rồi không dùng > 5 phút |
| Zombie | `MaxIdleSec > 3600` | `warning` | Một connection đơn lẻ không hoạt động > 1 giờ |

### Output struct

```go
type NodeConnections struct {
    NodeAddr      string             `json:"node_addr"`
    Role          string             `json:"role"`
    Total         int                `json:"total"`
    UniqueClients int                `json:"unique_clients"`
    Clients       []ClientInfo       `json:"clients"`
    Suspicious    []SuspiciousClient `json:"suspicious,omitempty"`
}

type ClientInfo struct {
    SourceAddr   string `json:"source_addr"`
    PodName      string `json:"pod_name,omitempty"`
    Namespace    string `json:"namespace,omitempty"`
    Deployment   string `json:"deployment,omitempty"`
    ConnCount    int    `json:"conn_count"`
    MaxIdleSec   int64  `json:"max_idle_sec"`
    MaxQBufBytes int64  `json:"max_qbuf_bytes"`
}
```

---

## 5. Command Distribution — INFO commandstats

**File:** `internal/connection/distribution.go`  
**Hàm:** `GetDistribution(ctx)` → `fetchCommandDist(ctx, addr)`

### Lệnh Redis

```
INFO commandstats
```

Chỉ gọi trên **replica**, không gọi master — đọc trên master là dấu hiệu cấu hình sai ở client, không phản ánh "phân phối tải đọc".

### Raw output từ Redis

Mỗi dòng là một lệnh đã được gọi ít nhất một lần kể từ khi node khởi động:

```
# Commandstats
cmdstat_get:calls=10000,usec=45000,usec_per_call=4.50,rejected_calls=0,failed_calls=0
cmdstat_set:calls=2000,usec=9000,usec_per_call=4.50,rejected_calls=0,failed_calls=0
cmdstat_hget:calls=500,usec=2200,usec_per_call=4.40,rejected_calls=0,failed_calls=0
cmdstat_scan:calls=120,usec=600,usec_per_call=5.00,rejected_calls=0,failed_calls=0
```

### Xử lý dữ liệu

```
raw string
  → split "\r\n" → từng dòng
  → lọc dòng bắt đầu "cmdstat_"
  → tách tên lệnh: "cmdstat_get:" → "get"
  → extractCalls("calls=10000,...") → 10000
  → isReadCommand("get")? → yes → reads += 10000
                          → no  → writes += 2000

ReadPct  = reads  / (reads + writes) * 100
WritePct = writes / (reads + writes) * 100
```

**Danh sách read commands** được định nghĩa tĩnh trong `readCommands` map: GET, MGET, HGET, LRANGE, SMEMBERS, ZRANGE, SCAN, EXISTS, TTL, ... (xem `distribution.go`).

### Phát hiện Overloaded

Sau khi thu thập đủ từ mọi replica:

```
totalReads = sum(replica.ReadCount cho tất cả replica)
replica.Overloaded = replica.ReadCount / totalReads > 0.80
```

Ngưỡng 80%: với 2 replica lý tưởng là 50/50. Vượt 80% → replica kia gần như không được dùng, thường do client cấu hình sai `ReadPreference` hoặc DNS/load balancer chỉ route về một pod.

### Output struct

```go
type ReplicaDistribution struct {
    NodeAddr   string  `json:"node_addr"`
    ReadCount  int64   `json:"read_count"`
    WriteCount int64   `json:"write_count"`
    TotalCount int64   `json:"total_count"`
    ReadPct    float64 `json:"read_pct"`
    WritePct   float64 `json:"write_pct"`
    Overloaded bool    `json:"overloaded"`
}
```

---

## 6. Slowlog — SLOWLOG GET

**File:** `internal/diagnostics/slowlog.go`  
**Hàm:** `GetSlowlog(ctx, limit)` → `fetchSlowlog(ctx, addr, count)`

### Lệnh Redis

```
SLOWLOG GET <count>
```

Redis ghi vào slowlog mỗi lệnh có thời gian thực thi vượt ngưỡng `slowlog-log-slower-than` (mặc định `10000 µs = 10 ms`). Gọi trên mọi node (master + replica) với `count=128`.

### Raw output từ Redis

Mỗi entry là một mảng lồng nhau (go-redis deserialize sẵn):

```
ID: 42
Timestamp: 2024-04-30 10:00:00
Duration: 15234µs
Args: ["GET", "user:session:abc123"]
ClientAddr: "10.0.0.5:54321"
ClientName: "app-worker"
```

### Xử lý dữ liệu

```
[]redis.SlowLog (từ go-redis)
  → map sang []SlowlogEntry (gắn thêm NodeAddr)
  → merge entries từ tất cả node vào một slice
  → sort giảm dần theo DurationUs  ← slowest first
  → cắt lấy top limit entries
```

Lý do fetch 128 từ mỗi node trước khi cắt: mỗi node có slowlog riêng. Nếu chỉ lấy ít thì có thể bỏ sót lệnh chậm nhất ở một node khác.

### Output struct

```go
type SlowlogEntry struct {
    NodeAddr   string    `json:"node_addr"`
    ID         int64     `json:"id"`
    Timestamp  time.Time `json:"timestamp"`
    DurationUs int64     `json:"duration_us"`
    Args       []string  `json:"args"`
    ClientAddr string    `json:"client_addr,omitempty"`
    ClientName string    `json:"client_name,omitempty"`
}
```

---

## 7. Pipeline Stats — INFO stats / clients / errorstats + CLIENT LIST

**File:** `internal/diagnostics/pipeline.go`  
**Hàm:** `GetPipelineStats(ctx)` → `fetchPipelineReport(ctx, addr)`

### Lệnh Redis (4 lệnh, gọi tuần tự)

```
INFO stats
INFO clients
INFO errorstats
CLIENT LIST
```

Gọi trên mọi node (master + replica). Gọi riêng từng lệnh vì cần xử lý lỗi độc lập — nếu một lệnh fail thì các lệnh còn lại vẫn được thực hiện.

### Trường trích xuất

**`INFO stats` → `rejected_calls`**
```
# Stats
rejected_calls:3
...
```
Số lệnh bị từ chối (server quá tải hoặc lệnh không hợp lệ).

**`INFO clients` → `client_recent_max_input_buffer`**
```
# Clients
client_recent_max_input_buffer:65536
...
```
Kích thước input buffer lớn nhất gần đây của bất kỳ client nào.

**`INFO errorstats` → đếm `EXECABORT`**
```
# Errorstats
errorstat_EXECABORT:count=5,first_seen=...,last_seen=...
errorstat_ERR:count=2,...
```
Đếm số lần MULTI/EXEC bị abort — dấu hiệu transaction đang dùng WATCH bị race condition.
Format: `errorstat_<ERROR>:count=N,...` → dùng `extractCount()` để parse N.

**`CLIENT LIST` → đếm `qbuf > 1 MiB`**

Mỗi client có `qbuf` = số byte đang đợi trong input buffer. `qbuf > 1 MiB` nghĩa là client đang gửi pipeline rất lớn — có thể gây áp lực bộ nhớ trên Redis.

### Output struct

```go
type PipelineReport struct {
    NodeAddr            string `json:"node_addr"`
    EXECAbortCount      int64  `json:"exec_abort_count"`
    RejectedCalls       int64  `json:"rejected_calls"`
    MaxInputBufferBytes int64  `json:"max_input_buffer_bytes"`
    OversizedPipelines  int    `json:"oversized_pipelines"` // client có qbuf > 1 MiB
}
```

---

## 8. Big Key Scanner — SCAN + MEMORY USAGE + TYPE + TTL

**File:** `internal/keys/scanner.go`  
**Hàm:** `ScanBigKeys(ctx, thresholdBytes, onKey)` → `scanNode()` → `inspectKey()`

### Lệnh Redis (3 lệnh per key)

```
SCAN cursor MATCH * COUNT 200    ← lặp toàn bộ keyspace
MEMORY USAGE <key> SAMPLES 0    ← ước tính kích thước key
TYPE <key>                       ← lấy kiểu dữ liệu
TTL <key>                        ← lấy thời gian sống còn lại
```

Ưu tiên chạy trên **replica** (không tải master). Fallback về master nếu không có replica.

### Luồng xử lý

```
SCAN (batch 200 key) → lặp đến cursor = 0
  ↓ mỗi key
  MEMORY USAGE key SAMPLES 0
    → sizeBytes < thresholdBytes? → bỏ qua (giảm số lần gọi TYPE + TTL)
    → sizeBytes >= threshold?
        → TYPE key  → "string" | "hash" | "list" | "set" | "zset" | "stream"
        → TTL key   → ttlDur (-1 = không có TTL, -2 = key không tồn tại)
        → tạo KeyReport
        → gọi onKey(report) callback (SSE streaming về client)
        → cập nhật NamespaceStat (gom nhóm theo prefix trước ":")
  sleep 1ms giữa mỗi batch để không block Redis
```

`SAMPLES 0` trong MEMORY USAGE: ước tính dựa trên encoding header thay vì sample toàn bộ data — nhanh hơn và đủ chính xác để so ngưỡng.

### Output struct

```go
type KeyReport struct {
    Key        string `json:"key"`
    Type       string `json:"type"`
    SizeBytes  int64  `json:"size_bytes"`
    NodeAddr   string `json:"node_addr"`
    Namespace  string `json:"namespace"`  // prefix trước ":" đầu tiên
    TTLSeconds int64  `json:"ttl_seconds"`
}

type NamespaceStat struct {
    Namespace  string  `json:"namespace"`
    KeyCount   int64   `json:"key_count"`
    TotalBytes int64   `json:"total_bytes"`
    AvgBytes   float64 `json:"avg_bytes"`
    MaxBytes   int64   `json:"max_bytes"`
}
```

---

## 9. Hot Key Detection — CONFIG GET + SCAN + OBJECT FREQ + TYPE

**File:** `internal/keys/hotkey.go`  
**Hàm:** `GetHotkeys(ctx, topN)` → `collectHotkeys(ctx, addr, topN, heap)`

### Lệnh Redis (4 lệnh)

```
CONFIG GET maxmemory-policy    ← kiểm tra LFU trước khi scan
SCAN cursor MATCH * COUNT 200  ← duyệt keyspace
OBJECT FREQ <key>              ← đọc LFU counter của key
TYPE <key>                     ← kiểu dữ liệu (chỉ khi key đủ điều kiện vào heap)
```

Chạy trên **replica**. Fallback về master nếu không có replica.

### Tại sao cần kiểm tra LFU trước?

`OBJECT FREQ` chỉ trả về giá trị có nghĩa khi Redis dùng LFU eviction policy (`allkeys-lfu` hoặc `volatile-lfu`). Với các policy khác (LRU, random, ...) counter luôn là 0. Service kiểm tra sớm để tránh scan toàn bộ keyspace vô ích.

```
CONFIG GET maxmemory-policy → "allkeys-lfu"
  → policy không kết thúc bằng "lfu"? → return error ngay
```

### Luồng xử lý

```
SCAN (batch 200 key) → lặp đến cursor = 0
  ↓ mỗi key
  OBJECT FREQ key → freq (0–255, logarithmic LFU counter)
    → lỗi? → skip key (key có thể đã bị evict)
    → heap chưa đầy (< topN)?  → heap.Push(entry)
    → heap đầy VÀ freq > heap[0].Frequency?
        → heap.Pop()            ← bỏ phần tử nhỏ nhất
        → TYPE key              ← chỉ gọi TYPE khi cần thiết để giảm round-trip
        → heap.Push(entry)
```

**Min-heap (hotHeap):** duy trì đúng topN key có frequency cao nhất. Root của heap là phần tử nhỏ nhất — so sánh nhanh O(1) để quyết định có push không, push/pop O(log N).

### Giá trị OBJECT FREQ

- Scale: logarithmic, 0–255
- Redis tăng counter mỗi khi key được đọc/ghi, có decay theo thời gian
- Không phải raw access count — chỉ dùng để so sánh tương đối giữa các key

### Output struct

```go
type HotKeyReport struct {
    Key       string `json:"key"`
    Type      string `json:"type"`
    Frequency int64  `json:"frequency"` // OBJECT FREQ value
    NodeAddr  string `json:"node_addr"`
    Namespace string `json:"namespace"`
}
```

---

## 10. Stale Lock Scanner — SCAN + TTL + MEMORY USAGE

**File:** `internal/diagnostics/locks.go`  
**Hàm:** `GetStaleLocks(ctx, pattern, staleThresholdSec)` → `scanLocks()`

### Lệnh Redis (3 lệnh per candidate key)

```
SCAN cursor MATCH <pattern> COUNT 200    ← tìm key theo pattern (vd: "lock:*")
TTL <key>                                ← kiểm tra thời gian sống
MEMORY USAGE <key> SAMPLES 0            ← chỉ gọi khi key là stale
```

Chạy trên **replica**. Fallback về master nếu không có replica. Dùng `seen map` để deduplicate key xuất hiện ở nhiều replica.

### Xác định stale

```
ttlSec = TTL key
  → -2 = key không tồn tại (skip)
  → -1 = không có TTL → IsStale = true  (lock không bao giờ tự hết hạn)
  → N  = staleThresholdSec == 0?  → không flag theo TTL dương
          staleThresholdSec > 0 VÀ ttlSec > staleThresholdSec? → IsStale = true
```

`MEMORY USAGE` chỉ được gọi **sau khi** xác nhận key là stale — tránh gọi thừa cho key bình thường.

### Output struct

```go
type LockReport struct {
    Key        string `json:"key"`
    TTLSeconds int64  `json:"ttl_seconds"` // -1 = không có TTL
    SizeBytes  int64  `json:"size_bytes"`
    NodeAddr   string `json:"node_addr"`
    IsStale    bool   `json:"is_stale"`
}
```

---

## 11. Quorum Check — SENTINEL CKQUORUM

**File:** `internal/sentinel/quorum.go`  
**Hàm:** `Check(ctx)`

### Lệnh Redis

```
SENTINEL CKQUORUM <masterName>
```

Gọi trực tiếp đến từng Sentinel node (qua `SentinelManagementClient`, port 26379), **không** phải Redis node (port 6379).

### Raw output từ Redis Sentinel

```
OK 3 usable Sentinels. Quorum and failover authorization can be reached
```
hoặc khi quorum không đủ:
```
NOQUORUM 1 usable Sentinels. Not enough for quorum or failover
```

### Xử lý dữ liệu

```
Lặp qua cfg.SentinelAddrs theo thứ tự:
  → SENTINEL CKQUORUM masterName
  → result.HasPrefix("OK")? → return true  (dừng sớm)
  → lỗi hoặc không OK?     → log warn, thử sentinel tiếp theo
→ hết danh sách → return false
```

Dừng sớm tại sentinel đầu tiên trả về OK — không cần hỏi tất cả.

### Output

`bool` — được nhúng vào `TopologySnapshot.QuorumOK`.

```go
type TopologySnapshot struct {
    ...
    QuorumOK   bool      `json:"quorum_ok"`
    CapturedAt time.Time `json:"captured_at"`
}
```

---

## 12. Resync Stats — INFO replication (backlog)

**File:** `internal/replication/resync.go`  
**Hàm:** `GetResyncStats(ctx)` → `fetchResync(ctx, addr, role)`

### Lệnh Redis

```
INFO replication
```

Gọi trên master và mọi replica — backlog chỉ có nghĩa trên master, nhưng `total_resyncs_processed` cần được đo trên replica.

### Fields extracted

| Redis field | Ánh xạ sang | Ghi chú |
|---|---|---|
| `total_resyncs_processed` | `TotalResyncs` | Số lần replica phải full resync; có từ Redis 6.2 |
| `repl_backlog_size` | `BacklogSize` | Tổng dung lượng backlog buffer (bytes), cấu hình qua `repl-backlog-size` |
| `repl_backlog_active` | `BacklogActiveSize` | Phần backlog đang chứa dữ liệu chưa được tất cả replica xác nhận |

### Xử lý dữ liệu

```
BacklogAlert = BacklogActiveSize / BacklogSize > 0.90
```

Khi backlog đầy, replica disconnect + reconnect sẽ bị **forced full resync** (RDB dump toàn bộ) thay vì partial resync — gây tăng đột biến CPU và network I/O trên master.

### Output struct

```go
type ResyncReport struct {
    NodeAddr          string `json:"node_addr"`
    Role              string `json:"role"`
    TotalResyncs      int64  `json:"total_resyncs"`
    BacklogSize       int64  `json:"backlog_size_bytes"`
    BacklogActiveSize int64  `json:"backlog_active_size_bytes"`
    BacklogAlert      bool   `json:"backlog_alert"`
    Advisory          string `json:"advisory,omitempty"`
}
```

---

## 13. TTL Report — SCAN + TTL

**File:** `internal/keys/ttl.go`  
**Hàm:** `GetTTLReport(ctx)` → `collectTTLStats(ctx, addr, ns)`

### Lệnh Redis

```
SCAN cursor MATCH * COUNT 200    ← duyệt keyspace
TTL <key>                        ← kiểm tra expiry từng key
```

Chạy trên replica; fallback master nếu không có replica.

### Giá trị TTL trả về

| Giá trị | Ý nghĩa | Hành động |
|---|---|---|
| `-1s` | Key tồn tại, không có expiry | Tính vào `noTTL` |
| `-2s` | Key không tồn tại (bị evict giữa SCAN và TTL) | Bỏ qua |
| `Ns` | Còn N giây | Tính vào `total` nhưng không vào `noTTL` |

### Xử lý dữ liệu

```
SCAN (batch 200 key) → lặp đến cursor = 0
  ↓ mỗi key
  TTL key → ttlDur
    → -1s? → st.total++; st.noTTL++
    → -2s? → bỏ qua
    → Ns?  → st.total++

NoTTLPct   = noTTL / total * 100
NoTTLAlert = NoTTLPct > 50
```

### Output struct

```go
type NamespaceTTLReport struct {
    Namespace  string  `json:"namespace"`
    TotalKeys  int64   `json:"total_keys"`
    NoTTLKeys  int64   `json:"no_ttl_keys"`
    NoTTLPct   float64 `json:"no_ttl_pct"`
    NoTTLAlert bool    `json:"no_ttl_alert"` // true khi >50% key không có TTL
}
```

---

## 14. Sentinel Pub/Sub — PSUBSCRIBE *

**File:** `internal/sentinel/pubsub.go`  
**Hàm:** `PubSubListener.subscribe(ctx, addr, flapTracker)`

### Lệnh Redis

```
PSUBSCRIBE *
```

Gửi đến **Sentinel node** (port 26379), không phải Redis node.  
`ReadTimeout = 0` — connection block indefinitely để nhận message bất kỳ lúc nào.

### Channels Sentinel publish

| Channel | Sự kiện |
|---|---|
| `+sdown` | Một sentinel phát hiện node down (subjectively down) |
| `-sdown` | Node phục hồi |
| `+odown` | Quorum đồng ý node down (objectively down) |
| `-odown` | Quorum hủy odown |
| `+switch-master` | Master mới được bầu sau failover |
| `+slave-reconf-done` | Replica đã reconfigure xong trỏ sang master mới |
| `+sentinel` | Sentinel mới join cluster |

### Payload format

```
<role> <name> <ip> <port> @ <master-name> <master-ip> <master-port>
```

`parseEvent` tách `ip` (field[2]) và `port` (field[3]) để điền `SentinelEvent.NodeAddr`.

### Phát hiện flapping

Node được coi là flapping khi nhận `+sdown` **≥ 3 lần trong 60 giây**:

```
flapTracker[nodeAddr] giữ slice timestamp gần đây
→ prune timestamp cũ hơn 60s
→ append timestamp hiện tại
→ len >= 3? → IsFlapping = true
```

### Output struct

```go
type SentinelEvent struct {
    Channel    string    `json:"channel"`
    Payload    string    `json:"payload"`
    NodeAddr   string    `json:"node_addr,omitempty"`
    IsFlapping bool      `json:"is_flapping,omitempty"`
    Timestamp  time.Time `json:"timestamp"`
}
```

---

## 15. Config Diff & Config Set — CONFIG GET / CONFIG SET

**File:** `internal/operations/configdiff.go`  
**Hàm:** `GetConfigDiff`, `SetConfig`, `fetchAllConfig`, `fetchConfigKey`

### Lệnh Redis

```
CONFIG GET *          ← lấy toàn bộ config (GetConfigDiff, fetchAllConfig)
CONFIG GET <key>      ← lấy một key cụ thể (fetchConfigKey)
CONFIG SET <key> <value>  ← áp dụng thay đổi (SetConfig)
```

### CONFIG GET * — raw output

```
maxmemory-policy: allkeys-lru
maxmemory: 2147483648
hz: 10
repl-backlog-size: 1048576
...
```

go-redis trả về `map[string]string` trực tiếp.  
Timeout 10s (thay vì 5s) vì Redis serialize hàng trăm key trước khi gửi.

### Luồng GetConfigDiff

```
Với mỗi node:
  CONFIG GET * → map[key]value
  → gom vào configMap[key][nodeAddr] = value

Với mỗi key trong configMap:
  → so sánh value giữa các node
  → giá trị khác nhau? → IsDrift=true → đưa vào kết quả
  → giống nhau? → bỏ qua
```

### Luồng SetConfig

```
1. CONFIG GET <key>          → đọc giá trị cũ (để ghi audit)
2. CONFIG SET <key> <value>  → áp dụng ngay, không cần restart
3. audit.record()            → lưu OldValue / NewValue / RemoteIP
```

CONFIG SET chỉ thay đổi in-memory — restart node sẽ mất. Dùng `CONFIG REWRITE` để persist (không được implement ở đây theo thiết kế).

### Output struct

```go
type ConfigDiff struct {
    Key     string            `json:"key"`
    Values  []NodeConfigValue `json:"values"`
    IsDrift bool              `json:"is_drift"`
}
```

---

## 16. Failover — SENTINEL CKQUORUM + SENTINEL FAILOVER + INFO replication

**File:** `internal/operations/failover.go`  
**Hàm:** `Failover(ctx, dryRun)`

### Luồng các lệnh Redis theo thứ tự

```
Bước 1 — Pre-flight quorum check:
  SENTINEL CKQUORUM <masterName>   → sentinel port 26379
  → "OK ..."? → tiếp tục
  → "NOQUORUM" hoặc lỗi? → return error

Bước 2 — Tính lag từng replica (x2 lệnh per replica):
  INFO replication (trên master)   → master_repl_offset
  INFO replication (trên replica)  → slave_repl_offset
  → lagBytes = masterOffset - replicaOffset
  → chọn replica có lagBytes nhỏ nhất

Bước 3 — Trigger failover (chỉ khi dryRun=false):
  SENTINEL FAILOVER <masterName>   → sentinel port 26379
  → dùng sc.Do() vì go-redis không có wrapper cho lệnh này

Bước 4 — Đợi master mới (poll mỗi 1s, tối đa 45s):
  Sentinel.GetMasterAddrByName → hỏi Sentinel cho đến khi trả về địa chỉ khác oldMaster
```

### Output struct

```go
type FailoverResult struct {
    DryRun          bool     `json:"dry_run"`
    PreChecks       []string `json:"pre_checks"`     // log từng bước kiểm tra
    SelectedReplica string   `json:"selected_replica"`
    OldMaster       string   `json:"old_master"`
    NewMaster       string   `json:"new_master,omitempty"`
    ElapsedMs       int64    `json:"elapsed_ms"`
}
```

---

## 17. Chaos — Pipeline + SCAN + DEL + SENTINEL FAILOVER

**File:** `internal/chaos/seeder.go`, `internal/chaos/trigger.go`

### Seed — Pipeline write

```
Pipeline (batch 100 lệnh):
  string: SET  <prefix><i> <value>
  hash:   HSET <prefix><i> field <value>
  list:   RPUSH <prefix><i> <value>
  set:    SADD  <prefix><i> <value>
  zset:   ZADD  <prefix><i> <score> <member>
  + EXPIRE <key> <ttlSec>  (nếu ttlSec > 0)

Pipeline.Exec() mỗi 100 lệnh → gửi toàn bộ batch, đọc response trong một round-trip
```

### Flush — SCAN + DEL

```
SCAN cursor MATCH <pattern> COUNT 200   → tìm key theo batch
DEL key1 key2 ... keyN                  → xóa cả batch (variadic DEL, một round-trip)
```

Không dùng `FLUSHALL`/`FLUSHDB` — chỉ xóa key khớp pattern.

### Chaos Failover — SENTINEL FAILOVER

```
PING         → kiểm tra sentinel reachable
SENTINEL FAILOVER <masterName>  → kích hoạt ngay, không có pre-check
```

Không có quorum check hay lag check — thiết kế cho chaos testing.

### Output structs

```go
type SeedResult struct {
    KeysCreated int64  `json:"keys_created"`
    Prefix      string `json:"prefix"`
    KeyType     string `json:"key_type"`
    ElapsedMs   int64  `json:"elapsed_ms"`
}

type FlushResult struct {
    KeysDeleted int64  `json:"keys_deleted"`
    Pattern     string `json:"pattern"`
    ElapsedMs   int64  `json:"elapsed_ms"`
}
```

---

## 18. Metrics Exporter — INFO memory stats

**File:** `internal/metrics/exporter.go`  
**Hàm:** `collectNodeMemory(ctx, addr)`

### Lệnh Redis

```
INFO memory stats
```

Gọi trên master + mọi replica theo chu kỳ `poll_interval`.

### Fields extracted

| Redis field | Section | Prometheus metric |
|---|---|---|
| `used_memory` | memory | `redis_used_memory_bytes` (gauge) |
| `mem_fragmentation_ratio` | memory | `redis_memory_fragmentation_ratio` (gauge) |
| `evicted_keys` | stats | `redis_evicted_keys_total` (counter) |

### Metrics khác (lấy từ service layer, không gọi Redis trực tiếp)

| Prometheus metric | Nguồn dữ liệu |
|---|---|
| `redis_sentinel_quorum_ok` | `GetTopology()` → `QuorumOK` |
| `redis_connected_clients` | `GetTopology()` → `NodeInfo.ConnectedClients` |
| `redis_replication_lag_bytes` | `GetReplicationLag()` → `ReplicaLag.LagBytes` |
| `redis_replica_promotion_score` | `GetReplicationLag()` → `ReplicaLag.PromotionScore` |

---

## Tổng hợp — Redis commands theo module

| Module | File | Lệnh Redis | Node đích |
|---|---|---|---|
| Topology | `sentinel/topology.go` | `INFO clients server` | master + mọi replica |
| Replication Lag | `replication/tracker.go` | `INFO replication` | master + mọi replica |
| Replication Resync | `replication/resync.go` | `INFO replication` | master + mọi replica |
| Memory | `diagnostics/memory.go` | `INFO memory stats persistence server` | master + mọi replica |
| Connection Monitor | `connection/monitor.go` | `CLIENT LIST` | master + mọi replica |
| Command Distribution | `connection/distribution.go` | `INFO commandstats` | chỉ replica |
| Slowlog | `diagnostics/slowlog.go` | `SLOWLOG GET` | master + mọi replica |
| Pipeline Stats | `diagnostics/pipeline.go` | `INFO stats` + `INFO clients` + `INFO errorstats` + `CLIENT LIST` | master + mọi replica |
| Big Key Scanner | `keys/scanner.go` | `SCAN` + `MEMORY USAGE` + `TYPE` + `TTL` | ưu tiên replica |
| TTL Report | `keys/ttl.go` | `SCAN` + `TTL` | ưu tiên replica |
| Hot Key Detection | `keys/hotkey.go` | `CONFIG GET` + `SCAN` + `OBJECT FREQ` + `TYPE` | ưu tiên replica |
| Stale Lock Scanner | `diagnostics/locks.go` | `SCAN` + `TTL` + `MEMORY USAGE` | ưu tiên replica |
| Quorum Check | `sentinel/quorum.go` | `SENTINEL CKQUORUM` | sentinel (port 26379) |
| Sentinel Events | `sentinel/pubsub.go` | `PSUBSCRIBE *` | sentinel (port 26379) |
| Config Diff | `operations/configdiff.go` | `CONFIG GET *` | master + mọi replica |
| Config Set | `operations/configdiff.go` | `CONFIG GET <key>` + `CONFIG SET` | node cụ thể |
| Failover | `operations/failover.go` | `SENTINEL CKQUORUM` + `INFO replication` + `SENTINEL FAILOVER` | sentinel + master + replica |
| Chaos Seed | `chaos/seeder.go` | Pipeline (`SET`/`HSET`/`RPUSH`/`SADD`/`ZADD` + `EXPIRE`) | master |
| Chaos Flush | `chaos/seeder.go` | `SCAN` + `DEL` | master |
| Chaos Failover | `chaos/trigger.go` | `SENTINEL FAILOVER` | sentinel (port 26379) |
| Metrics Exporter | `metrics/exporter.go` | `INFO memory stats` | master + mọi replica |

> **Ghi chú chung:** Mọi lệnh Redis đều được bọc trong `context.WithTimeout` (2–5 giây) để tránh block vô thời hạn khi node chậm hoặc mạng có vấn đề. Ngoại lệ duy nhất: `PSUBSCRIBE` dùng `ReadTimeout=0` vì Pub/Sub cần connection block indefinitely.
