# Skill: Redis & Sentinel Patterns

Read this file before working on any package under `internal/sentinel/`, `internal/keys/`, `internal/replication/`, or `internal/diagnostics/`.

---

## Sentinel Connection

Always use `NewFailoverClient` — never connect directly to a Redis node address:

```go
func NewSentinelClient(cfg *config.Config) *redis.Client {
    return redis.NewFailoverClient(&redis.FailoverOptions{
        MasterName:       cfg.MasterName,
        SentinelAddrs:    cfg.SentinelAddrs,
        SentinelPassword: cfg.SentinelPassword,
        Password:         cfg.RedisPassword,
        DB:               0,
        ReadTimeout:      2 * time.Second,
        WriteTimeout:     2 * time.Second,
        PoolSize:         10,
        DialTimeout:      3 * time.Second,
    })
}

// For reads that should go to replicas:
func NewReplicaClient(cfg *config.Config) *redis.Client {
    return redis.NewFailoverClient(&redis.FailoverOptions{
        MasterName:    cfg.MasterName,
        SentinelAddrs: cfg.SentinelAddrs,
        ReplicaOnly:   true, // route reads to replicas
        ReadTimeout:   2 * time.Second,
        WriteTimeout:  2 * time.Second,
    })
}
```

## Sentinel API Calls

```go
// Get master info
masterAddr, err := rdb.Do(ctx, "SENTINEL", "get-master-addr-by-name", masterName).StringSlice()

// Get replicas
replicas, err := rdb.Do(ctx, "SENTINEL", "replicas", masterName).MapStringStringSlice()

// Get all sentinels
sentinels, err := rdb.Do(ctx, "SENTINEL", "sentinels", masterName).MapStringStringSlice()

// Quorum check — run this on every sentinel address
result, err := rdb.Do(ctx, "SENTINEL", "ckquorum", masterName).Text()
// Returns "OK N usable Sentinels" or error

// Manual failover (use with care — always dry-run first)
err = rdb.Do(ctx, "SENTINEL", "failover", masterName).Err()
```

## Sentinel Pub/Sub Events

Subscribe on the Sentinel port (26379), not the Redis port:

```go
func SubscribeSentinelEvents(ctx context.Context, sentinelAddr string) {
    // Connect directly to sentinel for Pub/Sub
    sentinelClient := redis.NewClient(&redis.Options{
        Addr: sentinelAddr,
    })
    
    // Subscribe to all sentinel channels
    pubsub := sentinelClient.PSubscribe(ctx, "*")
    
    for msg := range pubsub.Channel() {
        event := parseSentinelEvent(msg.Channel, msg.Payload)
        // handle event
    }
}

// Key channels to watch:
// +sdown       — Subjective down (one sentinel thinks node is down)
// -sdown       — Node recovered from subjective down
// +odown       — Objective down (quorum agrees node is down)
// -odown       — Node recovered from objective down
// +failover-state-send-slaveof  — Failover starting
// +promoted-slave               — New master elected
// +slave-reconf-done            — Replica reconfigured to new master
// -failover-abort               — Failover aborted
```

**Flapping detection**: if `+sdown` and `-sdown` alternate for the same node more than N times in a window, raise a flapping alert — indicates CNI/network instability, not true node failure.

## Safe SCAN Pattern

**NEVER use `KEYS *`**. Always use SCAN with a reasonable count and sleep between batches:

```go
func ScanKeys(ctx context.Context, rdb *redis.Client, pattern string, fn func(key string) error) error {
    var cursor uint64
    for {
        keys, nextCursor, err := rdb.Scan(ctx, cursor, pattern, 100).Result()
        if err != nil {
            return fmt.Errorf("scan: %w", err)
        }
        for _, key := range keys {
            if err := fn(key); err != nil {
                return err
            }
        }
        cursor = nextCursor
        if cursor == 0 {
            break
        }
        // Yield to avoid starving other operations
        time.Sleep(1 * time.Millisecond)
    }
    return nil
}
```

## Memory Usage Per Key

```go
// MEMORY USAGE is safe but non-trivial cost — use SAMPLES 0 for speed
// SAMPLES 0 = only sample top-level key, no nested traversal
sizeBytes, err := rdb.MemoryUsage(ctx, key, 0).Result()

// For big key threshold check:
const BigKeyThreshold = 1 * 1024 * 1024 // 1MB
if sizeBytes > BigKeyThreshold {
    // report as big key
}
```

## Hot Key Detection (LFU)

Requires `maxmemory-policy` set to an LFU variant (`allkeys-lfu` or `volatile-lfu`):

```go
// Returns frequency counter (logarithmic scale, 0-255)
freq, err := rdb.Do(ctx, "OBJECT", "FREQ", key).Int()

// Check policy before using:
policy, err := rdb.ConfigGet(ctx, "maxmemory-policy").Result()
```

## Slowlog

```go
// Pull last 128 entries — reset count to 0 to keep accumulating
entries, err := rdb.SlowLogGet(ctx, 128).Result()
for _, entry := range entries {
    // entry.ID, entry.Time, entry.Duration, entry.Args, entry.ClientAddr
    if entry.Duration > 10*time.Millisecond {
        // flag as slow
    }
}

// Reset slowlog after reading if you want delta tracking:
rdb.SlowLogReset(ctx)
```

## INFO Sections

```go
// Always specify section — avoid INFO all (expensive)
info, err := rdb.Info(ctx, "replication").Result()
info, err := rdb.Info(ctx, "clients").Result()
info, err := rdb.Info(ctx, "memory").Result()
info, err := rdb.Info(ctx, "commandstats").Result()
info, err := rdb.Info(ctx, "persistence").Result()
info, err := rdb.Info(ctx, "stats").Result()

// Parse INFO output (it's key:value\r\n format):
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
```

## Replication Lag

```go
// From INFO replication on master:
masterOffset, _ := strconv.ParseInt(info["master_repl_offset"], 10, 64)

// From INFO replication on replica:
replicaOffset, _ := strconv.ParseInt(info["slave_repl_offset"], 10, 64)
lagBytes := masterOffset - replicaOffset

// Check for full resync:
fullSyncs, _ := strconv.ParseInt(info["total_resyncs_processed"], 10, 64)
```

## CONFIG GET / SET

```go
// Get all config (for drift detection)
configs, err := rdb.ConfigGet(ctx, "*").Result()
// Returns map[string]string

// Set with audit
oldVal := configs["maxmemory"]
err = rdb.ConfigSet(ctx, "maxmemory", newVal).Err()
// Log: timestamp, node, key, oldVal, newVal
```

## Context Timeouts — Always Set

```go
// For one-off diagnostic commands:
ctx, cancel := context.WithTimeout(parentCtx, 5*time.Second)
defer cancel()

// For long-running scans, use the parent context but check cancellation:
select {
case <-ctx.Done():
    return ctx.Err()
default:
}
```

## What NOT to Do

| Dangerous | Safe Alternative |
|---|---|
| `KEYS *` | `SCAN` with cursor |
| `MONITOR` | `SLOWLOG GET` or `OBJECT FREQ` |
| `DEBUG SLEEP` | Use chaos trigger with K8s pod deletion |
| `FLUSHALL` without prompt | Always require `--confirm` flag |
| Direct node connection | Always via Sentinel failover client |
| `INFO all` in polling loop | Specify section: `INFO replication` |
