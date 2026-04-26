# Skill: Error Handling Conventions

Read this file for all packages. These conventions apply project-wide.

---

## Error Wrapping

Always wrap errors with context. Use `fmt.Errorf` with `%w`:

```go
// Good
if err != nil {
    return fmt.Errorf("fetch topology from sentinel %s: %w", addr, err)
}

// Bad — loses context
if err != nil {
    return err
}

// Bad — loses error type for errors.Is/As
if err != nil {
    return fmt.Errorf("fetch topology: %v", err)  // %v not %w
}
```

## Sentinel Error Types

Define typed errors for expected failure modes:

```go
package sentinel

import "errors"

var (
    ErrNoMaster       = errors.New("no master found")
    ErrQuorumNotMet   = errors.New("sentinel quorum not met")
    ErrNodeUnreachable = errors.New("node unreachable")
    ErrFlapping       = errors.New("node is flapping — check CNI/network")
)

// Usage:
if masterAddr == "" {
    return nil, fmt.Errorf("get master %s: %w", masterName, ErrNoMaster)
}

// Caller can check type:
if errors.Is(err, ErrQuorumNotMet) {
    logger.Warn("quorum check failed — cluster may be degraded")
}
```

## Logging with Zap

Use structured fields, never string formatting in log calls:

```go
// Good
logger.Error("failed to fetch slowlog",
    zap.String("node", addr),
    zap.Error(err),
    zap.Duration("elapsed", elapsed),
)

logger.Info("big key detected",
    zap.String("key", key),
    zap.String("namespace", namespace),
    zap.Int64("size_bytes", sizeBytes),
)

logger.Warn("replication lag high",
    zap.String("replica", replicaAddr),
    zap.Int64("lag_bytes", lagBytes),
    zap.Duration("since", time.Since(lastCheck)),
)

// Bad — unstructured
logger.Error(fmt.Sprintf("failed to fetch slowlog from %s: %v", addr, err))
```

## Context Cancellation

Always check context before expensive operations:

```go
func ScanKeys(ctx context.Context, rdb *redis.Client, fn func(key string) error) error {
    var cursor uint64
    for {
        // Check cancellation at top of each loop iteration
        select {
        case <-ctx.Done():
            return ctx.Err()
        default:
        }

        keys, next, err := rdb.Scan(ctx, cursor, "*", 100).Result()
        if err != nil {
            return fmt.Errorf("scan cursor %d: %w", cursor, err)
        }
        // ...
    }
}
```

## Error Aggregation for Multi-node Operations

When running diagnostics across multiple nodes, collect all errors rather than failing fast:

```go
type MultiError struct {
    Errs []error
}

func (m *MultiError) Error() string {
    msgs := make([]string, len(m.Errs))
    for i, e := range m.Errs {
        msgs[i] = e.Error()
    }
    return strings.Join(msgs, "; ")
}

func (m *MultiError) Add(err error) {
    if err != nil {
        m.Errs = append(m.Errs, err)
    }
}

func (m *MultiError) OrNil() error {
    if len(m.Errs) == 0 {
        return nil
    }
    return m
}

// Usage in multi-node scan:
var merr MultiError
for _, node := range nodes {
    info, err := fetchInfo(ctx, node)
    if err != nil {
        merr.Add(fmt.Errorf("node %s: %w", node.Addr, err))
        continue
    }
    // process info
}
return results, merr.OrNil()
```

## Panic Policy

- **Never** use `panic()` except in `main()` for unrecoverable startup errors
- Use `log.Fatal` in `main()` only — never in library code
- If a goroutine might panic (e.g. third-party lib), add a recover wrapper:

```go
func safeGo(fn func()) {
    go func() {
        defer func() {
            if r := recover(); r != nil {
                logger.Error("recovered from panic", zap.Any("panic", r))
            }
        }()
        fn()
    }()
}
```

## Operational Errors vs Programming Errors

| Type | Example | Handling |
|---|---|---|
| Operational (expected) | Node unreachable, quorum not met | Return typed error, log warn |
| Transient | Redis timeout, K8s API 429 | Retry with backoff, log warn |
| Programming | nil pointer, wrong type assertion | Should not happen — log error + alert |
| Fatal startup | Can't parse config, no sentinel reachable | `log.Fatal` in main |

## Retry Pattern for Transient Errors

```go
func withRetry(ctx context.Context, maxAttempts int, fn func() error) error {
    var lastErr error
    for i := 0; i < maxAttempts; i++ {
        if err := ctx.Err(); err != nil {
            return err
        }
        lastErr = fn()
        if lastErr == nil {
            return nil
        }
        // Exponential backoff, capped at 30s
        wait := time.Duration(1<<uint(i)) * time.Second
        if wait > 30*time.Second {
            wait = 30 * time.Second
        }
        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-time.After(wait):
        }
    }
    return fmt.Errorf("after %d attempts: %w", maxAttempts, lastErr)
}
```
