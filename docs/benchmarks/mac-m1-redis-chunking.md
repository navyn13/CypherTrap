# Redis Check latency: without vs with chunking

**Machine:** Apple MacBook Air — **Apple M1** (arm64), 8 cores, 8 GB RAM  
**OS:** macOS 14.6.1 (Build 23G93) / Darwin 23.6.0  
**Redis:** 8.4.0 on `localhost:6379` (same machine)  
**Date:** 2026-07-11  
**Code:** `internal/ratelimit` chunked batcher (`checkChunkSize=64`, `checkMaxWait=100µs`)

## What was measured

End-to-end wait from the **caller** submitting a fixed-window rate-limit check until the allow/block result is returned.

| Mode | Path |
|------|------|
| **Without chunking** | One Lua `EVALSHA` (`INCR` + `PEXPIRE`) **per** `Check` |
| **With chunking** | Concurrent checks coalesce into batches of up to **64** keys; **one** multi-key Lua `EVALSHA` per batch |

Samples: **200 checks per worker**. Percentiles are over all samples for that concurrency.

Reproduce:

```bash
REDIS_URL=redis://localhost:6379 go test ./internal/ratelimit/ -run TestCompareCheckLatency -v -count=1
```

## Results (Apple M1)

### Summary table (p50 / p99)

| Concurrency | Samples | Without p50 | With p50 | Without p99 | With p99 |
|-------------|---------|-------------|----------|-------------|----------|
| 1 | 200 | **87µs** | **183µs** | 264µs | 309µs |
| 8 | 1,600 | **164µs** | **189µs** | 438µs | 336µs |
| 64 | 12,800 | **1.24ms** | **238µs** | 2.55ms | 590µs |
| 256 | 51,200 | **5.71ms** | **872µs** | 9.90ms | 1.47ms |

256 workers matches CypherTrap’s `messageWorkerCount`.

### Full percentiles

#### Concurrency = 1

| Metric | Without chunking | With chunking |
|--------|------------------|---------------|
| p50 | 87µs | 183µs |
| p95 | 184µs | 245µs |
| p99 | 264µs | 309µs |
| mean | 106µs | 196µs |
| Approx Redis RTTs | 200 | ~4 |

#### Concurrency = 8

| Metric | Without chunking | With chunking |
|--------|------------------|---------------|
| p50 | 164µs | 189µs |
| p95 | 268µs | 242µs |
| p99 | 438µs | 336µs |
| mean | 178µs | 196µs |
| Approx Redis RTTs | 1,600 | ~25 |

#### Concurrency = 64

| Metric | Without chunking | With chunking |
|--------|------------------|---------------|
| p50 | 1.24ms | 238µs |
| p95 | 2.06ms | 334µs |
| p99 | 2.55ms | 590µs |
| mean | 1.42ms | 253µs |
| Approx Redis RTTs | 12,800 | ~200 |

#### Concurrency = 256 (server worker count)

| Metric | Without chunking | With chunking |
|--------|------------------|---------------|
| p50 | 5.71ms | 872µs |
| p95 | 7.07ms | 1.15ms |
| p99 | 9.90ms | 1.47ms |
| mean | 5.88ms | 900µs |
| Approx Redis RTTs | 51,200 | ~800 |

## Takeaways

- **Low load (1 goroutine):** chunking is slower (~2× p50) because of coalesce wait (`checkMaxWait`) when batches rarely fill.
- **High load (64–256):** chunking wins — about **5–6× lower p50** at 256 workers, since ~64 checks share one Redis round trip instead of contending for one RTT each.
- Counters remain in **Redis** (shared source of truth). Chunking only reduces round trips; it does not move limits into process-local memory.

## Related code

- [`internal/ratelimit/batcher.go`](../../internal/ratelimit/batcher.go) — chunk batcher + multi-key Lua
- [`internal/ratelimit/algorithm.go`](../../internal/ratelimit/algorithm.go) — `Check` → `batcher.submit`
- [`internal/ratelimit/latency_test.go`](../../internal/ratelimit/latency_test.go) — `TestCompareCheckLatency`
