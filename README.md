# CypherTrap

> In Valorant, Cypher's trap wire detects every enemy that crosses it and stops them cold.
> CypherTrap does the same for your API.

A distributed rate limiting service written in Go. Fast, pluggable, and production-ready — built for startups that need to protect their APIs without paying for a SaaS tool.

---

## Why CypherTrap?

Every serious API needs rate limiting. Most teams bolt it on as an afterthought — a Redis counter in middleware that breaks the moment you scale to multiple servers.

CypherTrap is a **standalone service** that sits in front of your backend. Any server in your fleet asks it: _"is this request allowed?"_ — and gets a sub-millisecond answer backed by atomic, distributed state.

---

## Features

- **Three algorithms** — Token Bucket, Sliding Window Log, Fixed Window Counter. Pick the one that fits your use case.
- **Pluggable storage** — In-memory for single-node dev. Redis for distributed production. Same interface, zero code changes.
- **Multi-tenant rule engine** — Different limits per user tier, API key, or IP. Declare rules in YAML, no deploys needed.
- **HTTP + gRPC API** — REST for simplicity. gRPC for low-latency service-to-service calls.
- **Prometheus metrics** — `/metrics` endpoint out of the box. Track allowed vs blocked, active keys, and p99 latency.
- **Sub-millisecond decisions** — Designed to sit in the critical path without adding latency you can feel.

---

## Architecture

```
Clients (mobile / web / 3rd-party)
        │
        ▼
   Load Balancer
        │
        ├──► CypherTrap ◄──► Redis (shared counters)
        │        │
        │    allow / block
        │        │
        ├──► Auth Service        (allowed only)
        ├──► Business APIs       (allowed only)
        └──► Notifications       (allowed only)
                 │
            429 Too Many Requests (blocked)
```

CypherTrap intercepts every request before it reaches your backend. Blocked requests never touch your services.

---

## Quick Start

### Install

```bash
go install github.com/yourname/cyphertrap@latest
```

### Run with in-memory storage (dev)

```bash
cyphertrap start --config cyphertrap.yaml
```

### Run with Redis (production)

```bash
docker run -p 6379:6379 redis:alpine
cyphertrap start --config cyphertrap.yaml --backend redis --redis-addr localhost:6379
```

---

## Configuration

```yaml
# cyphertrap.yaml

server:
  http_port: 8080
  grpc_port: 9090

backend: memory   # or "redis"

redis:
  addr: localhost:6379
  password: ""
  db: 0

rules:
  - name: free-tier
    key_prefix: "user:free:"
    algorithm: token_bucket
    limit: 100
    window: 1m

  - name: paid-tier
    key_prefix: "user:paid:"
    algorithm: sliding_window
    limit: 10000
    window: 1m

  - name: global-ip
    key_prefix: "ip:"
    algorithm: fixed_window
    limit: 500
    window: 1m
```

---

## API

### HTTP

**Check a key**

```
POST /check
Content-Type: application/json

{
  "key": "user:free:user_123",
  "limit": 100,
  "window": "1m"
}
```

**Response — allowed**

```json
{
  "allowed": true,
  "remaining": 43,
  "reset_at": "2024-01-15T10:01:00Z"
}
```

**Response — blocked**

```json
{
  "allowed": false,
  "remaining": 0,
  "reset_at": "2024-01-15T10:01:00Z",
  "retry_after": 17
}
```

### gRPC

```protobuf
service RateLimiter {
  rpc Check (CheckRequest) returns (CheckResponse);
  rpc Reset (ResetRequest) returns (ResetResponse);
}

message CheckRequest {
  string key    = 1;
  int64  limit  = 2;
  string window = 3;
}

message CheckResponse {
  bool   allowed    = 1;
  int64  remaining  = 2;
  int64  reset_at   = 3;
  int64  retry_after = 4;
}
```

---

## Algorithms

### Token Bucket _(recommended for most APIs)_

A bucket holds N tokens. Each request consumes one. Tokens refill at a steady rate. Allows natural bursting — users can use their quota quickly, then slow down. Feels fair.

**Best for:** General-purpose API rate limiting, user-facing endpoints.

### Sliding Window Log

Tracks the exact timestamp of every request. Computes the count within the last N seconds precisely. More accurate than fixed windows, more memory than token bucket.

**Best for:** Strict per-second or per-minute limits where boundary exploits matter.

### Fixed Window Counter

Simple counter that resets at the start of each window (every minute, every hour). Minimal memory. Has a known edge case — a user can double their effective rate by sending requests at the boundary.

**Best for:** Loose limits, high-volume endpoints where the edge case is acceptable.

---

## Observability

CypherTrap exposes a Prometheus-compatible `/metrics` endpoint:

```
# Requests by outcome
cyphertrap_requests_total{status="allowed"} 48291
cyphertrap_requests_total{status="blocked"} 1203

# Active tracked keys
cyphertrap_active_keys 892

# Decision latency (histogram)
cyphertrap_decision_latency_ms_bucket{le="0.5"} 47100
cyphertrap_decision_latency_ms_bucket{le="1"}   48200
cyphertrap_decision_latency_p99 0.8
```

Plug this into Grafana and alert when block rates spike — usually means an integration gone wrong or an active abuse attempt.

---

## Project Structure

```
cyphertrap/
├── cmd/
│   └── cyphertrap/
│       └── main.go           # Entry point
├── internal/
│   ├── algorithm/
│   │   ├── token_bucket.go   # Token bucket implementation
│   │   ├── sliding_window.go # Sliding window log
│   │   └── fixed_window.go   # Fixed window counter
│   ├── storage/
│   │   ├── interface.go      # Storage interface
│   │   ├── memory.go         # In-memory backend
│   │   └── redis.go          # Redis backend
│   ├── api/
│   │   ├── http.go           # HTTP handlers
│   │   └── grpc.go           # gRPC server
│   ├── rules/
│   │   └── engine.go         # YAML rule loader + matcher
│   └── metrics/
│       └── prometheus.go     # Metrics instrumentation
├── proto/
│   └── ratelimiter.proto     # gRPC definitions
├── cyphertrap.yaml           # Example config
├── docker-compose.yaml       # Redis + CypherTrap stack
├── Makefile
└── README.md
```

---

## Design Decisions

**Why a standalone service instead of middleware?**

Middleware counters are per-process. Two server instances = two independent counters = users can bypass limits by hitting different nodes. A standalone service with shared Redis state solves this cleanly. It also means any service in your stack — not just one — can enforce rate limits.

**Why Go?**

Go's goroutine model handles high-concurrency check requests cheaply. The standard library's `sync` primitives make the in-memory backend safe without external dependencies. And the binary compiles to a single static executable — trivial to deploy.

**Why support multiple algorithms?**

No single algorithm is best everywhere. Token bucket is natural for user-facing APIs. Sliding window is better when boundary exploits are a real risk. Fixed window is simpler and cheaper for internal services. Hiding the choice behind a config key costs almost nothing and makes CypherTrap useful across more scenarios.

**What I'd add with more time**

- Leader election so CypherTrap itself can run in HA mode without Redis as a dependency
- Adaptive limits — automatically tighten limits when downstream services are degraded
- WebSocket support for real-time limit status streaming to clients

---

## Roadmap

- [x] Token bucket algorithm
- [x] Sliding window log
- [x] Fixed window counter
- [x] In-memory storage backend
- [x] Redis storage backend
- [x] HTTP API
- [x] gRPC API
- [x] YAML rule engine
- [x] Prometheus metrics
- [ ] Admin dashboard (live block/allow rates per key)
- [ ] HA mode without Redis dependency
- [ ] Adaptive rate limiting

---

## Running Tests

```bash
# Unit tests
make test

# Integration tests (requires Redis)
make test-integration

# Benchmark decision latency
make bench
```

---

## License

MIT — use it, fork it, ship it.

---

<p align="center">
  Built with Go · Inspired by Valorant's Cypher · Stops what pushes too hard
</p>