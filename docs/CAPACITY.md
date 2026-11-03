# Capacity plan

Every number is labelled by source: **given** by the assignment, **measured** in this repository, or **assumed**. Measured numbers come from `docs/results/`, produced by `cmd/loadgen` against `cmd/api`.

## 1. What was measured

**Environment.** One laptop with an Intel i7-7700HQ, 4 cores and 8 threads, and 7.9 GB of RAM. The API, its embedded in-process Redis emulator and the load generator all shared that machine. The emulator has a global lock and interprets Lua in Go, so these are functional floor numbers for the API path, **not** Redis capacity. CI repeats the same run on a 4-vCPU GitHub runner.

**Profile.** 40,000 users arriving over 15 s against 10,000 units, with 90% of holders paying. The generator allows up to 512 requests in flight.

| Run | In-flight caps per instance | Finished in | Throughput | Reserve p99 | Order p99 | Shed with 503 | Oversold | Duplicate charges | Drift |
|---|---|---|---|---|---|---|---|---|---|
| Queueing: caps above the offered concurrency | 1,024 reservations, 1,024 orders | 52 s for a 15 s schedule | 935 requests/sec | 1,009 ms | 194 ms | 0 | 0 | 0 | 0 |
| Shedding: reservation cap below the offered concurrency | 64 reservations, 64 orders | 15 s, on schedule | 3,250 requests/sec | 179 ms | 136 ms | 26,236 reservations, 0 orders | 0 | 0 | 0 |

**What this shows.**

- **Correctness held in both runs.** No unit was sold twice, no order kept two charges, and Redis matched the durable store.
- **Queueing is the failure mode from the last drop.** With 512 requests in flight taking roughly 0.55 s each, Little's Law gives about 930 requests/sec, which matches the measured 935. The 15-second spike took 52 seconds to serve.
- **Shedding fixes it.** Rejecting the excess at once kept admitted latency far under the 500 ms target and raised throughput about 3.5 times. The in-flight cap must therefore be set from measured capacity, not guessed.
- **Paying users are protected.** Order routes have their own pool, so the shedding run shed no payment start. An earlier version shared one pool and shed 1,521 of 8,992 payment starts from users who already held a unit.
- **The autoscaling signal works.** In the queueing run, in-flight requests reached 512, which the HPA formula turns into 11 replicas at 50 per replica. That decision is a simulation from real measurements; no cluster was used.

## 2. Traffic model

| Quantity | Formula | Value | Source |
|---|---|---|---|
| Peak offered traffic | Given | 500,000 requests/sec | Given |
| Reads and writes at 50:1 | 500,000 / 51 | 490,196 reads/sec and 9,804 writes/sec | Given, derived |
| Ramp rate | 498,000 / 30 s | About 16,600 requests/sec added each second | Derived |
| Waiting room polling at the edge | 10M users / 20 s poll | 500,000 requests/sec at the CDN | Given, assumed poll period |
| Waiting room polling at origin | Edge locations / cache TTL | About 300 requests/sec for 300 locations and a 1 s TTL | Assumed |
| Ticket issuance burst | 10M joins / 30 s | About 333,000 tickets/sec at the edge | Assumed, conservative: not all 10M need join in the first 30 s |
| Origin reads | 490,196 x (1 - CDN hit rate) | 24,510, 4,902 or 490 requests/sec at 95%, 99% or 99.9% | Assumed hit rate |

## 3. Payment provider: the real bottleneck

The provider limit is assumed to be a negotiated 1,000 charges/sec. Stripe's published live defaults are far lower: 100 requests/sec per account, 25 per endpoint and 50 requests in progress. Stripe names flash sales as a cause of rate limiting and asks for large increases at least 6 weeks ahead. Charge throughput is the smaller of the rate limit and concurrency divided by provider latency.

| Provider rate | Theoretical time for 50K charges | Time at the 80% cap | Consequence |
|---|---|---|---|
| 25/sec, Stripe's per-endpoint default | 2,000 s | About 42 minutes | The sale cannot finish inside a 15-minute drop: negotiate first |
| 100/sec | 500 s | 625 s | Over 10 minutes; unpaced payments would outlast a 5-minute hold |
| 500/sec | 100 s | 125 s | Fits inside a drop with pacing |
| 1,000/sec, assumed | 50 s | 62.5 s | Fits inside a drop with pacing |

**Admission cap.** Admission rate is at most 0.8 x provider rate / (payment conversion x average attempts per checkout). With conversion 0.8 and 1.25 attempts, both assumed, that is 800 admitted users/sec at 1,000 charges/sec. All provider calls share one quota: refunds and cancellations first, status lookups next, charges last and capped at 80%.

**Backlog.** With backlog B, arrivals λ and provider rate μ, the backlog drains in B / (μ - λ) while arrivals continue below μ, and in B / μ once they stop. At or above μ it cannot drain, so admission must fall.

## 4. Component sizing

| Component | Formula | Planning value | Source |
|---|---|---|---|
| API replicas | Origin traffic / (per-replica rate x 0.6) | 6,235 requests/sec / (3,000 x 0.6) = 4, pre-warmed to 12 across 3 zones for N+2 | Per-replica rate measured as a floor; utilisation assumed |
| Redis operations | Admitted rate x about 3 operations per reservation, plus probes near sell-out | About 2,400 operations/sec at 800 admitted/sec, and about 30,000 if every offered write arrived | Derived; one Redis shard handles this, and buckets spread it for headroom |
| Redis memory | 50K units plus 50K holds at about 200 bytes each | About 10 MB | Assumed bytes per entry, not measured |
| Postgres backend pool | Transactions/sec x duration / 0.6 by Little's Law, summed over API, worker and reconciler | About 2,240 transactions/sec x 5 ms / 0.6 = 19 connections | Duration assumed |
| PgBouncer client connections | Replicas x per-replica pool, summed over every process type | 12 API x 10 + 4 workers x 10 = 160, under `max_client_conn` | Derived |
| Warm capacity | Ramp rate x scale-out time, for traffic the waiting room does not absorb | The waiting room caps origin at the admitted rate, so warm capacity is sized for that rate rather than 500,000 requests/sec | Design |

## 5. How to reproduce

```sh
go build -o bin/api ./cmd/api && go build -o bin/loadgen ./cmd/loadgen
ADDR=:18081 DEV_MODE=true ADMIN_TOKEN=local-admin SALE_UNITS=10000 MAX_INFLIGHT=64 MAX_ORDER_INFLIGHT=64 ./bin/api &
ADMIN_TOKEN=local-admin ./bin/loadgen -target http://localhost:18081 -users 40000 -ramp 15s -concurrency 512
```

Run against a real Redis by setting `REDIS_ADDR` and leaving `DEV_MODE` unset, with real secrets in the environment.
