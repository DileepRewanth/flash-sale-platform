# Spike-Resilient Flash Sale and Ticketing Platform

A technical strategy and architecture proposal for a checkout platform that spikes from 2,000 to 500,000+ requests/sec in 30 seconds, plus a focused Go prototype of the parts that must be correct: reservations, the payment lifecycle, reconciliation and monitoring.

Every claim is labelled **built and tested**, **measured and extrapolated**, or **design only**. See [docs/CLAIMS.md](docs/CLAIMS.md).

## Problem summary

The last drop pushed p99 latency from 300 ms to 25 s, exhausted database connections, charged some users without giving them an order, and sold one seat twice. The first two are capacity failures. The last two are correctness failures, which this design treats as strict guarantees.

## Key decisions

| Decision | Record |
|---|---|
| Redis is the fast reservation gate, the durable store is the single commit point for ownership | [ADR 0001](docs/adr/0001-redis-gate-durable-commit.md) |
| Every unit has an ID; the hot item is split into Redis buckets | [ADR 0002](docs/adr/0002-per-unit-identity-and-buckets.md) |
| Waiting room with approximate FIFO and paced admission | [ADR 0003](docs/adr/0003-waiting-room-approximate-fifo.md) |
| Synchronous reservation, asynchronous payment confirmation | [ADR 0004](docs/adr/0004-async-payment-confirmation.md) |
| One payment state machine, provider outcome separate from the money decision | [ADR 0005](docs/adr/0005-payment-state-machine.md) |
| Transactional outbox, Kafka partitioned by order ID | [ADR 0006](docs/adr/0006-outbox-and-kafka.md) |
| Pre-warm for scheduled drops, autoscale on in-flight requests | [ADR 0007](docs/adr/0007-prewarm-plus-autoscale.md) |
| One home region per sale with lease fencing | [ADR 0008](docs/adr/0008-single-home-region-fencing.md) |
| Build versus buy | [ADR 0009](docs/adr/0009-build-vs-buy.md) |
| Request-based availability, reported per drop, alerted by burn rate | [ADR 0010](docs/adr/0010-request-based-slos.md) |

## Assumptions

The full list is in section 1.5 of the proposal. The ones that shape the design most:

- Holds last 5 minutes, one unit per user per sale, with a 30-minute hard deadline for unresolved payments.
- The payment provider allows a negotiated 1,000 charges/sec. Default provider limits are far lower, which makes the provider the real bottleneck.
- Authentication happens at the gateway; the prototype trusts an `X-User-ID` header.

## Measured results

Spike of 40,000 users over 15 s against 10,000 units, on one laptop with an embedded Redis emulator. Details and caveats are in [docs/CAPACITY.md](docs/CAPACITY.md).

| Run | Throughput | Reserve p99 | Order p99 | Oversold | Duplicate charges |
|---|---|---|---|---|---|
| Without effective shedding | 935 requests/sec | 1,009 ms | 194 ms | 0 | 0 |
| With load shedding | 3,250 requests/sec | 179 ms | 136 ms | 0 | 0 |

## How to read this repository

1. [docs/PROPOSAL.md](docs/PROPOSAL.md): all six sections of the assignment.
2. [docs/architecture/README.md](docs/architecture/README.md): the HLD diagram, its Mermaid source and a walkthrough.
3. [docs/adr/](docs/adr/): decision records with alternatives and trade-offs.
4. [docs/CAPACITY.md](docs/CAPACITY.md): measured numbers and the capacity model.
5. [docs/RUNBOOK.md](docs/RUNBOOK.md): live incident response and kill switches.
6. [docs/CLAIMS.md](docs/CLAIMS.md): every claim with its status and evidence.
7. [db/schema.sql](db/schema.sql): the target Postgres schema, which is design only.

## Repository map

```text
cmd/api            HTTP API, payment worker and detectors in one process
cmd/loadgen        Spike load generator with a correctness audit and autoscaling simulation
internal/inventory Redis gate: Lua holds, buckets, sweeper, rebuild
internal/store     Durable store, in memory, mirroring the Postgres transactions
internal/payment   Pure payment state machine and late-success policy
internal/psp       Fake payment provider with realistic failure modes
internal/worker    Dispatch, reconciliation, expiry, refunds, outbox relay
internal/reconcile Oversell, duplicate charge, orphan charge and drift detectors, plus repair
internal/httpapi   Routes, load shedding, webhooks, admin kill switches
internal/token     HMAC-signed hold tokens
internal/metrics   Prometheus metrics used by alerts and autoscaling
deploy/prometheus  Alert rules and their promtool tests
deploy/k8s         KEDA autoscaling config, design only
docs/              Proposal, diagrams, decision records, capacity, runbook, claims
```

## How to run

Requires Go 1.22 or later. No external services are needed in development mode.

```sh
# All tests, including the concurrency tests, under the race detector
go test -race ./...

# Run the API with an embedded Redis and generated secrets
DEV_MODE=true ADMIN_TOKEN=local-admin go run ./cmd/api

# Reserve, then create an order with the returned hold token
curl -s -X POST -H "X-User-ID: alice" localhost:8080/v1/sales/drop-1/holds
curl -s -X POST -H "X-User-ID: alice" -H "Idempotency-Key: k1" \
     -d '{"hold_token":"<token>"}' localhost:8080/v1/orders

# Audit correctness
curl -s -H "Authorization: Bearer local-admin" localhost:8080/v1/admin/sales/drop-1/audit

# Spike test
ADMIN_TOKEN=local-admin go run ./cmd/loadgen -users 40000 -ramp 15s
```

Configuration is read from environment variables; see [.env.example](.env.example). Outside development mode, `REDIS_ADDR`, `HOLD_TOKEN_SECRET`, `WEBHOOK_SECRET` and `ADMIN_TOKEN` are required, and the service refuses to start without them.

## Limitations

- The durable store is in memory. It mirrors the Postgres transactions and constraints in `db/schema.sql`, but no Postgres is used.
- The payment provider is a fake with scripted failure modes; no real provider integration exists.
- The CDN, WAF, bot protection, waiting room, Kafka, circuit breaker, retry budgets and multi-region failover are design only.
- Load numbers come from one laptop with an embedded Redis emulator. They show behaviour and give floor numbers, not production capacity.
