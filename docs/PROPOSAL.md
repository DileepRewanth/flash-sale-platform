# Spike-Resilient Flash Sale and Ticketing Platform: Technical Proposal

Every claim in this document carries one of three labels, and [CLAIMS.md](CLAIMS.md) links each to its evidence.

- **[Built]** means implemented in this repository and covered by tests.
- **[Measured]** means measured in this repository, then extrapolated with a stated formula.
- **[Design]** means proposed architecture that is not implemented here.

---

## 1. Problem understanding and goals

### 1.1 What went wrong in the last drop

| Symptom | Root cause in a naive design | What this design changes |
|---|---|---|
| p99 rose from 300 ms to 25 s | Requests queued behind a saturated database and slow provider calls | Waiting room caps origin load, origin sheds instead of queueing, the provider call is off the request path |
| Database hit connection limits | Every request held a connection, some across the provider call | Reservations never touch the database, the client never waits on the provider, pools are sized by Little's Law |
| Users charged without an order | The charge happened before a durable record, and timeouts were treated as failures | No charge without a durable attempt and a durably owned unit, and unknown never means failed |
| Same seat sold twice | Check-then-act on a shared counter, or a cache treated as the source of truth | Every unit has one durable owner, changed only by a guarded update from FREE |

### 1.2 Failure modes of a naive architecture under a 250x spike

- **Hot rows.** A single `available_count` row for the hot item serialises every reservation behind one row lock. Throughput collapses to one row's lock rate and latency becomes the queue length.
- **Connection exhaustion.** Replicas times pool size exceeds the database limit as autoscaling adds replicas. Holding connections across a slow provider call multiplies the problem.
- **Retry storms.** Clients and services retry immediately on errors. Offered load multiplies exactly when capacity is lowest.
- **Thundering herd.** Everyone refreshes at the opening second, caches are cold, and many requests recompute the same value at once.
- **Lost or duplicated money movements.** A timeout is treated as a failure, the unit is resold, and the original charge lands anyway.

### 1.3 Goals

| Goal | Target |
|---|---|
| Peak offered traffic | 500,000+ requests/sec, absorbed mostly at the edge |
| Reservation latency | p99 under 500 ms for admitted reservation requests |
| Availability | 99.95% of reservation requests, plus per-drop reporting |
| Oversell and retained duplicate charges | Zero, with real-time detection |
| Time to scale out | Pre-warmed before scheduled drops; unscheduled spikes absorbed by the waiting room while autoscaling catches up |
| Recovery | See section 3.8 for data loss and recovery time targets per failure |

### 1.4 Strict versus degradable guarantees

| Strict, never traded away | Allowed to degrade under load |
|---|---|
| No unit has two owners | Freshness of availability counts |
| No charge is kept twice for one order | Queue ordering, which is approximate FIFO |
| No charge without a durable order or a refund | Time spent in the waiting room |
| No paid order is lost | Speed of confirmation, which is asynchronous |
| Explicit cancellation always ends in a refund | Non-essential features such as recommendations |

### 1.5 Assumptions added to the brief

| Assumption | Value | Why it matters |
|---|---|---|
| Hold duration | 5 minutes, one unit per user per sale | Bounds how long stock is tied up by unpaid holds |
| Payment start grace | 2 minutes after the hold expires | Covers queueing before a charge is sent |
| Hard deadline for unresolved payments | 30 minutes | Longer than a drop, so a provider outage undersells instead of risking refunds |
| Provider throughput | 1,000 charges/sec negotiated; Stripe's published live default is 100 requests/sec per account and 25 per endpoint | The provider, not our servers, bounds the sale |
| Provider API | Idempotency keys, status lookup, cancel by our reference, signed webhooks, a list API | Needed for reconciliation |
| Charge model | One-step charge | The worst case: a late success needs a real refund. Authorize-then-capture is a later improvement |
| Authentication | Done at the gateway | Out of scope; the prototype trusts an `X-User-ID` header |

---

## 2. Proposed architecture

The diagram and a walkthrough are in [architecture/README.md](architecture/README.md).

### 2.1 Request flow

1. **Client to CDN.** Catalog, availability snapshots and waiting room status are served from the edge. **[Design]**
2. **Edge.** WAF and bot scoring filter traffic, and the waiting room issues signed tickets. **[Design]**
3. **Gateway.** Authentication and per-user and per-IP rate limits. **[Design]**
4. **Reservation.** An admitted user gets a Redis hold and a signed hold token. **[Built]**
5. **Order creation.** The hold token is exchanged for a durable order: one transaction claims the unit and writes the payment attempt. The client gets `202` at once. **[Built]**
6. **Payment.** A worker sends the charge with the attempt ID as idempotency key. Webhooks and a reconciler settle every outcome through one state machine. **[Built against a fake provider]**
7. **Confirmation.** The client polls the order. **[Built]**

### 2.2 Traffic shaping at the edge

- **Virtual waiting room [Design].** Tickets are stateless signed tokens carrying the arrival time plus random jitter inside a one-second bucket, so no central counter exists. Users who arrive before a scheduled opening are randomised across a window, so arriving early gives no edge. A controller advances one published watermark, paced by downstream health and payment capacity. Status is a CDN-cached document, so origin load does not grow with queue size. With 10M users polling every 20 s, the edge sees 500,000 requests/sec while the origin sees about one request per edge location per cache TTL.
- **Rate limiting and bot protection [Design].** Bought from the CDN or WAF vendor: per-IP and per-account limits, bot scoring and proof-of-work challenges for suspicious clients. The one-unit-per-user rule and the per-user claim in Redis **[Built]** make hoarding unprofitable.
- **Request coalescing [Design].** CDN request collapsing plus short TTLs on availability documents. Availability is shown in bands rather than exact counts, which is both cacheable and allowed to be stale.
- **Origin load shedding [Built].** Reservations, order routes and webhooks each have their own concurrency pool, a bulkhead, so a reservation flood can never shed a user who is already paying. When a pool is full the request is rejected at once with a jittered `Retry-After`, rather than queued.

### 2.3 Inventory reservation design [Built]

- **Per-unit identity.** Every unit, seat or not, has an ID. The same model serves "any unit" and "this exact seat".
- **Atomic hold.** A Lua script pops a unit from a Redis set and records the hold with its expiry in one step, so two users can never pop the same unit.
- **Hot-key handling.** One hot item is split into N buckets. Each bucket's keys share a hash tag, so a bucket lives on one Redis Cluster slot and the script stays legal, while different buckets spread across shards. Users start at different buckets and probe the rest before hearing "sold out". An unreachable bucket produces a retry answer, never a false sold out.
- **TTL holds and release.** Holds are not Redis key TTLs, because expiry events are best effort and cannot update a set atomically. They live in a sorted set read by a sweeper. Before returning a unit, the sweeper checks the durable store: a unit that payment already claimed is never put back.
- **Redis is a gate, not the truth.** The durable store decides ownership. Losing Redis data can reject a hold, but it cannot double-sell a unit, and Redis can be rebuilt from durable FREE units.

### 2.4 Payment sequence and lifecycle [Built]

No charge is sent unless a durable attempt exists and the unit is durably owned by that order.

| Step | Component | Transaction boundary |
|---|---|---|
| 1 | Reservation: Lua hold, signed token | Redis only |
| 2 | Order API: idempotency key with request hash, guarded claim FREE to RESERVED, order, attempt, outbox | One durable transaction, then `202` |
| 3 | Worker: lease the attempt, check the deadline, mark it UNKNOWN before sending | One short transaction |
| 4 | Worker: charge with the attempt ID as idempotency key | No transaction open |
| 5 | Worker, webhook or reconciler: apply the result through the state machine | One transaction |
| 6 | Client: poll the order | Read only |

Each attempt records two separate facts: what the provider said, and what the platform decided about the money.

```text
Provider outcome: NOT_SENT -> UNKNOWN -> SUCCEEDED | FAILED | CANCELLED
                  UNKNOWN -> NOT_SENT   only for an explicit not-processed answer to the only dispatch
                  FAILED or CANCELLED -> SUCCEEDED   the provider reports a late success
Disposition:      NONE -> RETAINED | REFUND_PENDING;  RETAINED -> REFUND_PENDING;  REFUND_PENDING -> REFUNDED
Order:            PENDING -> CONFIRMED | CANCELLED | EXPIRED
                  EXPIRED -> CONFIRMED   only after a late success re-acquires a unit
                  CONFIRMED, CANCELLED or EXPIRED -> REFUNDING -> REFUNDED
```

- **Unknown never means failed.** A timeout leaves the attempt UNKNOWN and the unit reserved until the provider answers or the 30-minute hard deadline passes.
- **Late success policy.** Re-acquire the same unit, else any free interchangeable unit, else refund. Refund also when the user has since placed another active order, and always after an explicit cancellation.
- **Re-send rule.** Same-key re-sends are allowed only for 5 minutes after the first send, far inside provider key retention: Stripe can prune keys after 24 hours and treats a pruned key as new, and Adyen keeps keys 7 to 14 days. After the window only status lookups and cancellation by reference are allowed. "Not found" is never treated as failure.

### 2.5 Async order pipeline

- **Outbox [Built, in memory].** State changes and outgoing work are written together: a NOT_SENT attempt is the payment work item, and release and take events carry inventory changes back to Redis with a version check.
- **Queue choice [Design].** Kafka, fed from the Postgres outbox by a change-data-capture relay. Kafka gives durable replay, consumer groups and per-key ordering. Partition by **order ID**, not item ID, because one hot item would otherwise land on one partition. Ordering is needed only per order, and consumers are idempotent because delivery is at least once.

### 2.6 Which data belongs where

| Data | Store | Why |
|---|---|---|
| Free units, holds, per-user claims | Redis **[Built]** | Atomic in-memory operations at high rate, rebuildable from the durable store |
| Unit ownership, orders, attempts, refunds, webhook dedupe, outbox | Postgres **[Design]**, in-memory store mirroring its transactions **[Built]** | Transactions, unique constraints and row locks give the strict guarantees |
| Order and payment events | Kafka **[Design]** | Durable, replayable, partitioned pipeline |
| Order status read model for global reads | Regional read replicas or a NoSQL table such as DynamoDB **[Design]** | High read volume with eventual consistency |
| Analytics and reconciliation history | Columnar store such as ClickHouse or BigQuery, fed from Kafka **[Design]** | Large scans without touching the transactional store |
| Waiting room state | Signed tokens plus a CDN-cached watermark **[Design]** | No per-user server state for 10M users |

### 2.7 Caching strategy

- **CDN [Design].** Catalog pages for minutes, availability bands and waiting room status for 1 to 2 seconds, with request collapsing.
- **Redis [Built].** The inventory gate itself.
- **Local in-process cache [Design].** Sale configuration and availability bands for under a second, so a cache miss storm cannot reach Redis.
- **Never cached.** Ownership and payment state, which always come from the durable store.

### 2.8 Pre-warming versus autoscaling

Traffic grows by about 16,600 requests/sec every second during the 30-second ramp, faster than any metric-driven scaler can react.

- **Scheduled drops.** Pre-warm with a cron trigger before the opening: replicas, Redis shards, connection pools and CDN caches. **[Design, config in `deploy/k8s`]**
- **Unscheduled spikes.** The waiting room is always armed and admits only what current capacity can serve, so autoscaling never sits on the correctness path. Autoscaling then grows capacity and the admission rate follows. **[Design]**
- **Scaling signal.** In-flight requests per replica, exported by the API. **[Built]** The KEDA trigger consumes it **[Design, config written]**, and the load generator replays real measurements through the HPA formula **[Measured, simulation]**.

### 2.9 Security and card data

- **Card data never touches our servers [Design].** The client enters card details into the provider's hosted fields or SDK, and our API receives only a payment token. Card numbers stay out of our systems, which shrinks PCI DSS scope to the simplest self-assessment questionnaire.
- **Hold tokens [Built].** HMAC-SHA256 signed, verified in constant time, bound to one user and short-lived.
- **Webhooks [Built].** HMAC over the timestamp and body, a 5-minute replay window, and deduplication by event ID.
- **Admin endpoints [Built].** A bearer token compared in constant time. In production they also sit behind the internal network and single sign-on **[Design]**.
- **Secrets [Built].** Read only from the environment. Outside development mode the service refuses to start without them, and CI scans the full git history for leaks.
- **Abuse [Design].** Bot scoring and per-account and per-IP limits at the edge, on top of the one-unit-per-user rule that Redis and the durable store both enforce **[Built]**.

---

## 3. Scalability, performance and fault tolerance

### 3.1 Horizontal scaling and autoscaling signals

Services are stateless: every API node verifies tokens with a shared key and reaches state only through Redis and the durable store. CPU alone is too slow a signal, because it rises only after requests are already queued, and averages hide saturation of a few nodes. Leading signals are in-flight requests per replica, the admission backlog, outbox depth and the age of the oldest outbox item. Scale-down is slow, with a 5-minute stabilisation window, because drops end abruptly and can restart.

### 3.2 Sharding and partitioning

- **Inventory.** Buckets per hot item, bucket keys hash-tagged to one slot, buckets spread across Redis shards. More buckets mean more parallelism but more probing near sell-out.
- **Durable store.** Partition units and orders by sale. One sale's writes stay on one primary, which keeps the guarded update a single-row operation, while sales spread across shards.
- **Events.** Kafka partitioned by order ID.

### 3.3 Backpressure and load shedding

Reject in this order, cheapest and least valuable first:

1. Anonymous browse traffic, which the CDN serves stale.
2. Waiting room joins beyond the bot and rate limits.
3. New admissions, by slowing the watermark.
4. Reservation requests from admitted users, with `Retry-After`.
5. Protected by their own pools, so reservation traffic can never shed them: order creation and order status for users mid-payment, and payment webhooks. The payment worker serves no external requests at all.

The three origin pools are **[Built]**; the edge stages are **[Design]**.

### 3.4 Retries, backoff and jitter

- **Clients** use exponential backoff with full jitter and honour `Retry-After`, which the API sends jittered **[Built]**.
- **Services** use a retry budget, such as retries capped at 10% of requests, so retries cannot multiply load during an outage **[Design]**.
- **Payment calls** are never retried blindly: a re-send uses the same key inside the re-send window only **[Built]**.

### 3.5 Circuit breakers, bulkheads and timeouts

- **Bulkheads [Built].** Separate concurrency pools for reservations, order routes and webhooks.
- **Circuit breaker around the provider [Design].** It opens on error rate or latency. While open, admissions slow and queued attempts wait rather than fail, because a payment may not be abandoned while its outcome is unknown.
- **Timeouts [Design for production values].** Every call carries a deadline shorter than its caller's. The provider timeout is short, and the reconciler, not a long timeout, resolves the outcome.

### 3.6 Idempotency and exactly-once effects [Built]

| Operation | Mechanism |
|---|---|
| Reservation | One claim per user per sale in Redis, so a double click returns the same hold |
| Order creation | Idempotency key stored with a request hash: the same key and body replays, a different body is rejected |
| Charge | Attempt ID as the provider idempotency key, with re-sends bounded by the re-send window |
| Webhooks | Deduplicated by provider event ID in the same transaction that applies them |
| Refund | Refund key derived from the attempt, so a retried refund is never doubled |
| Inventory release | Set semantics plus a version check, so duplicate or stale events are harmless |

Exactly-once *effects* come from at-least-once delivery plus idempotent consumers and a single state machine.

### 3.7 Partial failures

| Failure | Behaviour |
|---|---|
| Payment succeeded but the order write failed | The unit stays RESERVED to the order. The reconciler looks the charge up and confirms it **[Built]** |
| Reservation expired mid-payment | An UNKNOWN attempt protects the unit until the hard deadline. A success after that re-acquires a unit or is refunded **[Built]** |
| Queue consumer crash | Attempts were marked UNKNOWN before the call, so a lookup or a same-key re-send resolves them **[Built]** |
| Durable claim conflicts | `409`, no attempt written, no charge sent **[Built]** |
| Redis loses data | Payment steps do not need Redis. Rebuild adds only durable FREE units **[Built]** |
| Provider outage | Units stay reserved, admissions slow, the reconciler resolves outcomes afterwards **[Built for resolution, Design for the breaker]** |

### 3.8 Multi-AZ and multi-region [Design]

- **One home region per sale.** Only the home region may change unit ownership. Reads are global through the CDN and replicas.
- **Within a region.** Stateless services across three zones, Redis with replicas, Postgres with a synchronous replica in another zone.
- **Across regions.** Asynchronous Postgres replication to a pre-warmed standby. Redis is rebuilt, not replicated.
- **Fencing.** A quorum store such as etcd across three regions holds the sale's epoch and lease. A local fence row in each database bridges the two systems: every ownership update checks the epoch and an unexpired lease in the same SQL statement, and ownership transactions run under a `transaction_timeout`. The old region fences itself when it cannot renew. Each region uses its own provider credential, because the provider is the only gate a paused process cannot slip past. The old credential is revoked automatically once the old lease plus a margin has expired, without waiting for operator approval: the old region is already fenced by then, so revocation cannot cost any availability.
- **Failover.** An operator-approved compare-and-set raises the epoch after the old lease plus a margin has passed. Inherited UNKNOWN attempts are never re-sent from the new region: Adyen does not check idempotency keys across its regions. They are resolved by lookup, cancellation and a provider ledger scan, and their units stay quarantined while other stock sells.

| Failure | Data loss target | Selling recovery target |
|---|---|---|
| Redis instance | Unpaid holds only | Under 30 s for that instance's buckets |
| One zone | None for orders | Under 60 s |
| Whole region | Replication lag under 5 s, paid orders recovered from the provider ledger | About 4.4 minutes with assumed provider timings, against a 5-minute target |

The figure adds the lease and margin, credential revocation, the provider's maximum request lifetime, the ledger visibility delay, and the ledger scan with the Redis rebuild: 30 + 15 + 10 + 120 + 60 + 30 = 265 s. Operator approval and promotion run in parallel inside the provider wait. If revocation waited for approval, recovery would take about 5.4 minutes and miss the target. All six inputs are assumptions to confirm with the real provider and infrastructure. Consistency outranks availability for inventory, so a region failure inside a drop is reported as a breach.

### 3.9 Sustaining 99.95%, including deployments and dependency outages

- **Deployments.** Freeze before scheduled drops. Rolling deploys with readiness checks and graceful shutdown that drains in-flight requests **[Built for shutdown]**.
- **Dependency outages.** Redis down returns retry answers rather than errors that mislead users. Provider down keeps units reserved and pauses admissions. Database down blocks only order creation, while holds continue.
- **Kill switches [Built].** Pause reservations, rebuild Redis inventory and repair drift through admin endpoints.

---

## 4. Load testing and observability

### 4.1 Test plan

| Profile | Shape | Pass criteria |
|---|---|---|
| Spike | Baseline to peak in 30 s, hold 5 to 15 minutes | Admitted p99 under 500 ms, excess shed rather than queued, zero oversell |
| Soak | Peak admission rate for 2 hours | No memory growth, no drift, stable p99 |
| Stress | Step up until saturation | Graceful degradation: shedding rises, errors do not |
| Dependency failure | Provider timeouts, Redis loss, worker crash | Strict guarantees hold, recovery within targets |
| Game day | Full drop rehearsal before every major sale | Runbook steps executed by the on-call team |

The load generator **[Built]** runs the spike shape against the real API and audits correctness at the end. Failure injection is built as integration tests: provider timeouts, late successes, key expiry, worker crash after marking an attempt, Redis wipe and rebuild **[Built]**. A chaos platform for production game days is **[Design]**.

### 4.2 Signals that matter

| Signal | Metric |
|---|---|
| Saturation, the autoscaling signal | `flashsale_http_inflight_requests` **[Built]** |
| Shedding | `flashsale_http_shed_total` **[Built]** |
| Reservation results | `flashsale_reservations_total` by result **[Built]** |
| Latency | `flashsale_http_request_duration_seconds` by route **[Built]** |
| Payment results | `flashsale_payment_results_total` **[Built]** |
| Correctness | Oversold units, duplicate charges, charged without order, stuck payments, drift **[Built]** |
| Queue depth and oldest item age | Kafka lag and outbox age **[Design]** |

### 4.3 SLIs, SLOs and alerting

- **Availability counts requests, not minutes.** A time-based 99.95% allows 21.6 minutes a month, which could hide a total outage of a 15-minute drop. A sold-out answer counts as success. Shed requests are reported as their own rejection rate.
- **Per drop.** The same ratio is reported for every drop, plus selling pause time, because a paused waiting room produces no failed requests.
- **Alert on symptoms and burn rate, not traffic.** A multi-window burn-rate alert fires when the error budget burns 14.4 times too fast over both 5 minutes and 1 hour, so an intentional spike does not page anyone while real failures do. Correctness alerts page at the first occurrence.

Alert rules are in `deploy/prometheus`. All seven pass promtool's syntax check in CI, and five have firing tests; the latency and burn-rate rules do not **[Built]**. No Prometheus server or Alertmanager runs in this repository, so no alert has been delivered end to end **[Design]**.

### 4.4 Detecting oversell, duplicate charges and stuck reservations [Built]

Independent detectors read the durable store, Redis and the provider ledger directly, rather than trusting the code that wrote them. They catch units with two confirmed orders, kept charges beyond one per order, charges with no confirmed order, payments unresolved past the hard deadline, and Redis drift in both directions. Drift seen in two consecutive scans is repaired automatically.

### 4.5 Live incident runbook

[RUNBOOK.md](RUNBOOK.md) covers detection, kill switches, manual scale-up and communication for each failure.

---

## 5. Trade-off analysis

| Trade-off | Decision | Cost accepted |
|---|---|---|
| Fairness versus throughput and simplicity | Approximate FIFO from timestamped tickets and randomised pre-queue | Ordering can invert within a one-second bucket |
| Strong inventory consistency versus availability under partition | Consistency: one home region, fenced writes | A region failure pauses selling for minutes |
| Synchronous versus asynchronous confirmation | Synchronous hold and durable claim, asynchronous payment confirmation | Clients poll, and confirmation can take seconds |
| Over-provisioning versus autoscaling lag | Pre-warm for scheduled drops, waiting room for unscheduled spikes | Idle capacity before drops |
| Build versus buy | Buy CDN, WAF, bot protection and managed Kafka; build the waiting room logic, inventory and payment state machine | Vendor cost and lock-in at the edge |

Each is detailed in [adr/](adr/).

---

## 6. Execution plan

| Phase | What ships | Validation before the next drop |
|---|---|---|
| v0 | A bought waiting room and rate limits in front of the current system, idempotency keys on payment calls, and the detectors running against current data | Shadow load test at 2x the last peak; detectors show the current oversell and orphan-charge rate as a baseline |
| v1 | The new reservation gate and payment state machine, first in shadow mode: they process mirrored traffic and record decisions, but the shadow path never charges a card | Shadow decisions match or beat the old path on a full drop; then a canary on small drops at 1%, 10% and 50% of traffic |
| v2 | Full cutover, pre-warming, the autoscaling signal, Kafka, and the multi-region standby with fencing | A game day with region failover rehearsed; a spike test at the full 500,000 requests/sec profile before the next major drop |

Each phase has rollback criteria: any non-zero oversell or duplicate-charge alert, or the burn-rate alert, returns traffic to the previous path.
