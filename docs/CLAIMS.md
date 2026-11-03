# Claims matrix

Each feature is labelled built and tested, measured and extrapolated, or design only. Nothing labelled design is claimed to work.

| Claim | Status | Evidence |
|---|---|---|
| Atomic Redis holds never give one unit to two users | Built and tested | `internal/inventory`: `TestNoOversellUnderConcurrency` |
| A double click returns the same hold | Built and tested | `TestRepeatReserveReturnsSameHold` |
| Hot item split into buckets, with probing before sold out | Built and tested | `TestEmptyHomeBucketProbesOtherBuckets` |
| An unreachable bucket is never reported as sold out | Built and tested | `TestBrokenBucketIsNotReportedAsSoldOut` |
| Expired holds never return a durably claimed unit | Built and tested | `TestSweepNeverReturnsDurablyClaimedUnit` |
| Only one order can claim a unit | Built and tested | `internal/store`: `TestOnlyOneOrderWinsAUnit` |
| Idempotent order creation with a request fingerprint | Built and tested | `TestCreateOrderIsIdempotent`, `TestReserveOrderAndConfirmOverHTTP` |
| One active order per user per sale | Built and tested | `TestOneActiveOrderPerUser` |
| Unresolved payments protect their unit until the hard deadline | Built and tested | `TestUnresolvedPaymentProtectsUnitUntilHardDeadline` |
| Timeouts resolved by lookup, never by a blind re-send | Built and tested | `internal/worker`: `TestTimeoutAfterChargeIsResolvedByLookupNotResend` |
| No re-send after the re-send window, even when keys are pruned | Built and tested | `TestNoResendAfterWindowEvenWhenKeysAreForgotten`, `TestForgottenKeysStillResolveByLookup` |
| Late success re-acquires a unit or is refunded | Built and tested | `TestLateSuccessAfterExpiryReacquiresSameUnit`, `TestLateSuccessAfterUnitResoldIsRefunded` |
| Competing successful attempts keep exactly one charge | Built and tested, under the race detector | `TestCompetingSuccessesKeepOneCharge` |
| Cancellation during an in-flight payment | Built and tested | `TestCancelDuringInFlightPayment` |
| A stale queued attempt is never charged | Built and tested | `TestQueuedAttemptPastDeadlineIsNeverCharged` |
| Webhooks are signed, timestamped and deduplicated | Built and tested | `TestDuplicateWebhookAppliesOnce`, `TestWebhookSignatureAndDuplicates` |
| Redis wipe and rebuild causes no oversell | Built and tested | `TestRedisWipeAndRebuildCausesNoOversell` |
| Oversell, duplicate charge, orphan charge and drift detection | Built and tested | `internal/reconcile` tests |
| Drift repair after two consecutive scans | Built and tested | `TestDriftTrackerNeedsTwoScans`, `TestRepairFixesRedisAfterRechecking` |
| Load shedding with separate pools for reservations, order routes and webhooks | Built and tested | `TestLoadSheddingRejectsFastWhenFull` |
| Reservation kill switch | Built and tested | `TestPauseKillSwitch` |
| Alert rules | All seven pass promtool's syntax check in CI. Five have firing tests: oversell, duplicate charge, charged without order, stuck payments and drift. The latency and burn-rate rules have none. No alert has been delivered end to end. | `deploy/prometheus/alerts_test.yml` |
| Autoscaling signal exported and consumed | Metric built; KEDA config written but not applied to a cluster; replica decisions simulated from real measurements | `flashsale_http_inflight_requests`, `deploy/k8s/keda-scaledobject.yaml`, `docs/results/laptop-spike.json` |
| Spike behaviour and capacity inputs | Measured on a laptop against embedded Redis, then extrapolated | `docs/CAPACITY.md`, `docs/results/laptop-spike.json`, `docs/results/laptop-spike-shedding.json` |
| Durable store in Postgres | Design: schema provided; the prototype's in-memory store mirrors its transactions | `db/schema.sql`, `internal/store` |
| Kafka pipeline, CDN, WAF, bot protection, waiting room | Design only | `docs/PROPOSAL.md` |
| Provider circuit breaker, retry budgets, shared provider rate limit | Design only | `docs/PROPOSAL.md` sections 3.4 and 3.5 |
| Multi-region failover and fencing | Design only | `docs/PROPOSAL.md` section 3.8, `docs/adr/0008-single-home-region-fencing.md` |
| Real payment provider integration | Design only; tests use a fake provider with realistic failure modes | `internal/psp` |
| 500,000 requests/sec and 10M users | Extrapolated only | `docs/CAPACITY.md` |
