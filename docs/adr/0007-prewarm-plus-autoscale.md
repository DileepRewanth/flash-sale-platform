# ADR 0007: Pre-warm for scheduled drops, autoscale on a leading signal

**Status:** accepted; metric built, KEDA config written, cluster design only

**Context.** Traffic grows about 16,600 requests/sec every second for 30 seconds. Node provisioning and CPU-based scaling take minutes.

**Decision.** A cron trigger pre-warms replicas before scheduled drops. Autoscaling uses in-flight requests per replica, a leading signal, with fast scale-up and a slow scale-down. The waiting room covers unscheduled spikes by admitting only what current capacity can serve.

**Alternatives.**
- *Over-provision permanently for peak.* Lowest risk, highest cost.
- *CPU-based autoscaling only.* Cheap, but it reacts after queues have formed.

**Consequences.** Idle capacity before drops costs money, which is the price of not depending on scaling speed for correctness.
