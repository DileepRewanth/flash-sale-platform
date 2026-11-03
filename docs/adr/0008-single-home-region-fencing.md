# ADR 0008: One home region per sale, with lease-based fencing

**Status:** accepted, design only

**Context.** Inventory must never be sold by two regions at once, even during a partition.

**Decision.** Each sale has one home region. A quorum store holds the sale's epoch and lease; a local fence row lets every ownership update check the epoch and lease in the same SQL statement. Ownership transactions run under `transaction_timeout`, and promotion waits for the old lease plus a margin longer than that timeout. Each region has its own provider credential, revoked automatically once the old lease plus a margin has expired, so no paused process in the old region can still charge. Inherited unresolved attempts are never re-sent from the new region.

**Alternatives.**
- *Active-active inventory across regions.* Lower latency and higher availability, but a partition can sell one unit twice.
- *Regional stock quotas.* Each region sells its own share with no coordination; a good later step, at the cost of stranded stock in a quiet region.

**Consequences.** A region failure pauses selling for about 4.4 minutes with assumed provider timings, and is reported as a breach of that drop's availability.
