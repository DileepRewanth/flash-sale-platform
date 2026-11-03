# ADR 0010: Request-based availability, reported per drop, alerted by burn rate

**Status:** accepted

**Context.** A time-based 99.95% allows 21.6 minutes of downtime a month, which could hide a total outage of a 15-minute drop. Raw traffic alerts would page on every intentional spike.

**Decision.** Availability is successful requests over valid requests on the reservation path. Sold out counts as success; shed requests are tracked as a separate rejection rate. The ratio is reported monthly and per drop, alongside selling pause time. Paging uses a multi-window burn-rate rule plus zero-tolerance correctness alerts.

**Alternatives.**
- *Time-based uptime.* Easy to explain, but blind to short, high-value outages.
- *Static thresholds on error counts.* Simple, but noisy during intentional spikes.

**Consequences.** A peak-time outage can spend the whole monthly budget by itself, and the report says so rather than averaging it away.
