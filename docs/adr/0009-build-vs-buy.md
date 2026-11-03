# ADR 0009: Build versus buy

**Status:** accepted

| Component | Choice | Reason |
|---|---|---|
| CDN, WAF, bot protection | Buy | Global capacity and bot intelligence cannot be built in time |
| Waiting room front end | Buy for v0, consider building later | A vendor product protects the current system within days |
| Waiting room pacing logic | Build | It must read our own capacity and provider signals |
| Kafka | Buy as a managed service | Operating Kafka is not a differentiator |
| Inventory gate and payment state machine | Build | They encode the correctness rules that failed last time |
| Payment provider | Buy | Regulated and outside our core |

**Consequences.** Vendor cost and lock-in at the edge, in exchange for time to market and global capacity.
