# ADR 0005: One payment state machine, with provider outcome separate from disposition

**Status:** accepted

**Context.** Worker responses, webhooks, expiry and reconciliation all learn about the same payment, in any order and sometimes twice. The last drop charged users without orders.

**Decision.** Every source applies results through one pure state machine under one lock. Each attempt records what the provider said and, separately, what the platform decided about the money. Unknown never means failed. Same-key re-sends stop after 5 minutes, far inside provider key retention. Late success re-acquires a unit or refunds.

**Alternatives.**
- *One status column.* Fewer fields, but "money moved" and "we keep it" blur, which is where duplicate charges hide.
- *Treat timeouts as failures and retry with a new key.* Simple, but it produces exactly the double charges seen before.
- *Event sourcing for payments.* Strong audit trail, but more machinery than this prototype needs.

**Consequences.** Units can stay reserved for up to the 30-minute hard deadline during a provider outage, which undersells rather than risks refunds.
