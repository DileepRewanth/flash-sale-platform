# ADR 0004: Synchronous reservation, asynchronous payment confirmation

**Status:** accepted

**Context.** The provider is rate limited and sometimes slow. Holding a client connection, and a database connection, across its call is what exhausted connections in the last drop.

**Decision.** The hold and the durable claim are synchronous and fast. Order creation returns `202`; a worker calls the provider, and the client polls the order.

**Alternatives.**
- *Synchronous charge inside the request.* Simpler client, but latency and connections track the provider's worst case.
- *Fully asynchronous reservation.* Even more absorbent, but users would not know whether they hold a unit.

**Consequences.** Confirmation takes seconds rather than milliseconds, and clients must poll. Provider outages no longer consume API capacity.
