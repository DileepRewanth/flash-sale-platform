# ADR 0001: Redis is the reservation gate, the durable store is the commit point

**Status:** accepted

**Context.** Reservations must be fast at spike rates, but the last drop sold one seat twice. A single database counter becomes a hot row, and a cache used as the source of truth can lose writes on failover.

**Decision.** Holds are taken in Redis with an atomic Lua script. Ownership changes only in the durable store, through a guarded update from FREE to RESERVED made when payment starts. Holds that never reach payment cost no database write.

**Alternatives.**
- *Database only, with `SELECT ... FOR UPDATE` on a counter.* Correct, but the counter row serialises every request.
- *Database only, one row per unit with `SKIP LOCKED`.* No hot row, but every hold costs a database write, and most holds never convert during a spike.
- *Redis only.* Fastest, but asynchronous replication can lose a decrement on failover and resell a unit.

**Consequences.** Redis data loss can reject a hold but never create a second owner. Redis can be rebuilt from durable FREE units. The cost is two systems that can drift, which is why the drift detector and repair exist.
