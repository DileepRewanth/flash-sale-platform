# ADR 0002: Per-unit identity, split into buckets for the hot item

**Status:** accepted

**Context.** The brief mixes interchangeable stock, which suits a counter, with seats, which need identity. One hot item must not become one hot Redis key.

**Decision.** Every unit has an ID and lives in one of N Redis sets. A unit's bucket is a hash of its ID, so no mapping is stored. Users start at a hashed bucket and probe the others before hearing sold out.

**Alternatives.**
- *A counter with `DECR`.* Simple, but cannot tell seats apart or prove which unit someone owns.
- *One set per item.* Correct, but one hot key caps throughput at one Redis core.
- *Pre-assigned per-user buckets with no probing.* Faster, but reports sold out while other buckets still have stock.

**Consequences.** Throughput scales with buckets spread over shards. Near sell-out, probing costs more operations, which the capacity plan measures.
