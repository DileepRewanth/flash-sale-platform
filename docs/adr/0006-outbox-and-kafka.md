# ADR 0006: Transactional outbox, relayed to Kafka partitioned by order ID

**Status:** accepted; outbox built in memory, Kafka design only

**Context.** A state change and the work it triggers must never diverge, and the pipeline must survive consumer crashes.

**Decision.** Work items are written in the same transaction as the state change. A relay publishes them to Kafka, partitioned by order ID. Consumers are idempotent because delivery is at least once.

**Alternatives.**
- *Dual writes to the database and the queue.* Simpler, but a crash between the two loses or duplicates work.
- *Partition by item ID.* Keeps item events together, but one hot item becomes one hot partition.
- *SQS or RabbitMQ.* Fine for queues, but weaker replay and per-key ordering than a log.

**Consequences.** Ordering is guaranteed only per order, which is the only ordering the state machine needs.
