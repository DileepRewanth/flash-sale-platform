# ADR 0003: Waiting room with approximate FIFO

**Status:** accepted, design only

**Context.** Up to 10M users wait for 50K units. Strict global FIFO needs a single ordering authority, which becomes the next hot spot at 333K joins/sec.

**Decision.** Stateless signed tickets carry arrival time plus random jitter inside a one-second bucket. Early arrivals before a scheduled opening are randomised across a window. A controller advances one CDN-cached watermark at the rate downstream capacity and the payment provider allow, and also caps admission tokens per second.

**Alternatives.**
- *Strict FIFO with a central counter.* Fair to the millisecond, but a single point of contention and failure.
- *Lottery for everyone.* Simple and bot-resistant, but unpopular for walk-up traffic after opening.
- *No waiting room, autoscaling only.* Cannot react within the 30-second ramp.

**Consequences.** Ordering can invert within a bucket and across issuer clock skew. The assignment allows queue ordering to degrade, so this trade is acceptable.
