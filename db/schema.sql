-- Target Postgres schema. DESIGN ONLY: the prototype's in-memory store
-- (internal/store) mirrors these constraints and transactions, but this file is
-- not executed by the prototype.

CREATE TABLE inventory_units (
    sale_id   TEXT    NOT NULL,
    unit_id   TEXT    NOT NULL,
    state     TEXT    NOT NULL CHECK (state IN ('FREE', 'RESERVED', 'SOLD')),
    order_id  TEXT,
    version   BIGINT  NOT NULL DEFAULT 0,
    PRIMARY KEY (sale_id, unit_id)              -- one row and one owner per unit
);

CREATE TABLE orders (
    order_id    TEXT PRIMARY KEY,
    sale_id     TEXT NOT NULL,
    unit_id     TEXT NOT NULL,
    user_id     TEXT NOT NULL,
    hold_id     TEXT NOT NULL UNIQUE,           -- one order per hold
    state       TEXT NOT NULL,
    pay_by      TIMESTAMPTZ NOT NULL,
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- One active order per unit, and one active order per user per sale.
CREATE UNIQUE INDEX orders_active_unit ON orders (sale_id, unit_id) WHERE state IN ('PENDING', 'CONFIRMED');
CREATE UNIQUE INDEX orders_active_user ON orders (sale_id, user_id) WHERE state IN ('PENDING', 'CONFIRMED');

CREATE TABLE payment_attempts (
    attempt_id        TEXT PRIMARY KEY,         -- also the provider idempotency key
    order_id          TEXT NOT NULL REFERENCES orders (order_id),
    user_id           TEXT NOT NULL,
    idempotency_key   TEXT NOT NULL,
    request_hash      TEXT NOT NULL,            -- same key with a different body is rejected
    provider_outcome  TEXT NOT NULL,            -- NOT_SENT, UNKNOWN, SUCCEEDED, FAILED, CANCELLED
    disposition       TEXT NOT NULL,            -- NONE, RETAINED, REFUND_PENDING, REFUNDED
    provider_endpoint TEXT,                     -- pinned: never re-sent through another provider region
    first_dispatch_at TIMESTAMPTZ,
    sends             INT  NOT NULL DEFAULT 0,
    UNIQUE (user_id, idempotency_key)
);
-- One in-flight attempt per order, and at most one kept charge per order.
CREATE UNIQUE INDEX attempts_in_flight ON payment_attempts (order_id) WHERE provider_outcome IN ('NOT_SENT', 'UNKNOWN');
CREATE UNIQUE INDEX attempts_retained  ON payment_attempts (order_id) WHERE disposition = 'RETAINED';

CREATE TABLE webhook_events (
    provider   TEXT NOT NULL,
    event_id   TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (provider, event_id)
);

CREATE TABLE refunds (
    attempt_id TEXT PRIMARY KEY REFERENCES payment_attempts (attempt_id), -- one refund per attempt
    state      TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE outbox (
    id          BIGSERIAL PRIMARY KEY,
    topic       TEXT NOT NULL,
    key         TEXT NOT NULL,                  -- order ID, the Kafka partition key
    payload     JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_until TIMESTAMPTZ,
    processed_at TIMESTAMPTZ
);

-- Multi-region fence: renewed only after the quorum-backed lease is renewed.
CREATE TABLE sale_fences (
    sale_id          TEXT PRIMARY KEY,
    epoch            BIGINT NOT NULL,
    lease_expires_at TIMESTAMPTZ NOT NULL
);

-- Step 2, the guarded claim. Runs inside the order-creation transaction.
-- $1 order, $2 sale, $3 unit, $4 epoch from the hold token, $5 hold token expiry.
-- UPDATE inventory_units
--    SET state = 'RESERVED', order_id = $1, version = version + 1
--  WHERE sale_id = $2 AND unit_id = $3 AND state = 'FREE'
--    AND clock_timestamp() < $5
--    AND EXISTS (SELECT 1 FROM sale_fences
--                 WHERE sale_id = $2 AND epoch = $4
--                   AND lease_expires_at > clock_timestamp());
-- Zero rows updated means the unit is taken, the hold expired or the region is fenced.

-- Bound every ownership transaction, so a stalled one cannot commit after a failover.
-- Requires Postgres 17 or later.
-- ALTER ROLE flashsale_app SET transaction_timeout = '5s';
