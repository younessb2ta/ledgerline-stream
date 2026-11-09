-- Ledgerline streaming schema (Postgres 16).
-- Every write from Flink is an idempotent upsert; the 2PC alternative is
-- documented in docs/adr (0003). Apply with `make db-init`.

CREATE TABLE IF NOT EXISTS ledger_events (
    event_id          text PRIMARY KEY,
    event_type        text NOT NULL,
    event_ts         timestamptz NOT NULL,
    order_id          text NOT NULL,
    customer_id       text NOT NULL,
    product_id        text,
    quantity          integer NOT NULL,
    unit_price_cents  bigint NOT NULL,
    amount_cents      bigint NOT NULL,
    currency          text NOT NULL,
    schema_version    integer NOT NULL,
    ingested_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ledger_events_customer_idx ON ledger_events (customer_id);
CREATE INDEX IF NOT EXISTS ledger_events_order_idx ON ledger_events (order_id);
CREATE INDEX IF NOT EXISTS ledger_events_ts_idx ON ledger_events (event_ts);

-- Running balance per customer: order_paid adds, refund_issued subtracts.
-- last_event_ts makes replayed (older) updates harmless.
CREATE TABLE IF NOT EXISTS customer_balances (
    customer_id    text PRIMARY KEY,
    balance_cents  bigint NOT NULL,
    last_event_ts  timestamptz NOT NULL,
    updated_at     timestamptz NOT NULL DEFAULT now()
);

-- 1-minute tumbling net revenue (paid - refunded), event time.
CREATE TABLE IF NOT EXISTS revenue_windows (
    window_start   timestamptz PRIMARY KEY,
    window_end     timestamptz NOT NULL,
    revenue_cents  bigint NOT NULL,
    event_count    bigint NOT NULL,
    updated_at     timestamptz NOT NULL DEFAULT now()
);

-- Written from CheckpointListener.notifyCheckpointComplete.
-- One snapshot per (checkpoint, customer); replays cannot duplicate it.
CREATE TABLE IF NOT EXISTS balance_snapshots (
    checkpoint_id  bigint NOT NULL,
    customer_id    text NOT NULL,
    balance_cents  bigint NOT NULL,
    snapshot_ts    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (checkpoint_id, customer_id)
);

-- Events that arrived after the watermark + allowed lateness. They are kept
-- for restatement, never silently dropped (ADR-0005).
CREATE TABLE IF NOT EXISTS late_events (
    event_id          text PRIMARY KEY,
    event_ts         timestamptz NOT NULL,
    arrival_ts       timestamptz NOT NULL,
    lateness_seconds bigint NOT NULL,
    payload          jsonb NOT NULL
);

-- Bounded replay results, compared with revenue_windows.
CREATE TABLE IF NOT EXISTS revenue_windows_restated (
    window_start   timestamptz PRIMARY KEY,
    window_end     timestamptz NOT NULL,
    revenue_cents  bigint NOT NULL,
    event_count    bigint NOT NULL,
    restated_at    timestamptz NOT NULL DEFAULT now()
);

-- Operational markers written by the demo scripts (checkpoint counts, kills).
CREATE TABLE IF NOT EXISTS pipeline_events (
    id          bigserial PRIMARY KEY,
    kind        text NOT NULL,
    detail      jsonb,
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- Every INSERT attempt, including the ones ON CONFLICT DO NOTHING skips.
-- A BEFORE INSERT trigger fires before conflict detection, so this audit
-- table makes replayed writes visible after a failover: one row per event,
-- `attempts > 1` means the job tried the same event more than once and the
-- primary key absorbed it. Without it, an idempotent sink looks identical to
-- a sink that never replayed anything.
CREATE TABLE IF NOT EXISTS insert_attempts (
    event_id  text PRIMARY KEY,
    attempts  bigint NOT NULL DEFAULT 0
);

CREATE OR REPLACE FUNCTION count_insert_attempt() RETURNS trigger AS $$
BEGIN
    INSERT INTO insert_attempts (event_id, attempts)
    VALUES (NEW.event_id, 1)
    ON CONFLICT (event_id) DO UPDATE
        SET attempts = insert_attempts.attempts + 1;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS ledger_events_attempt ON ledger_events;
CREATE TRIGGER ledger_events_attempt
    BEFORE INSERT ON ledger_events
    FOR EACH ROW EXECUTE FUNCTION count_insert_attempt();
