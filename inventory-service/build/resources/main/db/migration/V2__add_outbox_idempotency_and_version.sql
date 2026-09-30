CREATE TABLE outbox_events (
    id            UUID        PRIMARY KEY,
    aggregate_id  TEXT        NOT NULL,
    topic         TEXT        NOT NULL,
    payload_type  TEXT        NOT NULL,
    payload       TEXT        NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ
);
CREATE INDEX idx_outbox_unpublished ON outbox_events (published_at) WHERE published_at IS NULL;

CREATE TABLE processed_events (
    event_id      UUID        PRIMARY KEY,
    handler_name  TEXT        NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Optimistic locking: version column on stock_items
ALTER TABLE stock_items ADD COLUMN version BIGINT NOT NULL DEFAULT 0;
