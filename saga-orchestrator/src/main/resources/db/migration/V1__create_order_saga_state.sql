CREATE TABLE order_saga_state (
    order_id        UUID            PRIMARY KEY,
    customer_id     UUID            NOT NULL,
    item_id         UUID            NOT NULL,
    quantity        INT             NOT NULL,
    total_amount    NUMERIC(12, 2)  NOT NULL,
    state           VARCHAR(40)     NOT NULL,
    failure_reason  TEXT,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT now()
);
