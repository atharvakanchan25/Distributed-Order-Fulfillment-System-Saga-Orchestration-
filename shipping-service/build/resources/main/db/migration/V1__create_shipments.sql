CREATE TABLE shipments (
    id          UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id    UUID          NOT NULL UNIQUE,
    customer_id UUID          NOT NULL,
    item_id     UUID          NOT NULL,
    quantity    INT           NOT NULL,
    status      VARCHAR(20)   NOT NULL,
    created_at  TIMESTAMPTZ   NOT NULL DEFAULT now()
);
