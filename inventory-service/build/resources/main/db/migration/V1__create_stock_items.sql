CREATE TABLE stock_items (
    id          UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    item_id     UUID          NOT NULL UNIQUE,
    quantity    INT           NOT NULL CHECK (quantity >= 0),
    updated_at  TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- Seed a few items so the happy-path test has stock to reserve
INSERT INTO stock_items (item_id, quantity) VALUES
    ('aaaaaaaa-0000-0000-0000-000000000001', 100),
    ('aaaaaaaa-0000-0000-0000-000000000002', 50);
