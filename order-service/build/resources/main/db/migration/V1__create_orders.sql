CREATE TABLE orders (
    id          UUID            PRIMARY KEY,
    customer_id UUID            NOT NULL,
    item_id     UUID            NOT NULL,
    quantity    INT             NOT NULL CHECK (quantity > 0),
    total_price NUMERIC(12, 2)  NOT NULL CHECK (total_price >= 0),
    status      VARCHAR(20)     NOT NULL,
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ     NOT NULL DEFAULT now()
);
