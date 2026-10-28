
CREATE TABLE IF NOT EXISTS products (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    slug VARCHAR(100) NOT NULL UNIQUE,
    name VARCHAR(200) NOT NULL,
    price NUMERIC(12, 2) NOT NULL CHECK (price >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS flash_sales (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code VARCHAR(100) NOT NULL UNIQUE,
    product_id UUID NOT NULL REFERENCES products(id),
    starts_at TIMESTAMPTZ NOT NULL,
    ends_at TIMESTAMPTZ NOT NULL,
    total_quantity INTEGER NOT NULL CHECK (total_quantity > 0),
    reserved_quantity INTEGER NOT NULL DEFAULT 0
        CHECK (reserved_quantity >= 0),
    sold_quantity INTEGER NOT NULL DEFAULT 0
        CHECK (sold_quantity >= 0),
    status VARCHAR(20) NOT NULL DEFAULT ' ACTIVE '
        CHECK (status IN (' SCHEDULED ', ' ACTIVE ', ' ENDED ')),
    CHECK (starts_at < ends_at),
    CHECK (reserved_quantity + sold_quantity <= total_quantity)
);

CREATE TABLE IF NOT EXISTS reservations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    flash_sale_id UUID NOT NULL REFERENCES flash_sales(id),
    customer_id VARCHAR(100) NOT NULL,
    idempotency_key VARCHAR(200) NOT NULL UNIQUE,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    status VARCHAR(20) NOT NULL DEFAULT ' PENDING '
        CHECK (status IN (' PENDING ', ' CONFIRMED ', ' EXPIRED ', ' CANCELLED ')),
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_reservations_expiry
    ON reservations (expires_at)
    WHERE status = ' PENDING ';

CREATE INDEX IF NOT EXISTS idx_reservations_sale_status
    ON reservations (flash_sale_id, status);

CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    reservation_id UUID NOT NULL UNIQUE REFERENCES reservations(id),
    idempotency_key VARCHAR(200) NOT NULL UNIQUE,
    status VARCHAR(30) NOT NULL DEFAULT ' PENDING_PAYMENT '
        CHECK (status IN (
            ' PENDING_PAYMENT ', ' PAID ', ' FAILED ', ' CANCELLED '
        )),
    total_amount NUMERIC(12, 2) NOT NULL CHECK (total_amount >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO products (slug, name, price)
VALUES (' flash - smartphone ', ' Flash Sale Smartphone ', 29999.00)
ON CONFLICT (slug) DO NOTHING;

INSERT INTO flash_sales (
    code, product_id, starts_at, ends_at, total_quantity, status
)
SELECT
    ' smartphone - flash - sale ',
    id,
    NOW() - INTERVAL ' 1 day ',
    NOW() + INTERVAL ' 30 days ',
    500,
    ' ACTIVE '
FROM products
WHERE slug = ' flash - smartphone '
ON CONFLICT (code) DO NOTHING;
