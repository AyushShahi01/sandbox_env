CREATE SCHEMA IF NOT EXISTS source;

CREATE TABLE
    IF NOT EXISTS source.customers (
        customer_id TEXT PRIMARY KEY,
        email TEXT NOT NULL UNIQUE,
        country TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL
    );

CREATE TABLE
    IF NOT EXISTS source.orders (
        order_id TEXT PRIMARY KEY,
        customer_id TEXT NOT NULL REFERENCES source.customers (customer_id),
        order_timestamp TIMESTAMPTZ NOT NULL,
        quantity INTEGER NOT NULL CHECK (quantity > 0),
        unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
        status TEXT NOT NULL CHECK (status IN ('placed', 'paid', 'cancelled'))
    );