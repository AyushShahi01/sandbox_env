CREATE SCHEMA IF NOT EXISTS staging;

CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE
    IF NOT EXISTS staging.customers (
        batch_id TEXT NOT NULL,
        customer_id TEXT NOT NULL,
        email TEXT NOT NULL,
        country TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (batch_id, customer_id)
    );

CREATE TABLE
    IF NOT EXISTS staging.orders (
        batch_id TEXT NOT NULL,
        order_id TEXT NOT NULL,
        customer_id TEXT NOT NULL,
        order_timestamp TIMESTAMPTZ NOT NULL,
        quantity INTEGER NOT NULL,
        unit_price NUMERIC(12, 2) NOT NULL,
        order_total NUMERIC(14, 2) NOT NULL,
        status TEXT NOT NULL,
        PRIMARY KEY (batch_id, order_id)
    );

CREATE TABLE
    IF NOT EXISTS warehouse.dim_customers (
        customer_id TEXT PRIMARY KEY,
        email TEXT NOT NULL,
        country TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        loaded_at TIMESTAMPTZ NOT NULL DEFAULT now ()
    );

CREATE TABLE
    IF NOT EXISTS warehouse.fact_orders (
        order_id TEXT PRIMARY KEY,
        customer_id TEXT NOT NULL REFERENCES warehouse.dim_customers (customer_id),
        order_timestamp TIMESTAMPTZ NOT NULL,
        quantity INTEGER NOT NULL,
        unit_price NUMERIC(12, 2) NOT NULL,
        order_total NUMERIC(14, 2) NOT NULL,
        status TEXT NOT NULL,
        loaded_at TIMESTAMPTZ NOT NULL DEFAULT now ()
    );

CREATE TABLE
    IF NOT EXISTS ops.etl_runs (
        batch_id TEXT PRIMARY KEY,
        dag_run_id TEXT NOT NULL,
        status TEXT NOT NULL,
        source_customers INTEGER,
        source_orders INTEGER,
        published_customers INTEGER,
        published_orders INTEGER,
        total_revenue NUMERIC(16, 2),
        error_message TEXT,
        recorded_at TIMESTAMPTZ NOT NULL DEFAULT now ()
    );