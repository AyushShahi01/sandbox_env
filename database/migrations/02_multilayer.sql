CREATE SCHEMA IF NOT EXISTS layer1;

CREATE SCHEMA IF NOT EXISTS layer2;

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE
    IF NOT EXISTS layer1.customers_snapshot (
        platform_run_id TEXT NOT NULL,
        customer_id TEXT NOT NULL,
        email TEXT NOT NULL,
        country TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (platform_run_id, customer_id)
    );

CREATE TABLE
    IF NOT EXISTS layer1.orders_snapshot (
        platform_run_id TEXT NOT NULL,
        order_id TEXT NOT NULL,
        customer_id TEXT NOT NULL,
        order_timestamp TIMESTAMPTZ NOT NULL,
        quantity INTEGER NOT NULL,
        unit_price NUMERIC(12, 2) NOT NULL,
        order_total NUMERIC(14, 2) NOT NULL,
        status TEXT NOT NULL,
        PRIMARY KEY (platform_run_id, order_id),
        FOREIGN KEY (platform_run_id, customer_id) REFERENCES layer1.customers_snapshot (platform_run_id, customer_id)
    );

CREATE TABLE
    IF NOT EXISTS layer2.enriched_orders (
        platform_run_id TEXT NOT NULL,
        order_id TEXT NOT NULL,
        customer_id TEXT NOT NULL,
        country TEXT NOT NULL,
        order_timestamp TIMESTAMPTZ NOT NULL,
        order_date DATE NOT NULL,
        quantity INTEGER NOT NULL,
        unit_price NUMERIC(12, 2) NOT NULL,
        order_total NUMERIC(14, 2) NOT NULL,
        status TEXT NOT NULL,
        PRIMARY KEY (platform_run_id, order_id)
    );

CREATE TABLE
    IF NOT EXISTS layer2.daily_revenue (
        platform_run_id TEXT NOT NULL,
        order_date DATE NOT NULL,
        order_count INTEGER NOT NULL,
        revenue_total NUMERIC(16, 2) NOT NULL,
        PRIMARY KEY (platform_run_id, order_date)
    );

CREATE TABLE
    IF NOT EXISTS layer2.customer_metrics (
        platform_run_id TEXT NOT NULL,
        customer_id TEXT NOT NULL,
        eligible_order_count INTEGER NOT NULL,
        revenue_total NUMERIC(16, 2) NOT NULL,
        PRIMARY KEY (platform_run_id, customer_id),
        FOREIGN KEY (platform_run_id, customer_id) REFERENCES layer1.customers_snapshot (platform_run_id, customer_id)
    );

CREATE TABLE
    IF NOT EXISTS analytics.daily_revenue (
        order_date DATE PRIMARY KEY,
        order_count INTEGER NOT NULL,
        revenue_total NUMERIC(16, 2) NOT NULL,
        platform_run_id TEXT NOT NULL,
        published_at TIMESTAMPTZ NOT NULL DEFAULT now ()
    );

CREATE TABLE
    IF NOT EXISTS analytics.customer_summary (
        customer_id TEXT PRIMARY KEY,
        eligible_order_count INTEGER NOT NULL,
        revenue_total NUMERIC(16, 2) NOT NULL,
        platform_run_id TEXT NOT NULL,
        published_at TIMESTAMPTZ NOT NULL DEFAULT now ()
    );

CREATE TABLE
    IF NOT EXISTS ops.pipeline_runs (
        platform_run_id TEXT NOT NULL,
        dag_id TEXT NOT NULL,
        airflow_run_id TEXT NOT NULL,
        parent_dag_id TEXT,
        parent_run_id TEXT,
        input_platform_run_id TEXT,
        status TEXT NOT NULL,
        started_at TIMESTAMPTZ NOT NULL DEFAULT now (),
        ended_at TIMESTAMPTZ,
        input_count INTEGER,
        output_count INTEGER,
        error_summary TEXT,
        publication_status TEXT,
        PRIMARY KEY (platform_run_id, dag_id)
    );

CREATE TABLE
    IF NOT EXISTS ops.platform_manifests (
        platform_run_id TEXT PRIMARY KEY,
        base_dag_id TEXT NOT NULL,
        base_airflow_run_id TEXT NOT NULL,
        base_status TEXT NOT NULL,
        base_customer_count INTEGER,
        base_order_count INTEGER,
        analytics_status TEXT NOT NULL DEFAULT 'pending',
        analytics_airflow_run_id TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now (),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now ()
    );

CREATE INDEX IF NOT EXISTS ix_pipeline_runs_platform ON ops.pipeline_runs (platform_run_id);