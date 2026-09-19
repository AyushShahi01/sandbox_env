from etl.db import connect


def publish_batch(batch_id: str, dag_run_id: str, inject_failure: bool = False, platform_run_id: str | None = None) -> None:
    platform_run_id = platform_run_id or batch_id
    with connect("warehouse_db") as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE warehouse.fact_orders, warehouse.dim_customers")
            cur.execute("""INSERT INTO warehouse.dim_customers (customer_id, email, country, created_at)
                SELECT customer_id, email, country, created_at FROM staging.customers WHERE batch_id = %s""", (batch_id,))
            cur.execute("""INSERT INTO warehouse.fact_orders (order_id, customer_id, order_timestamp, quantity, unit_price, order_total, status)
                SELECT order_id, customer_id, order_timestamp, quantity, unit_price, order_total, status
                FROM staging.orders WHERE batch_id = %s""", (batch_id,))
            cur.execute("DELETE FROM layer1.orders_snapshot WHERE platform_run_id = %s", (platform_run_id,))
            cur.execute("DELETE FROM layer1.customers_snapshot WHERE platform_run_id = %s", (platform_run_id,))
            cur.execute("""INSERT INTO layer1.customers_snapshot (platform_run_id, customer_id, email, country, created_at)
                SELECT %s, customer_id, email, country, created_at FROM staging.customers WHERE batch_id = %s""", (platform_run_id, batch_id))
            cur.execute("""INSERT INTO layer1.orders_snapshot (platform_run_id, order_id, customer_id, order_timestamp, quantity, unit_price, order_total, status)
                SELECT %s, order_id, customer_id, order_timestamp, quantity, unit_price, order_total, status
                FROM staging.orders WHERE batch_id = %s""", (platform_run_id, batch_id))
            if inject_failure:
                raise RuntimeError("SANDBOX_INJECTED_FAILURE: publish_error before commit")
            cur.execute("""INSERT INTO ops.etl_runs (batch_id, dag_run_id, status, source_customers, source_orders,
                published_customers, published_orders, total_revenue)
                SELECT %s, %s, 'success',
                    (SELECT count(*) FROM staging.customers WHERE batch_id = %s),
                    (SELECT count(*) FROM staging.orders WHERE batch_id = %s),
                    (SELECT count(*) FROM warehouse.dim_customers),
                    (SELECT count(*) FROM warehouse.fact_orders),
                    COALESCE((SELECT sum(order_total) FROM warehouse.fact_orders WHERE status <> 'cancelled'), 0)
                ON CONFLICT (batch_id) DO UPDATE SET
                    dag_run_id = EXCLUDED.dag_run_id,
                    status = EXCLUDED.status,
                    source_customers = EXCLUDED.source_customers,
                    source_orders = EXCLUDED.source_orders,
                    published_customers = EXCLUDED.published_customers,
                    published_orders = EXCLUDED.published_orders,
                    total_revenue = EXCLUDED.total_revenue,
                    recorded_at = now()""",
                (batch_id, dag_run_id, batch_id, batch_id))
            cur.execute("""INSERT INTO ops.platform_manifests
                (platform_run_id, base_dag_id, base_airflow_run_id, base_status, base_customer_count, base_order_count, updated_at)
                VALUES (%s, 'sandbox_ecommerce_etl', %s, 'success',
                    (SELECT count(*) FROM layer1.customers_snapshot WHERE platform_run_id = %s),
                    (SELECT count(*) FROM layer1.orders_snapshot WHERE platform_run_id = %s), now())
                ON CONFLICT (platform_run_id) DO UPDATE SET base_airflow_run_id=EXCLUDED.base_airflow_run_id,
                base_status='success', base_customer_count=EXCLUDED.base_customer_count,
                base_order_count=EXCLUDED.base_order_count, updated_at=now()""", (platform_run_id, dag_run_id, platform_run_id, platform_run_id))
