from etl.db import connect


def summarize(batch_id: str) -> dict[str, object]:
    with connect("warehouse_db") as conn:
        with conn.cursor() as cur:
            cur.execute("""SELECT (SELECT count(*) FROM staging.customers WHERE batch_id = %s),
                (SELECT count(*) FROM staging.orders WHERE batch_id = %s),
                (SELECT count(*) FROM warehouse.dim_customers),
                (SELECT count(*) FROM warehouse.fact_orders),
                COALESCE((SELECT sum(order_total) FROM warehouse.fact_orders WHERE status <> 'cancelled'), 0)""", (batch_id, batch_id))
            row = cur.fetchone()
    return {"staged_customers": row[0], "staged_orders": row[1], "published_customers": row[2], "published_orders": row[3], "non_cancelled_revenue": str(row[4])}
