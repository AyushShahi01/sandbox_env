from etl.db import connect


def validate_staged_batch(batch_id: str, expected_customers: int, expected_orders: int) -> None:
    errors: list[str] = []
    with connect("warehouse_db") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM staging.customers WHERE batch_id = %s", (batch_id,))
            customers = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM staging.orders WHERE batch_id = %s", (batch_id,))
            orders = cur.fetchone()[0]
            if customers != expected_customers:
                errors.append(f"customer row count {customers} != expected {expected_customers}")
            if orders != expected_orders:
                errors.append(f"order row count {orders} != expected {expected_orders}")
            cur.execute("""SELECT count(*) FROM staging.orders o
                LEFT JOIN staging.customers c ON c.batch_id = o.batch_id AND c.customer_id = o.customer_id
                WHERE o.batch_id = %s AND c.customer_id IS NULL""", (batch_id,))
            unknown = cur.fetchone()[0]
            if unknown:
                errors.append(f"unknown customer references: {unknown}")
            cur.execute("SELECT count(*) FROM staging.orders WHERE batch_id = %s AND (quantity <= 0 OR unit_price < 0 OR status NOT IN ('placed','paid','cancelled'))", (batch_id,))
            invalid = cur.fetchone()[0]
            if invalid:
                errors.append(f"invalid order rows: {invalid}")
            cur.execute("SELECT count(*) FROM staging.orders WHERE batch_id = %s AND order_total <> round(quantity * unit_price, 2)", (batch_id,))
            mismatches = cur.fetchone()[0]
            if mismatches:
                errors.append(f"arithmetic mismatches: {mismatches}")
    if errors:
        raise ValueError("BATCH_VALIDATION_FAILED: " + "; ".join(errors))
