import json
from pathlib import Path
from typing import Any

from psycopg2.extras import execute_values

from etl.db import connect


def write_batch_file(batch: dict[str, list[dict[str, Any]]], path: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(batch), encoding="utf-8")


def read_batch_file(path: str) -> dict[str, list[dict[str, Any]]]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def stage_batch(batch_id: str, batch: dict[str, list[dict[str, Any]]]) -> None:
    with connect("warehouse_db") as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM staging.customers WHERE batch_id = %s", (batch_id,))
            cur.execute("DELETE FROM staging.orders WHERE batch_id = %s", (batch_id,))
            execute_values(cur, "INSERT INTO staging.customers (batch_id, customer_id, email, country, created_at) VALUES %s", [(
                batch_id, r["customer_id"], r["email"], r["country"], r["created_at"]) for r in batch["customers"]])
            execute_values(cur, "INSERT INTO staging.orders (batch_id, order_id, customer_id, order_timestamp, quantity, unit_price, order_total, status) VALUES %s", [(
                batch_id, r["order_id"], r["customer_id"], r["order_timestamp"], r["quantity"], r["unit_price"], r["order_total"], r["status"]) for r in batch["orders"]])
