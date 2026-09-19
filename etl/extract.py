from datetime import datetime
from decimal import Decimal
from typing import Any

from etl.db import connect


def _json_value(value: Any) -> Any:
    if isinstance(value, (datetime, Decimal)):
        return str(value)
    return value


def extract_source() -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {"customers": [], "orders": []}
    with connect("source_db") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT customer_id, email, country, created_at FROM source.customers ORDER BY customer_id")
            result["customers"] = [dict(zip([d.name for d in cur.description], map(_json_value, row))) for row in cur.fetchall()]
            cur.execute("SELECT order_id, customer_id, order_timestamp, quantity, unit_price, status FROM source.orders ORDER BY order_id")
            result["orders"] = [dict(zip([d.name for d in cur.description], map(_json_value, row))) for row in cur.fetchall()]
    return result
