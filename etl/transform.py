from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from etl.config import STATUSES


def money(value: Any) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def transform_batch(raw: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    customers = []
    for row in raw["customers"]:
        customers.append({
            "customer_id": str(row["customer_id"]).strip(),
            "email": str(row["email"]).strip().lower(),
            "country": str(row["country"]).strip().upper(),
            "created_at": str(row["created_at"]),
        })
    orders = []
    for row in raw["orders"]:
        quantity = int(row["quantity"])
        unit_price = money(row["unit_price"])
        orders.append({
            "order_id": str(row["order_id"]).strip(),
            "customer_id": str(row["customer_id"]).strip(),
            "order_timestamp": str(row["order_timestamp"]),
            "quantity": quantity,
            "unit_price": str(unit_price),
            "order_total": str(money(quantity * unit_price)),
            "status": str(row["status"]).strip().lower(),
        })
    return {"customers": customers, "orders": orders}


def inject_invalid_data(batch: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    if not batch["orders"]:
        raise ValueError("Cannot inject invalid data into an empty order batch")
    batch["orders"][0]["quantity"] = -1
    return batch


def validate_batch(batch: dict[str, list[dict[str, Any]]], now: datetime | None = None) -> list[str]:
    errors: list[str] = []
    now = now or datetime.now(timezone.utc)
    customers = batch["customers"]
    orders = batch["orders"]
    customer_ids = [r.get("customer_id") for r in customers]
    if any(not value for value in customer_ids):
        errors.append("customers contain missing customer_id")
    if len(customer_ids) != len(set(customer_ids)):
        errors.append("duplicate customer_id values")
    order_ids = [r.get("order_id") for r in orders]
    if any(not value for value in order_ids):
        errors.append("orders contain missing order_id")
    if len(order_ids) != len(set(order_ids)):
        errors.append("duplicate order_id values")
    known = set(customer_ids)
    for row in orders:
        if row.get("customer_id") not in known:
            errors.append(f"unknown customer: {row.get('customer_id')}")
        if row.get("status") not in STATUSES:
            errors.append(f"invalid status: {row.get('status')}")
        if int(row.get("quantity", 0)) <= 0:
            errors.append(f"non-positive quantity: {row.get('order_id')}")
        price = money(row.get("unit_price", "-1"))
        if price < 0:
            errors.append(f"negative unit_price: {row.get('order_id')}")
        timestamp = datetime.fromisoformat(str(row["order_timestamp"]).replace("Z", "+00:00"))
        if timestamp > now:
            errors.append(f"future order_timestamp: {row.get('order_id')}")
        expected = money(int(row["quantity"]) * price)
        if money(row.get("order_total")) != expected:
            errors.append(f"order_total mismatch: {row.get('order_id')}")
    return errors
