from datetime import datetime, timezone

from etl.transform import validate_batch


def valid_batch():
    return {"customers": [{"customer_id": "C1", "email": "a@example.test", "country": "US", "created_at": "2024-01-01T00:00:00+00:00"}], "orders": [{"order_id": "O1", "customer_id": "C1", "order_timestamp": "2024-01-02T00:00:00+00:00", "quantity": 2, "unit_price": "4.50", "order_total": "9.00", "status": "paid"}]}


def test_invalid_quantity_is_rejected():
    batch = valid_batch()
    batch["orders"][0]["quantity"] = 0
    assert any("non-positive quantity" in error for error in validate_batch(batch, datetime(2025, 1, 1, tzinfo=timezone.utc)))


def test_unknown_customer_is_rejected():
    batch = valid_batch()
    batch["orders"][0]["customer_id"] = "MISSING"
    assert any("unknown customer" in error for error in validate_batch(batch, datetime(2025, 1, 1, tzinfo=timezone.utc)))


def test_duplicate_order_is_rejected():
    batch = valid_batch()
    batch["orders"].append(dict(batch["orders"][0]))
    assert "duplicate order_id values" in validate_batch(batch, datetime(2025, 1, 1, tzinfo=timezone.utc))
