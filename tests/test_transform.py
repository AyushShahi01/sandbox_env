from decimal import Decimal

from etl.transform import money, transform_batch


def test_money_uses_decimal_rounding():
    assert money("10.005") == Decimal("10.01")


def test_transform_normalizes_and_calculates_total():
    batch = transform_batch({"customers": [{"customer_id": " C1 ", "email": "A@EXAMPLE.TEST ", "country": "us", "created_at": "2024-01-01T00:00:00+00:00"}], "orders": [{"order_id": " O1 ", "customer_id": "C1", "order_timestamp": "2024-01-02T00:00:00+00:00", "quantity": 2, "unit_price": "4.995", "status": "PAID"}]})
    assert batch["customers"][0]["email"] == "a@example.test"
    assert batch["customers"][0]["country"] == "US"
    assert batch["orders"][0]["order_total"] == "10.00"
    assert batch["orders"][0]["status"] == "paid"
