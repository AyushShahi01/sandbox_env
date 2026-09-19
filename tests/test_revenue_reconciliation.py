from decimal import Decimal


def test_revenue_definition_excludes_cancelled():
    rows = [(Decimal("10.00"), "paid"), (Decimal("4.00"), "cancelled")]
    assert sum(total for total, status in rows if status != "cancelled") == Decimal("10.00")
