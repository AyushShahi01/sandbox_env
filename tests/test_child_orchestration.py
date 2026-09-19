from pathlib import Path


def test_parent_child_contract_is_declared():
    content = (Path(__file__).parents[1] / "dags" / "sandbox_orders_enrichment.py").read_text(encoding="utf-8")
    helper = (Path(__file__).parents[1] / "etl" / "dag_helpers.py").read_text(encoding="utf-8")
    assert "sandbox_daily_revenue" in content
    assert "wait_for_completion=True" in helper
