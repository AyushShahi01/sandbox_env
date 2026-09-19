from pathlib import Path


def test_phase2_dags_exist():
    dag_dir = Path(__file__).parents[1] / "dags"
    expected = {"sandbox_platform_orchestrator.py", "sandbox_orders_enrichment.py", "sandbox_daily_revenue.py", "sandbox_customer_metrics.py", "sandbox_analytics_publish.py"}
    assert expected.issubset({path.name for path in dag_dir.iterdir()})
