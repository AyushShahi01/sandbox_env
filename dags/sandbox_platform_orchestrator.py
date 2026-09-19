from datetime import datetime

from airflow import DAG
from etl.dag_helpers import trigger


CONF = {
    "platform_run_id": "{{ dag_run.run_id }}",
    "parent_dag_id": "{{ dag.dag_id }}",
    "parent_run_id": "{{ dag_run.run_id }}",
    "failure_mode": "{{ dag_run.conf.get('failure_mode', 'none') }}",
    "failure_modes": "{{ dag_run.conf.get('failure_modes', {}) }}",
}

with DAG(
    "sandbox_platform_orchestrator",
    start_date=datetime(2024, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args={"retries": 0},
    tags=["sandbox", "root"],
) as dag:
    base = trigger("trigger_base", "sandbox_ecommerce_etl", CONF, "base__{{ dag_run.run_id }}")
    orders = trigger("trigger_orders_enrichment", "sandbox_orders_enrichment", CONF, "orders__{{ dag_run.run_id }}")
    customers = trigger("trigger_customer_metrics", "sandbox_customer_metrics", CONF, "customers__{{ dag_run.run_id }}")
    final = trigger("trigger_analytics_publish", "sandbox_analytics_publish", CONF, "analytics__{{ dag_run.run_id }}")
    base >> [orders, customers] >> final
