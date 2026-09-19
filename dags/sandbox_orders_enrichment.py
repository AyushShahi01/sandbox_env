from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from etl.layers import enrich_orders
from etl.manifests import record_run
from etl.run_context import RunContext, conf_mode
from etl.dag_helpers import trigger


def task(**context):
    run = RunContext.from_airflow(context)
    enrich_orders(run, conf_mode(context, "orders_enrichment"))

with DAG("sandbox_orders_enrichment", start_date=datetime(2024, 1, 1), schedule=None, catchup=False, max_active_runs=1, is_paused_upon_creation=False, default_args={"retries": 0}, tags=["sandbox", "layer2"]) as dag:
    enrich = PythonOperator(task_id="enrich_orders", python_callable=task)
    daily = trigger(
        "trigger_daily_revenue", "sandbox_daily_revenue",
        {"platform_run_id": "{{ dag_run.conf.get('platform_run_id') }}", "parent_dag_id": "{{ dag.dag_id }}", "parent_run_id": "{{ dag_run.run_id }}", "failure_modes": "{{ dag_run.conf.get('failure_modes', {}) }}"},
        "daily__{{ dag_run.conf.get('platform_run_id') }}",
    )
    enrich >> daily
