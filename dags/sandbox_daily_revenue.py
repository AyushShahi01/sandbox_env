from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from etl.layers import build_daily_revenue
from etl.run_context import RunContext, conf_mode


def task(**context):
    run = RunContext.from_airflow(context)
    build_daily_revenue(run, conf_mode(context, "daily_revenue"))

with DAG("sandbox_daily_revenue", start_date=datetime(2024, 1, 1), schedule=None, catchup=False, max_active_runs=1, is_paused_upon_creation=False, default_args={"retries": 0}, tags=["sandbox", "layer2", "child"]) as dag:
    build = PythonOperator(task_id="build_daily_revenue", python_callable=task)
