from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from etl.layers import build_customer_metrics
from etl.run_context import RunContext, conf_mode


def task(**context):
    run = RunContext.from_airflow(context)
    build_customer_metrics(run, conf_mode(context, "customer_metrics"))

with DAG("sandbox_customer_metrics", start_date=datetime(2024, 1, 1), schedule=None, catchup=False, max_active_runs=1, is_paused_upon_creation=False, default_args={"retries": 0}, tags=["sandbox", "layer2"] ) as dag:
    build = PythonOperator(task_id="build_customer_metrics", python_callable=task)
