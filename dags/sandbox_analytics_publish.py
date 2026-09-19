from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from etl.layers import publish_analytics
from etl.run_context import RunContext, conf_mode


def task(**context):
    run = RunContext.from_airflow(context)
    publish_analytics(run, conf_mode(context, "analytics_publish"))

with DAG("sandbox_analytics_publish", start_date=datetime(2024, 1, 1), schedule=None, catchup=False, max_active_runs=1, is_paused_upon_creation=False, default_args={"retries": 0}, tags=["sandbox", "analytics"]) as dag:
    publish = PythonOperator(task_id="publish_analytics", python_callable=task)
