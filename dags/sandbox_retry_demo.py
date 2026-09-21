from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor

from etl.retry_demo import fail_once_then_manual_success


with DAG(
    dag_id="sandbox_retry_demo",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["sandbox", "retry-demo"],
) as dag:
    start = EmptyOperator(task_id="start")
    wait_ten_seconds = TimeDeltaSensor(
        task_id="wait_ten_seconds",
        delta=timedelta(seconds=10),
        mode="reschedule",
    )
    flaky_step = PythonOperator(
        task_id="fail_once_then_manual_success",
        python_callable=fail_once_then_manual_success,
        retries=0,
    )
    finish = EmptyOperator(task_id="finish")

    start >> wait_ten_seconds >> flaky_step >> finish