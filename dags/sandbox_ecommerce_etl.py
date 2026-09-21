from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

from etl.config import ALLOWED_FAILURE_MODES
from etl.extract import extract_source
from etl.publish import publish_batch
from etl.reporting import summarize
from etl.stage import read_batch_file, stage_batch, write_batch_file
from etl.transform import inject_invalid_data, transform_batch, validate_batch
from etl.validate import validate_staged_batch
from etl.run_context import platform_id_from_context

DATA_DIR = Path("/opt/airflow/data")


def mode(context) -> str:
    value = (context["dag_run"].conf or {}).get("failure_mode", "none")
    if value not in ALLOWED_FAILURE_MODES:
        raise ValueError(f"Unknown failure_mode {value!r}; allowed values: {sorted(ALLOWED_FAILURE_MODES)}")
    return value


def batch_id(context) -> str:
    return context["dag_run"].run_id.replace("/", "_")


def check_source_task(**context):
    from etl.db import connect
    with connect("source_db") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('source.customers'), to_regclass('source.orders')")
            tables = cur.fetchone()
    if not all(tables):
        raise RuntimeError("SOURCE_CHECK_FAILED: source.customers and source.orders are required")


def extract_task(**context):
    if mode(context) == "connection_timeout":
        marker = DATA_DIR / f"{batch_id(context)}-connection-timeout.marker"
        if not marker.exists():
            marker.touch()
            raise RuntimeError("SANDBOX_INJECTED_FAILURE: CONNECTION_TIMEOUT while opening PostgreSQL connection")
    if mode(context) == "extract_error":
        raise RuntimeError("SANDBOX_INJECTED_FAILURE: extract_error before extraction")
    path = DATA_DIR / f"{batch_id(context)}-raw.json"
    write_batch_file(extract_source(), str(path))
    context["ti"].xcom_push(key="raw_path", value=str(path))


def transform_task(**context):
    if mode(context) == "transform_error":
        raise RuntimeError("SANDBOX_INJECTED_FAILURE: transform_error during transformation")
    raw = read_batch_file(context["ti"].xcom_pull(task_ids="extract", key="raw_path"))
    batch = transform_batch(raw)
    if mode(context) == "invalid_data":
        inject_invalid_data(batch)
    path = DATA_DIR / f"{batch_id(context)}-transformed.json"
    write_batch_file(batch, str(path))
    context["ti"].xcom_push(key="batch_path", value=str(path))
    context["ti"].xcom_push(key="customer_count", value=len(batch["customers"]))
    context["ti"].xcom_push(key="order_count", value=len(batch["orders"]))


def stage_task(**context):
    batch = read_batch_file(context["ti"].xcom_pull(task_ids="transform", key="batch_path"))
    stage_batch(batch_id(context), batch)


def validate_task(**context):
    validate_staged_batch(
        batch_id(context),
        context["ti"].xcom_pull(task_ids="transform", key="customer_count"),
        context["ti"].xcom_pull(task_ids="transform", key="order_count"),
    )


def publish_task(**context):
    publish_batch(batch_id(context), context["dag_run"].run_id, mode(context) == "publish_error", platform_id_from_context(context))


def summarize_task(**context):
    result = summarize(batch_id(context))
    print(f"ETL_SUMMARY batch_id={batch_id(context)} result={result}")

with DAG(
    dag_id="sandbox_ecommerce_etl",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["sandbox", "etl"],
) as dag:
    check_source = PythonOperator(task_id="check_source", python_callable=check_source_task)
    extract = PythonOperator(task_id="extract", python_callable=extract_task)
    transform = PythonOperator(task_id="transform", python_callable=transform_task)
    stage = PythonOperator(task_id="stage", python_callable=stage_task)
    validate = PythonOperator(task_id="validate", python_callable=validate_task)
    publish = PythonOperator(task_id="publish", python_callable=publish_task)
    summarize_op = PythonOperator(task_id="summarize", python_callable=summarize_task)
    check_source >> extract >> transform >> stage >> validate >> publish >> summarize_op
