from typing import Any
from etl.db import connect
from etl.run_context import RunContext


def record_run(run: RunContext, status: str, input_count: int | None = None, output_count: int | None = None, error: str | None = None, publication_status: str | None = None) -> None:
    with connect("warehouse_db") as conn:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO ops.pipeline_runs
                (platform_run_id, dag_id, airflow_run_id, parent_dag_id, parent_run_id, input_platform_run_id, status, input_count, output_count, error_summary, publication_status, ended_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, CASE WHEN %s IN ('success','failed') THEN now() ELSE NULL END)
                ON CONFLICT (platform_run_id, dag_id) DO UPDATE SET airflow_run_id=EXCLUDED.airflow_run_id,
                status=EXCLUDED.status, input_count=EXCLUDED.input_count, output_count=EXCLUDED.output_count,
                error_summary=EXCLUDED.error_summary, publication_status=EXCLUDED.publication_status,
                ended_at=EXCLUDED.ended_at""", (run.platform_run_id, run.dag_id, run.airflow_run_id, run.parent_dag_id, run.parent_run_id, run.input_platform_run_id, status, input_count, output_count, error, publication_status, status))


def require_base_manifest(platform_run_id: str) -> dict[str, Any]:
    with connect("warehouse_db") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT platform_run_id, base_dag_id, base_airflow_run_id, base_status, base_customer_count, base_order_count FROM ops.platform_manifests WHERE platform_run_id = %s", (platform_run_id,))
            row = cur.fetchone()
    if not row or row[3] != "success":
        raise ValueError(f"UPSTREAM_MANIFEST_FAILED: no successful base manifest for {platform_run_id}")
    return {"platform_run_id": row[0], "base_dag_id": row[1], "base_airflow_run_id": row[2], "customer_count": row[4], "order_count": row[5]}
