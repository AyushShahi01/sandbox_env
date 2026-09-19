from datetime import timedelta
import time

from airflow.exceptions import AirflowException
from airflow.models.dagrun import DagRun
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState


class ReconcileTriggerDagRunOperator(TriggerDagRunOperator):
    """Trigger once, then wait for an existing deterministic child run on retry."""

    def execute(self, context):
        target_run_id = self.trigger_run_id
        deadline = time.monotonic() + self.execution_timeout.total_seconds()
        while True:
            with create_session() as session:
                existing = session.query(DagRun).filter(
                    DagRun.dag_id == self.trigger_dag_id,
                    DagRun.run_id == target_run_id,
                ).one_or_none()
                state = existing.state if existing else None
            if state == DagRunState.SUCCESS:
                return
            if state == DagRunState.FAILED:
                raise AirflowException(f"Child DAG {self.trigger_dag_id} run {target_run_id} failed")
            if existing is None:
                return super().execute(context)
            if time.monotonic() >= deadline:
                raise AirflowException(f"Timed out waiting for child DAG {self.trigger_dag_id} run {target_run_id}")
            time.sleep(self.poke_interval)


def run_conf(context, failure_modes: dict[str, str] | None = None) -> dict[str, object]:
    conf = context["dag_run"].conf or {}
    platform_id = conf.get("platform_run_id", context["dag_run"].run_id)
    modes = dict(conf.get("failure_modes") or {})
    if failure_modes:
        modes.update(failure_modes)
    return {"platform_run_id": platform_id, "parent_dag_id": context["dag"].dag_id, "parent_run_id": context["dag_run"].run_id, "failure_modes": modes}


def trigger(task_id: str, target_dag: str, conf_template: str, run_id_template: str) -> TriggerDagRunOperator:
    return ReconcileTriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=target_dag,
        conf=conf_template,
        trigger_run_id=run_id_template,
        allowed_states=["success"],
        failed_states=["failed"],
        poke_interval=5,
        execution_timeout=timedelta(minutes=30),
        reset_dag_run=False,
        wait_for_completion=True,
    )
