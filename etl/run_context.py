from dataclasses import dataclass
import re
from typing import Any


def safe_id(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.+-]", "_", value)


def platform_id_from_context(context: dict[str, Any]) -> str:
    conf = context.get("dag_run").conf or {}
    value = conf.get("platform_run_id")
    if value:
        return safe_id(str(value))
    return safe_id(str(context["dag_run"].run_id))


def conf_mode(context: dict[str, Any], namespace: str | None = None) -> str:
    conf = context.get("dag_run").conf or {}
    if namespace:
        modes = conf.get("failure_modes") or {}
        if isinstance(modes, str):
            import ast
            try:
                modes = ast.literal_eval(modes)
            except (SyntaxError, ValueError):
                modes = {}
        value = modes.get(namespace, "none")
    else:
        value = conf.get("failure_mode", "none")
    return str(value)


@dataclass(frozen=True)
class RunContext:
    platform_run_id: str
    dag_id: str
    airflow_run_id: str
    parent_dag_id: str | None = None
    parent_run_id: str | None = None
    input_platform_run_id: str | None = None

    @classmethod
    def from_airflow(cls, context: dict[str, Any]) -> "RunContext":
        conf = context["dag_run"].conf or {}
        return cls(
            platform_run_id=platform_id_from_context(context),
            dag_id=context["dag"].dag_id,
            airflow_run_id=context["dag_run"].run_id,
            parent_dag_id=conf.get("parent_dag_id"),
            parent_run_id=conf.get("parent_run_id"),
            input_platform_run_id=conf.get("input_platform_run_id") or conf.get("platform_run_id"),
        )
