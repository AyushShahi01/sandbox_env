import importlib.util
from pathlib import Path

import pytest


def test_dag_file_exists():
    assert Path(__file__).parents[1].joinpath("dags", "sandbox_ecommerce_etl.py").exists()


def test_dag_import():
    if importlib.util.find_spec("airflow") is None:
        pytest.skip("Airflow is available in Docker, not required on the Windows host")
    spec = importlib.util.spec_from_file_location("sandbox_ecommerce_etl", Path(__file__).parents[1] / "dags" / "sandbox_ecommerce_etl.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.dag.dag_id == "sandbox_ecommerce_etl"
