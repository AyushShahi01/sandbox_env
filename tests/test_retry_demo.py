from pathlib import Path

import pytest


def test_retry_demo_contract():
    airflow = pytest.importorskip("airflow")
    del airflow

    from dags.sandbox_retry_demo import dag

    task = dag.get_task("fail_once_then_manual_success")
    assert task.retries == 0
    wait_task = dag.get_task("wait_ten_seconds")
    assert wait_task.delta.total_seconds() == 10


def test_retry_demo_fails_then_manual_rerun_succeeds():
    from etl.retry_demo import fail_once_then_manual_success

    class TaskInstance:
        try_number = 1

    with pytest.raises(RuntimeError, match="intentional first attempt failure"):
        fail_once_then_manual_success(ti=TaskInstance())

    TaskInstance.try_number = 2
    fail_once_then_manual_success(ti=TaskInstance())