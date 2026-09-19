import pytest


@pytest.mark.integration
def test_multilayer_docker_flow_requires_running_stack():
    pytest.skip("Docker-backed Phase 2 integration test: execute with the running Airflow/PostgreSQL stack")
