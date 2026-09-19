from etl.run_context import safe_id


def test_safe_id_is_stable_and_shell_safe():
    assert safe_id("manual__2026-01-01T00:00:00+00:00") == "manual__2026-01-01T00_00_00+00_00"
