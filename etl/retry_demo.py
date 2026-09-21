def fail_once_then_manual_success(**context):
    """Fail once, then succeed when the task is manually rerun."""
    if context["ti"].try_number == 1:
        raise RuntimeError("SANDBOX_INJECTED_FAILURE: intentional first attempt failure")
    print("Manual rerun succeeded")