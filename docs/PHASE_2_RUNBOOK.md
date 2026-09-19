# Phase 2 Runbook

Run the Phase 1 setup, apply the idempotent migration through `airflow-init`, and seed the source. `sandbox_platform_orchestrator` runs daily at `06:00 UTC`; for an immediate manual run, trigger it from the Airflow UI or with `airflow dags trigger sandbox_platform_orchestrator`. The base and child DAGs remain trigger-only. In Trigger DAG configuration, use `{}` for a normal run.

For namespaced failures use:

```json
{"failure_modes":{"daily_revenue":"daily_revenue_error"}}
{"failure_modes":{"analytics_publish":"analytics_publish_error"}}
```

Inspect the root and linked child runs in Airflow. Query `database/verify_multilayer.sql` in `warehouse_db` and filter every result by the displayed `platform_run_id`. A failed branch must leave `analytics.daily_revenue` and `analytics.customer_summary` tied to the previous successful platform ID.
