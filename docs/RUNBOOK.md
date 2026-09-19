# Runbook

Use `docker compose ps` and `docker compose logs airflow-scheduler` first. A missing database schema usually means the PostgreSQL volume predates this project; inspect the volume before considering a deliberate local reset. Do not use `docker compose down -v` as routine troubleshooting because it deletes all local data.

For a clean sandbox-only reset, stop the stack, remove the project volume shown by `docker volume ls`, then start again. This is destructive only to this local sandbox.

After every successful run, save the warehouse counts and non-cancelled revenue. Trigger `invalid_data` and `publish_error` and confirm those values are unchanged. Logs include `SANDBOX_INJECTED_FAILURE` for intentionally injected operational failures.
