# Failure scenarios

Trigger modes through the Airflow UI using **Trigger DAG > Configuration** with one of these JSON objects:

```json
{"failure_mode":"none"}
{"failure_mode":"connection_timeout"}
{"failure_mode":"extract_error"}
{"failure_mode":"transform_error"}
{"failure_mode":"invalid_data"}
{"failure_mode":"publish_error"}
```

`extract_error` and `transform_error` fail before staging. `invalid_data` injects a duplicate order and negative quantity; validation fails before publication. `publish_error` raises inside the publication transaction, so PostgreSQL rolls back the truncation and inserts. Unknown mode values are rejected. Retries are disabled by default so the intended failed task remains visible.

`connection_timeout` fails the `extract` task once with a deterministic connection-timeout marker. The task creates a per-run marker in the shared data volume, so clearing the failed task from the agentic incident system allows the same run to succeed on its next execution.
