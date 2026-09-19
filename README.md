# Standalone ETL Sandbox

A deliberately small, Dockerized e-commerce ETL pipeline. It generates synthetic source data, loads a PostgreSQL warehouse through a single Airflow DAG, validates each candidate batch, and publishes snapshots atomically. It contains no AI, agent, dashboard, Kafka, Airbyte, dbt, ticketing, or messaging component.

## Versions and design

- Apache Airflow `2.10.5` on Python `3.12`
- PostgreSQL `16.4`
- Airflow `LocalExecutor`, one PostgreSQL service with `airflow_db`, `source_db`, and `warehouse_db`
- Host ports: PostgreSQL `5433`, Airflow `8080`
- Default seed: 100 customers, 1,000 orders, deterministic seed `42`
- DAG: `sandbox_ecommerce_etl`, manual schedule, no task retries by default
- Large records move through a shared Docker volume as bounded JSON files; XCom carries only paths and counts

## Prerequisites

Run these in PowerShell from this directory:

```powershell
docker --version
docker compose version
docker info
Copy-Item .env.example .env
```

Edit `.env` and replace both local passwords. Keep `.env` private. If ports are occupied, change `POSTGRES_PORT` or `AIRFLOW_PORT`.

## Start and initialize

```powershell
docker compose config --quiet
docker compose build
docker compose up -d postgres
docker compose run --rm airflow-init
docker compose up -d airflow-webserver airflow-scheduler
docker compose ps
```

The PostgreSQL initialization scripts run only when the PostgreSQL volume is empty. `airflow-init` applies schemas idempotently and runs metadata migration. Open <http://localhost:8080> and log in with the values in `.env` (defaults are `admin` and `change-me-airflow-only`).

## Seed and run

```powershell
docker compose exec airflow-scheduler python /opt/airflow/scripts/seed_source.py --customers 100 --orders 1000 --seed 42
docker compose exec airflow-scheduler bash -lc 'PGPASSWORD="$ETL_DB_PASSWORD" psql -h "$ETL_DB_HOST" -U "$ETL_DB_USER" -d "$ETL_SOURCE_DB" -c "SELECT count(*) AS customers FROM source.customers; SELECT count(*) AS orders FROM source.orders;"'
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow dags trigger sandbox_ecommerce_etl
docker compose exec airflow-scheduler airflow dags list-runs -d sandbox_ecommerce_etl
```

Airflow CLI output can be inspected with `docker compose logs airflow-scheduler`. In the UI, open the DAG grid and wait for `check_source`, `extract`, `transform`, `stage`, `validate`, `publish`, and `summarize` to succeed.

## Verify the warehouse

```powershell
docker compose exec airflow-scheduler bash -lc 'PGPASSWORD="$ETL_DB_PASSWORD" psql -h "$ETL_DB_HOST" -U "$ETL_DB_USER" -d "$ETL_WAREHOUSE_DB" -f /opt/airflow/database/verification_queries.sql'
docker compose exec airflow-scheduler bash -lc 'PGPASSWORD="$ETL_DB_PASSWORD" psql -h "$ETL_DB_HOST" -U "$ETL_DB_USER" -d "$ETL_WAREHOUSE_DB" -c "SELECT count(*) AS duplicate_order_ids FROM warehouse.fact_orders GROUP BY order_id HAVING count(*) > 1; SELECT COALESCE(sum(order_total) FILTER (WHERE status <> ''cancelled''), 0) AS non_cancelled_revenue FROM warehouse.fact_orders;"'
```

Expected output types are integer counts, zero duplicate groups, and a numeric revenue value. Exact revenue is data-derived and is intentionally not hardcoded here. Trigger the same DAG again and confirm counts and revenue remain unchanged.

## Failure demonstrations

Use the Airflow UI's **Trigger DAG > Configuration** and enter one of these JSON objects:

```powershell
{"failure_mode":"extract_error"}
{"failure_mode":"invalid_data"}
{"failure_mode":"publish_error"}
```

`extract_error` fails before extraction. `invalid_data` reaches staging but fails validation before publication. `publish_error` raises inside the publication transaction and PostgreSQL rolls back. Before and after each failure, rerun the warehouse verification queries and confirm the published counts and numeric revenue are unchanged. Logs contain an explicit `SANDBOX_INJECTED_FAILURE` marker for operational injections.

## Tests

Pure tests require only Python dependencies:

```powershell
docker compose run --rm airflow-scheduler pytest -q /opt/airflow/tests
```

The included Docker-backed integration placeholder is skipped unless explicitly implemented against the running databases. To verify the DAG in its real environment:

```powershell
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler python -c "from dags.sandbox_ecommerce_etl import dag; print(dag.dag_id)"
```

## Stop and retain data

```powershell
docker compose down
docker compose up -d postgres
docker compose run --rm airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```

This retains database and log volumes. A destructive reset of this sandbox only may remove its project-specific Docker volume after stopping the stack; do that deliberately, never as routine troubleshooting.

See [docs/DATA_CONTRACT.md](docs/DATA_CONTRACT.md), [docs/FAILURE_SCENARIOS.md](docs/FAILURE_SCENARIOS.md), and [docs/RUNBOOK.md](docs/RUNBOOK.md) for contracts and recovery guidance.

## Phase 2 multilayer pipeline

Phase 2 adds immutable `layer1` snapshots, independent `layer2` branches, a nested daily-revenue child DAG, and transactional `analytics` publication. Apply the additive migration through `airflow-init` as shown in the startup commands, then trigger the root DAG:

```powershell
docker compose exec airflow-scheduler airflow dags trigger sandbox_platform_orchestrator
```

The root DAG runs `sandbox_ecommerce_etl`, then branches to `sandbox_orders_enrichment` and `sandbox_customer_metrics`. The orders branch triggers and waits for `sandbox_daily_revenue`. Only after both branches succeed does it trigger `sandbox_analytics_publish`.

The root orchestrator is scheduled daily at `06:00 UTC`. The base and downstream DAGs remain trigger-only because the root controls their platform ID and dependency order. `catchup=False` prevents historical runs from being created automatically.

Use [docs/PIPELINE_CONTRACTS.md](docs/PIPELINE_CONTRACTS.md), [docs/MULTILAYER_ARCHITECTURE.md](docs/MULTILAYER_ARCHITECTURE.md), [docs/PHASE_2_RUNBOOK.md](docs/PHASE_2_RUNBOOK.md), and `database/verify_multilayer.sql` to inspect platform-specific lineage and reconciliation.
