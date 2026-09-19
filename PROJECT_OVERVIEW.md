# Standalone ETL Sandbox Project Overview

## 1. Project Summary

This project is a small, self-contained e-commerce ETL sandbox. It generates deterministic synthetic customer and order data, stores that data in a PostgreSQL source database, transforms and validates it with Python, and publishes a clean snapshot to a separate PostgreSQL warehouse database.

Apache Airflow orchestrates the pipeline through one DAG named `sandbox_ecommerce_etl`.

The project is designed for local development and operational testing. It demonstrates:

- Docker-based reproducible setup
- PostgreSQL source and warehouse separation
- Deterministic synthetic data generation
- Batch-scoped staging
- Validation before publication
- Decimal-safe monetary calculations
- Idempotent reruns
- Atomic warehouse publication
- Controlled operational and data-quality failures
- Airflow task-level observability

This project does not implement AI agents, LangChain, LangGraph, Ollama, dashboards, Kafka, Airbyte, dbt, Kubernetes, Slack, Teams, ticketing, or remediation automation.

## 2. Technology Stack

| Component               | Version or choice                           |
| ----------------------- | ------------------------------------------- |
| Operating system target | Windows 10/11 with PowerShell               |
| Container runtime       | Docker Desktop with Linux containers / WSL2 |
| Orchestration           | Apache Airflow 2.10.5                       |
| Airflow Python runtime  | Python 3.12                                 |
| Database                | PostgreSQL 16.4                             |
| Airflow executor        | LocalExecutor                               |
| ETL language            | Python                                      |
| Database driver         | `psycopg2-binary` 2.9.9                     |
| Test framework          | `pytest` 8.3.4                              |
| Host Airflow port       | `8080`                                      |
| Host PostgreSQL port    | `5433`                                      |

One PostgreSQL container hosts three logical databases:

- `airflow_db`: Airflow metadata only
- `source_db`: synthetic source data
- `warehouse_db`: staging, published warehouse tables, and ETL audit data

The ETL code never uses the Airflow metadata database as a source or warehouse.

## 3. Architecture

```text
Synthetic seed script
        |
        v
PostgreSQL: source_db
  source.customers
  source.orders
        |
        v
Airflow DAG: sandbox_ecommerce_etl
  check_source
       |
     extract
       |
    transform
       |
      stage
       |
    validate
       |
     publish
       |
   summarize
        |
        v
PostgreSQL: warehouse_db
  staging.customers
  staging.orders
  warehouse.dim_customers
  warehouse.fact_orders
  ops.etl_runs
```

Large datasets are not sent through Airflow XCom. Extracted and transformed batches are written to the shared Docker volume mounted at `/opt/airflow/data`. XCom contains only file paths and small row-count metadata.

## 4. Repository Structure

```text
sandbox_env/
├── README.md
├── PROJECT_OVERVIEW.md
├── projectplan.md
├── .env.example
├── .gitignore
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── dags/
│   └── sandbox_ecommerce_etl.py
├── etl/
│   ├── __init__.py
│   ├── config.py
│   ├── db.py
│   ├── extract.py
│   ├── transform.py
│   ├── stage.py
│   ├── validate.py
│   ├── publish.py
│   └── reporting.py
├── database/
│   ├── init/
│   │   └── 01_create_databases.sql
│   ├── source_schema.sql
│   ├── warehouse_schema.sql
│   └── verification_queries.sql
├── scripts/
│   └── seed_source.py
├── tests/
│   ├── test_transform.py
│   ├── test_validation.py
│   ├── test_idempotency.py
│   └── test_dag_import.py
└── docs/
    ├── DATA_CONTRACT.md
    ├── FAILURE_SCENARIOS.md
    └── RUNBOOK.md
```

## 5. Synthetic Dataset

The dataset is generated locally. No third-party or real customer data is downloaded.

Default seed command:

```powershell
docker compose exec airflow-scheduler python /opt/airflow/scripts/seed_source.py --customers 100 --orders 1000 --seed 42
```

Defaults:

- 100 customers
- 1,000 orders
- Random seed `42`
- Fixed base timestamp `2024-01-01T00:00:00Z`
- Customer IDs such as `CUST-00001`
- Order IDs such as `ORD-0000001`
- Synthetic emails using `example.test`
- Countries from a fixed documented list
- Statuses: `placed`, `paid`, and `cancelled`

The seed script resets only the sandbox source tables and inserts a deterministic dataset. It does not touch warehouse publication tables.

## 6. Database Model

### Source database

Database: `source_db`

Schema: `source`

### `source.customers`

| Column        | Type          | Rules               |
| ------------- | ------------- | ------------------- |
| `customer_id` | `TEXT`        | Primary key         |
| `email`       | `TEXT`        | Required and unique |
| `country`     | `TEXT`        | Required            |
| `created_at`  | `TIMESTAMPTZ` | Required            |

### `source.orders`

| Column            | Type            | Rules                             |
| ----------------- | --------------- | --------------------------------- |
| `order_id`        | `TEXT`          | Primary key                       |
| `customer_id`     | `TEXT`          | Foreign key to `source.customers` |
| `order_timestamp` | `TIMESTAMPTZ`   | Required                          |
| `quantity`        | `INTEGER`       | Must be greater than zero         |
| `unit_price`      | `NUMERIC(12,2)` | Must be non-negative              |
| `status`          | `TEXT`          | `placed`, `paid`, or `cancelled`  |

### Warehouse database

Database: `warehouse_db`

Schemas:

- `staging`: candidate batch rows before publication
- `warehouse`: published customer and order snapshot
- `ops`: lightweight ETL audit information

Staging tables are keyed by `(batch_id, business_key)`. Published tables have one row per customer or order business key.

### `warehouse.dim_customers`

Contains normalized customer attributes:

- `customer_id`
- `email`
- `country`
- `created_at`
- `loaded_at`

### `warehouse.fact_orders`

Contains transformed order facts:

- `order_id`
- `customer_id`
- `order_timestamp`
- `quantity`
- `unit_price`
- `order_total`
- `status`
- `loaded_at`

`order_total` is calculated as:

```text
order_total = quantity * unit_price
```

Money is calculated with Python `Decimal` and stored in PostgreSQL `NUMERIC` columns. Cancelled orders remain in the fact table, but revenue verification excludes cancelled orders.

## 7. ETL Processing Flow

### `check_source`

Checks connectivity to `source_db` and confirms that `source.customers` and `source.orders` exist. Missing tables or connection failures stop the DAG with an actionable error.

### `extract`

Reads source customers and orders in a bounded database operation and writes a raw batch file to the shared ETL volume.

### `transform`

Normalizes email, country, identifiers, and statuses. It calculates `order_total` using decimal-safe arithmetic and writes the transformed batch file.

### `stage`

Deletes any previous staging rows for the current batch ID and inserts the transformed customers and orders into `warehouse_db` staging tables. Retrying the same task does not create duplicate staging rows.

### `validate`

Validates only the current staged batch. Critical checks include:

- Expected customer and order counts
- Missing customer IDs
- Duplicate customer IDs
- Missing order IDs
- Duplicate order IDs
- Unknown customer references
- Invalid statuses
- Non-positive quantities
- Negative prices
- Future timestamps
- Incorrect order totals

Validation errors stop downstream publication.

### `publish`

Publishes the validated snapshot inside one PostgreSQL transaction:

1. Clear the current published fact and dimension rows.
2. Insert the staged customer snapshot.
3. Insert the staged order snapshot.
4. Write an audit record.
5. Commit only after every operation succeeds.

Any exception before commit rolls back the entire publication transaction.

### `summarize`

Logs staged counts, published counts, batch ID, and non-cancelled revenue.

## 8. Idempotency and Safety

The pipeline uses a stable batch ID derived from the Airflow run identity.

Safety properties:

- Staging writes replace only the current batch.
- Published fact orders have a primary key on `order_id`.
- Published customers have a primary key on `customer_id`.
- Repeating a run produces stable row counts and totals.
- Validation runs before publication.
- Invalid candidate batches do not replace the previous published snapshot.
- Publication is transactional.
- Airflow has `max_active_runs=1`.
- Automatic task retries are disabled by default so injected failures remain visible.

## 9. Failure Demonstrations

Fault injection is disabled by default. The Airflow UI can trigger the DAG with these configurations:

```json
{ "failure_mode": "none" }
```

Normal successful run.

```json
{ "failure_mode": "extract_error" }
```

Raises `SANDBOX_INJECTED_FAILURE` before extraction. No publication occurs.

```json
{ "failure_mode": "transform_error" }
```

Raises `SANDBOX_INJECTED_FAILURE` during transformation. No publication occurs.

```json
{ "failure_mode": "invalid_data" }
```

Modifies the candidate batch with invalid data. The batch reaches staging, validation fails, and previously published tables remain unchanged.

```json
{ "failure_mode": "publish_error" }
```

Raises `SANDBOX_INJECTED_FAILURE` inside the publication transaction. PostgreSQL rolls back the publication changes.

Unknown failure modes are rejected.

## 10. Setup and Operation

From PowerShell:

```powershell
Copy-Item .env.example .env
docker compose config --quiet
docker compose build
docker compose up -d postgres
docker compose run --rm airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```

Airflow is available at:

```text
http://localhost:8080
```

Default credentials are:

```text
Username: admin
Password: change-me-airflow-only
```

Use the values in the private `.env` file if they were changed.

After startup:

```powershell
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow dags trigger sandbox_ecommerce_etl
```

The DAG can also be triggered from the Airflow UI.

## 11. Verification

The verification SQL file is:

```text
database/verification_queries.sql
```

It checks:

- Source customer count
- Source order count
- Warehouse customer count
- Warehouse order count
- Duplicate order IDs
- Non-cancelled revenue

For the default seed, expected row-count types are:

- Source customers: integer count, normally `100`
- Source orders: integer count, normally `1000`
- Published customers: integer count, normally `100`
- Published orders: integer count, normally `1000`
- Duplicate order groups: no rows / zero duplicates
- Revenue: numeric value calculated from the generated data

The exact revenue should be read from the database rather than hardcoded in documentation.

## 12. Testing

Run the local Python checks with:

```powershell
python -m pytest -q tests
python -m compileall -q etl dags scripts tests
```

The current unit suite covers:

- Decimal-safe money calculations
- String normalization
- Order-total calculations
- Invalid quantities
- Unknown customers
- Duplicate order IDs
- DAG file presence
- Airflow DAG import when Airflow is installed

Docker-backed tests should be run explicitly in the Airflow image. Tests that require a live database are not treated as successful when Docker is unavailable.

## 13. Volumes and Persistence

Docker volumes:

- `postgres-data`: PostgreSQL databases
- `airflow-logs`: Airflow task logs
- `etl-data`: intermediate raw and transformed batch files

PostgreSQL initialization scripts run automatically only for an empty PostgreSQL data volume. The Airflow initialization container applies the database schemas idempotently on subsequent starts.

Normal shutdown retains data:

```powershell
docker compose down
```

A volume reset is destructive and should be reserved for deliberately resetting this local sandbox.

## 14. Current Scope and Limitations

Included:

- Local Docker deployment
- Synthetic e-commerce source data
- PostgreSQL ETL pipeline
- Airflow orchestration
- Validation and transaction safety
- Failure injection
- Unit tests
- Windows PowerShell documentation

Not included:

- Production authentication
- High availability
- Cloud deployment
- Streaming ingestion
- External data sources
- AI monitoring or remediation
- Notifications and ticketing
- Dashboards
- Kubernetes

This sandbox is ready to be monitored by a separate future project, but it does not contain an AI or agent component itself.

## 15. Phase 2 Multilayer Extension

Phase 2 adds a durable, run-keyed data platform without replacing the Phase 1 DAG or tables:

```text
sandbox_platform_orchestrator
     |
  sandbox_ecommerce_etl
     |
     layer1 snapshots
  /             \
orders enrichment   customer metrics
  |
daily revenue child
  \             /
    analytics publish
     |
  analytics.*
```

The new DAGs are:

- `sandbox_platform_orchestrator`: root fan-out/fan-in orchestration
- `sandbox_orders_enrichment`: Layer 2 order enrichment and child parent
- `sandbox_daily_revenue`: child aggregation of non-cancelled orders
- `sandbox_customer_metrics`: independent customer aggregation branch
- `sandbox_analytics_publish`: cross-branch reconciliation and atomic final publication

The additive migration is `database/migrations/02_multilayer.sql`. It creates `layer1`, `layer2`, and `analytics` schemas, immutable run-keyed snapshots, `ops.pipeline_runs`, and `ops.platform_manifests`. Every downstream query is keyed by the exact `platform_run_id` and requires a successful base manifest. Final analytics publication uses one transaction and a PostgreSQL advisory lock, so a failed publication leaves the previous analytics snapshot intact.

The multilayer contracts and runbook are documented in [docs/PIPELINE_CONTRACTS.md](docs/PIPELINE_CONTRACTS.md), [docs/MULTILAYER_ARCHITECTURE.md](docs/MULTILAYER_ARCHITECTURE.md), and [docs/PHASE_2_RUNBOOK.md](docs/PHASE_2_RUNBOOK.md).

## 15. Validation Status

The implementation has been checked with:

- Docker Compose configuration validation
- Airflow image build
- PostgreSQL startup and health checks
- Airflow metadata initialization
- Source and warehouse schema initialization
- DAG import and registration
- Successful end-to-end DAG execution
- Python compilation
- Unit tests: `6 passed, 2 skipped`

Phase 2 verification additionally observed:

- Six DAGs registered with no import errors.
- Root run `manual__2026-09-19T09:36:22+00:00` completed successfully.
- Base, orders enrichment, customer metrics, daily revenue child, and analytics publication all completed successfully.
- Layer 1 contained 1,000 orders and non-cancelled revenue of `109224.24`.
- Layer 2 daily revenue and customer metrics both reconciled to `109224.24`.
- Final analytics publication contained 220 daily rows and 100 customer rows.
- Injected analytics publication failure rolled back and preserved the successful manifest and analytics rows.
- Current Phase 2 unit/contract tests: `10 passed, 3 skipped`.

The skipped tests are Docker-backed integration placeholders; the live Docker root flow and core multilayer database flow were executed separately as documented above.

The Airflow UI remains the recommended interface for triggering and observing configured fault-injection scenarios.
