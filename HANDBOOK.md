# ETL Sandbox Handbook

## Purpose

This project is a local e-commerce data platform built with Airflow and PostgreSQL. It turns synthetic source data into validated warehouse and analytics tables.

It is designed to demonstrate reliable data engineering:

- Every run has a unique `platform_run_id`.
- Downstream pipelines read only the exact successful upstream run.
- Invalid data stops before publication.
- Final analytics tables update atomically.
- Failed runs do not overwrite the last successful analytics result.

No AI or agent component is included.

## How It Works

```text
source data
    |
    v
root orchestrator
    |
    v
base ETL
    |
    v
Layer 1 validated snapshots
    |
    +-------------------+
    |                   |
    v                   v
orders enrichment   customer metrics
    |
    v
 daily revenue
    |
    +-------------------+
                        v
              analytics publication
```

The root DAG runs every day at `06:00 UTC`. All child DAGs are triggered by their parent and do not run independently.

## Pipelines

### 1. `sandbox_platform_orchestrator`

**Purpose:** Controls the complete platform run.

**It does:**

1. Starts the base ETL.
2. Waits for the base ETL to succeed.
3. Starts order enrichment and customer metrics in parallel.
4. Waits for both branches.
5. Starts final analytics publication.

**Example:** A daily run creates platform ID `manual__2026-09-24T00_00_00+00_00` and passes it to every child pipeline.

### 2. `sandbox_ecommerce_etl`

**Purpose:** Builds the trusted base dataset.

**It does:**

```text
check source -> extract -> transform -> stage -> validate -> publish
```

It reads `source.customers` and `source.orders`, validates them, keeps the Phase 1 warehouse tables updated, and writes immutable Layer 1 snapshots.

**Example:** 100 customers and 1,000 orders become a validated Layer 1 snapshot for one platform run.

### 3. `sandbox_orders_enrichment`

**Purpose:** Adds customer information to each order.

It reads only the matching Layer 1 orders and customers, joins each order to its customer country, and writes `layer2.enriched_orders`.

After success, it triggers the daily revenue child pipeline.

**Example:** An order for customer `CUST-00001` receives that customer’s country, such as `US`.

### 4. `sandbox_daily_revenue`

**Purpose:** Produces daily sales totals.

It reads only enriched orders and groups non-cancelled orders by their UTC order date.

It writes `layer2.daily_revenue`.

**Example:**

```text
2024-02-15 | 3 orders | 245.80 revenue
```

### 5. `sandbox_customer_metrics`

**Purpose:** Produces customer-level sales metrics.

It reads the matching Layer 1 snapshot and calculates each customer’s non-cancelled order count and revenue. Customers with no eligible orders are retained with zero totals.

It writes `layer2.customer_metrics`.

**Example:**

```text
CUST-00001 | 4 orders | 318.50 revenue
```

### 6. `sandbox_analytics_publish`

**Purpose:** Publishes the final consumer-facing analytics snapshot.

Before publishing, it confirms that:

- Both Layer 2 branches completed.
- Daily revenue equals customer revenue.
- Both equal the Layer 1 non-cancelled revenue.

It then replaces both analytics tables in one transaction:

- `analytics.daily_revenue`
- `analytics.customer_summary`

If publication fails, both tables remain unchanged.

## Data Layers

| Layer      | Meaning                           | Main tables                                                                 |
| ---------- | --------------------------------- | --------------------------------------------------------------------------- |
| Source     | Raw synthetic data                | `source.customers`, `source.orders`                                         |
| Layer 1    | Validated, immutable run snapshot | `layer1.customers_snapshot`, `layer1.orders_snapshot`                       |
| Layer 2    | Derived branch outputs            | `layer2.enriched_orders`, `layer2.daily_revenue`, `layer2.customer_metrics` |
| Analytics  | Current published result          | `analytics.daily_revenue`, `analytics.customer_summary`                     |
| Operations | Run lineage and status            | `ops.platform_manifests`, `ops.pipeline_runs`                               |

## One Normal Run

1. Start Docker services.
2. Seed the source database.
3. Open Airflow at `http://localhost:8080`.
4. Trigger `sandbox_platform_orchestrator`, or wait for the `06:00 UTC` schedule.
5. Airflow runs the dependency chain automatically.
6. Query the final analytics tables.

For the default dataset, the expected reconciliation is:

```text
Layer 1 revenue       109224.24
Daily revenue         109224.24
Customer revenue      109224.24
```

The exact value is generated from the seed and should be verified from PostgreSQL.

## Failure Behavior

A failure in any child pipeline stops its parent and prevents final analytics publication. The previous successful analytics snapshot remains available.

Examples:

```json
{ "failure_modes": { "daily_revenue": "daily_revenue_error" } }
```

```json
{ "failure_modes": { "analytics_publish": "analytics_publish_error" } }
```

Use the Airflow UI’s **Trigger DAG > Configuration** to run these examples.

## Useful Checks

```powershell
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler airflow dags list
```

For multilayer counts and revenue reconciliation:

```powershell
docker compose exec airflow-scheduler bash -lc 'PGPASSWORD="$ETL_DB_PASSWORD" psql -h "$ETL_DB_HOST" -U "$ETL_DB_USER" -d "$ETL_WAREHOUSE_DB" -f /opt/airflow/database/verify_multilayer.sql'
```

For full setup and recovery instructions, see [README.md](README.md) and [docs/PHASE_2_RUNBOOK.md](docs/PHASE_2_RUNBOOK.md).
