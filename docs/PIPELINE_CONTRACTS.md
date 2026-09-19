# Phase 2 Pipeline Contracts

## Run identity

The root DAG uses its Airflow run ID as `platform_run_id`. Every triggered DAG receives that ID in `dag_run.conf`; every Layer 1 and Layer 2 query uses it explicitly. Standalone base runs use their own DAG run ID as the platform ID.

## DAG contract

- `sandbox_platform_orchestrator` triggers and waits for the base DAG, then two independent branches, then final publication.
- `sandbox_orders_enrichment` must complete before it triggers `sandbox_daily_revenue`.
- `sandbox_customer_metrics` is independent of the orders branch.
- `sandbox_analytics_publish` runs only after both root branches succeed.
- Child run IDs are deterministic (`base__...`, `orders__...`, `daily__...`, `customers__...`, `analytics__...`).

## Revenue contract

Cancelled orders remain in Layer 1 and enriched outputs but are excluded from every revenue and eligible-order aggregate. Dates are UTC dates derived from `order_timestamp`. Daily revenue and customer metrics must equal the Layer 1 non-cancelled total before analytics publication.

## Failure contract

Failure modes are namespaced under `failure_modes`: `orders_enrichment`, `daily_revenue`, `customer_metrics`, and `analytics_publish`. Failed child DAGs make their waiting parent fail. Final analytics tables are replaced only in one transaction after reconciliation; a publication exception rolls back both tables and the manifest update.
