from etl.db import connect
from etl.manifests import require_base_manifest, record_run
from etl.run_context import RunContext


def _check_mode(mode: str, expected: str) -> None:
    if mode == expected:
        raise RuntimeError(f"SANDBOX_INJECTED_FAILURE: {expected}")


def enrich_orders(run: RunContext, failure_mode: str = "none") -> None:
    manifest = require_base_manifest(run.platform_run_id)
    _check_mode(failure_mode, "enrichment_error")
    with connect("warehouse_db") as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM layer2.enriched_orders WHERE platform_run_id = %s", (run.platform_run_id,))
            cur.execute("""INSERT INTO layer2.enriched_orders
                (platform_run_id, order_id, customer_id, country, order_timestamp, order_date, quantity, unit_price, order_total, status)
                SELECT o.platform_run_id, o.order_id, o.customer_id, c.country, o.order_timestamp,
                    (o.order_timestamp AT TIME ZONE 'UTC')::date, o.quantity, o.unit_price, o.order_total, o.status
                FROM layer1.orders_snapshot o
                LEFT JOIN layer1.customers_snapshot c ON c.platform_run_id=o.platform_run_id AND c.customer_id=o.customer_id
                WHERE o.platform_run_id=%s""", (run.platform_run_id,))
            cur.execute("SELECT count(*) FROM layer2.enriched_orders WHERE platform_run_id=%s", (run.platform_run_id,))
            count = cur.fetchone()[0]
            if count != manifest["order_count"]:
                raise ValueError(f"ENRICHMENT_VALIDATION_FAILED: output {count} != input {manifest['order_count']}")
            cur.execute("SELECT count(*) FROM layer2.enriched_orders WHERE platform_run_id=%s AND country IS NULL", (run.platform_run_id,))
            if cur.fetchone()[0]:
                raise ValueError("ENRICHMENT_VALIDATION_FAILED: missing customer lookup")
    record_run(run, "success", manifest["order_count"], count, publication_status="ready")


def build_daily_revenue(run: RunContext, failure_mode: str = "none") -> None:
    _check_mode(failure_mode, "daily_revenue_error")
    with connect("warehouse_db") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status, count(*) FROM layer2.enriched_orders WHERE platform_run_id=%s GROUP BY status", (run.platform_run_id,))
            if not cur.fetchall():
                raise ValueError("DAILY_REVENUE_INPUT_FAILED: enriched orders are absent")
            cur.execute("DELETE FROM layer2.daily_revenue WHERE platform_run_id=%s", (run.platform_run_id,))
            cur.execute("""INSERT INTO layer2.daily_revenue (platform_run_id, order_date, order_count, revenue_total)
                SELECT platform_run_id, order_date, count(*)::integer, sum(order_total)
                FROM layer2.enriched_orders WHERE platform_run_id=%s AND status <> 'cancelled'
                GROUP BY platform_run_id, order_date""", (run.platform_run_id,))
            if failure_mode == "daily_revenue_validation_error":
                raise ValueError("SANDBOX_INJECTED_FAILURE: daily_revenue_validation_error")
            cur.execute("SELECT count(*) FROM layer2.daily_revenue WHERE platform_run_id=%s", (run.platform_run_id,))
            count = cur.fetchone()[0]
    record_run(run, "success", None, count, publication_status="ready")


def build_customer_metrics(run: RunContext, failure_mode: str = "none") -> None:
    manifest = require_base_manifest(run.platform_run_id)
    _check_mode(failure_mode, "customer_metrics_error")
    with connect("warehouse_db") as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM layer2.customer_metrics WHERE platform_run_id=%s", (run.platform_run_id,))
            cur.execute("""INSERT INTO layer2.customer_metrics (platform_run_id, customer_id, eligible_order_count, revenue_total)
                SELECT c.platform_run_id, c.customer_id, count(o.order_id)::integer,
                    COALESCE(sum(o.order_total), 0)
                FROM layer1.customers_snapshot c
                LEFT JOIN layer1.orders_snapshot o ON o.platform_run_id=c.platform_run_id
                    AND o.customer_id=c.customer_id AND o.status <> 'cancelled'
                WHERE c.platform_run_id=%s GROUP BY c.platform_run_id, c.customer_id""", (run.platform_run_id,))
            cur.execute("SELECT count(*) FROM layer2.customer_metrics WHERE platform_run_id=%s", (run.platform_run_id,))
            count = cur.fetchone()[0]
            if count != manifest["customer_count"]:
                raise ValueError(f"CUSTOMER_METRICS_VALIDATION_FAILED: output {count} != input {manifest['customer_count']}")
    record_run(run, "success", manifest["customer_count"], count, publication_status="ready")


def publish_analytics(run: RunContext, failure_mode: str = "none") -> None:
    require_base_manifest(run.platform_run_id)
    with connect("warehouse_db") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM layer2.daily_revenue WHERE platform_run_id=%s", (run.platform_run_id,))
            daily_count = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM layer2.customer_metrics WHERE platform_run_id=%s", (run.platform_run_id,))
            customer_count = cur.fetchone()[0]
            if not daily_count or not customer_count:
                raise ValueError("ANALYTICS_INPUT_FAILED: both layer2 branches must be ready")
            cur.execute("""SELECT COALESCE((SELECT sum(revenue_total) FROM layer2.daily_revenue WHERE platform_run_id=%s),0),
                COALESCE((SELECT sum(revenue_total) FROM layer2.customer_metrics WHERE platform_run_id=%s),0),
                COALESCE((SELECT sum(order_total) FROM layer1.orders_snapshot WHERE platform_run_id=%s AND status <> 'cancelled'),0)""", (run.platform_run_id, run.platform_run_id, run.platform_run_id))
            totals = cur.fetchone()
            if len(set(totals)) != 1:
                raise ValueError(f"ANALYTICS_RECONCILIATION_FAILED: daily={totals[0]} customer={totals[1]} base={totals[2]}")
            cur.execute("SELECT pg_advisory_xact_lock(hashtext('sandbox_analytics_publish'))")
            cur.execute("TRUNCATE analytics.daily_revenue, analytics.customer_summary")
            cur.execute("""INSERT INTO analytics.daily_revenue (order_date, order_count, revenue_total, platform_run_id)
                SELECT order_date, order_count, revenue_total, platform_run_id FROM layer2.daily_revenue WHERE platform_run_id=%s""", (run.platform_run_id,))
            cur.execute("""INSERT INTO analytics.customer_summary (customer_id, eligible_order_count, revenue_total, platform_run_id)
                SELECT customer_id, eligible_order_count, revenue_total, platform_run_id FROM layer2.customer_metrics WHERE platform_run_id=%s""", (run.platform_run_id,))
            if failure_mode == "analytics_publish_error":
                raise RuntimeError("SANDBOX_INJECTED_FAILURE: analytics_publish_error before commit")
            cur.execute("UPDATE ops.platform_manifests SET analytics_status='success', analytics_airflow_run_id=%s, updated_at=now() WHERE platform_run_id=%s", (run.airflow_run_id, run.platform_run_id))
    record_run(run, "success", daily_count + customer_count, daily_count + customer_count, publication_status="published")
