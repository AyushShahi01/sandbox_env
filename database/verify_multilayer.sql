SELECT
    platform_run_id,
    base_status,
    analytics_status,
    base_customer_count,
    base_order_count
FROM
    ops.platform_manifests
ORDER BY
    created_at DESC;

SELECT
    platform_run_id,
    count(*) AS orders,
    sum(order_total) FILTER (
        WHERE
            status <> 'cancelled'
    ) AS revenue
FROM
    layer1.orders_snapshot
GROUP BY
    platform_run_id
ORDER BY
    platform_run_id;

SELECT
    platform_run_id,
    sum(revenue_total) AS daily_revenue
FROM
    layer2.daily_revenue
GROUP BY
    platform_run_id;

SELECT
    platform_run_id,
    sum(revenue_total) AS customer_revenue
FROM
    layer2.customer_metrics
GROUP BY
    platform_run_id;

SELECT
    platform_run_id,
    count(*) AS enriched_orders
FROM
    layer2.enriched_orders
GROUP BY
    platform_run_id;