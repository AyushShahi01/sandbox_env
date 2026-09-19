SELECT 'source_customers' AS metric, count(*)::text AS value FROM source.customers
UNION ALL SELECT 'source_orders', count(*)::text FROM source.orders
UNION ALL SELECT 'warehouse_customers', count(*)::text FROM warehouse.dim_customers
UNION ALL SELECT 'warehouse_orders', count(*)::text FROM warehouse.fact_orders;
SELECT count(*) AS duplicate_order_ids FROM warehouse.fact_orders GROUP BY order_id HAVING count(*) > 1;
SELECT COALESCE(sum(order_total) FILTER (WHERE status <> 'cancelled'), 0) AS non_cancelled_revenue FROM warehouse.fact_orders;
