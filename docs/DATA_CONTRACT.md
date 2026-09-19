# Data contract

The seed defaults are 100 customers and 1,000 orders, generated with seed `42` and a fixed base timestamp of `2024-01-01T00:00:00Z`. IDs are stable (`CUST-00001`, `ORD-0000001`) and all data is synthetic.

Source tables use text business keys, UTC timestamps, positive integer quantities, `NUMERIC(12,2)` prices, and statuses `placed`, `paid`, or `cancelled`. Customer email and country are normalized during transformation; business keys are trimmed but not otherwise changed.

Warehouse staging is batch-scoped by `batch_id`. Customer rows are unique on `(batch_id, customer_id)` and order rows on `(batch_id, order_id)`. `order_total` is `quantity * unit_price`, rounded half-up to two decimal places using `Decimal` and PostgreSQL `NUMERIC`.

The published dimension contains every valid source customer. The published fact contains every valid source order, including cancelled orders. Cancelled orders remain queryable, but revenue verification excludes them. Future order timestamps, missing or duplicate keys, unknown customers, invalid statuses, non-positive quantities, negative prices, and arithmetic mismatches are critical validation failures.
