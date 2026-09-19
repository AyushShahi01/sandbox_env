import argparse
import os
import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import psycopg2

from etl.config import connection_kwargs


def seed(customers_count: int, orders_count: int, seed_value: int) -> None:
    random.seed(seed_value)
    countries = ["US", "CA", "GB", "DE", "AU", "IN"]
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    with psycopg2.connect(**connection_kwargs(os.getenv("ETL_SOURCE_DB", "source_db"))) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE source.orders, source.customers CASCADE")
            customers = [(f"CUST-{i:05d}", f"customer{i:05d}@example.test", countries[(i - 1) % len(countries)], base + timedelta(days=i)) for i in range(1, customers_count + 1)]
            cur.executemany("INSERT INTO source.customers (customer_id, email, country, created_at) VALUES (%s,%s,%s,%s)", customers)
            orders = []
            for i in range(1, orders_count + 1):
                customer_id = customers[(i * 7) % customers_count][0]
                timestamp = base + timedelta(days=30 + (i % 330), minutes=i)
                quantity = 1 + (i % 5)
                price = Decimal("9.99") + Decimal(i % 90) + Decimal((i * 13) % 100) / Decimal("100")
                status = ("placed", "paid", "cancelled")[i % 3]
                orders.append((f"ORD-{i:07d}", customer_id, timestamp, quantity, price, status))
            cur.executemany("INSERT INTO source.orders (order_id, customer_id, order_timestamp, quantity, unit_price, status) VALUES (%s,%s,%s,%s,%s,%s)", orders)
    print(f"Seeded {customers_count} customers and {orders_count} orders with seed {seed_value}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--customers", type=int, default=100)
    parser.add_argument("--orders", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    seed(args.customers, args.orders, args.seed)
