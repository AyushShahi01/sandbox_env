\connect postgres

SELECT 'CREATE DATABASE airflow_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow_db')\gexec
SELECT 'CREATE DATABASE source_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'source_db')\gexec
SELECT 'CREATE DATABASE warehouse_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'warehouse_db')\gexec
