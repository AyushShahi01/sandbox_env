import os

ALLOWED_FAILURE_MODES = {"none", "connection_timeout", "extract_error", "transform_error", "invalid_data", "publish_error"}
STATUSES = {"placed", "paid", "cancelled"}


def env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def connection_kwargs(database: str) -> dict[str, object]:
    return {
        "host": env("ETL_DB_HOST", "localhost"),
        "port": int(env("ETL_DB_PORT", "5433")),
        "user": env("ETL_DB_USER", "sandbox_admin"),
        "password": env("ETL_DB_PASSWORD", "change-me-local-only"),
        "dbname": database,
    }
