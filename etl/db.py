from contextlib import contextmanager
from typing import Iterator

import psycopg2
from psycopg2.extensions import connection

from etl.config import connection_kwargs


@contextmanager
def connect(database: str) -> Iterator[connection]:
    conn = psycopg2.connect(**connection_kwargs(database))
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
