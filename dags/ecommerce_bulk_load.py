"""Stream 10M synthetic records to PostgreSQL using bounded-memory COPY batches."""
from datetime import datetime
from hashlib import sha256
import os
import random
import time

import psycopg
from airflow.sdk import dag, task, get_current_context

ROWS_PER_RUN = int(os.getenv("BULK_ROWS", "10000000"))
CHUNK_SIZE = int(os.getenv("BULK_CHUNK_SIZE", "25000"))


def connection_string() -> str:
    return os.getenv(
        "ECOMMERCE_DATABASE_URL",
        f"postgresql://airflow:{os.environ['POSTGRES_PASSWORD']}@postgres:5432/airflow",
    )


@dag(
    dag_id="ecommerce_bulk_load_10m",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "bulk", "postgresql"],
)
def ecommerce_bulk_load_10m():
    @task(execution_timeout=None)
    def load_in_chunks() -> dict[str, int | float]:
        with psycopg.connect(connection_string(), connect_timeout=10) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS ecommerce")
                cur.execute(
                    """CREATE TABLE IF NOT EXISTS ecommerce.synthetic_history (
                        id BIGINT PRIMARY KEY,
                        customer_id INTEGER NOT NULL,
                        amount NUMERIC(12, 2) NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL
                    )"""
                )
                run_id = get_current_context()["run_id"]
                base_id = int.from_bytes(sha256(run_id.encode()).digest()[:4], "big") * ROWS_PER_RUN
                cur.execute("CREATE TEMP TABLE bulk_stage (id BIGINT, customer_id INTEGER, amount NUMERIC(12,2), created_at TIMESTAMPTZ) ON COMMIT PRESERVE ROWS")
                started = time.monotonic()
                rng = random.Random(20261020 + base_id)
                created_at = datetime.now().astimezone()
                for batch_start in range(1, ROWS_PER_RUN + 1, CHUNK_SIZE):
                    batch_end = min(batch_start + CHUNK_SIZE, ROWS_PER_RUN + 1)
                    cur.execute("TRUNCATE bulk_stage")
                    with cur.copy(
                        "COPY bulk_stage (id, customer_id, amount, created_at) FROM STDIN"
                    ) as copy:
                        for offset in range(batch_start, batch_end):
                            copy.write_row((base_id + offset, rng.randint(1, 1_000_000), round(rng.uniform(1, 5000), 2), created_at))
                    cur.execute("INSERT INTO ecommerce.synthetic_history SELECT * FROM bulk_stage ON CONFLICT (id) DO NOTHING")
                    conn.commit()
                return {"rows_written": ROWS_PER_RUN, "seconds": round(time.monotonic() - started, 2)}

    load_in_chunks()


ecommerce_bulk_load_10m()
