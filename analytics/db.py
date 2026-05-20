import logging
import duckdb
from pathlib import Path
from streaming.config import BRONZE_PATH, SILVER_PATH, GOLD_PATH

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

con = duckdb.connect()

def has_files(path: str) -> bool:
    return any(Path(path).rglob("*.parquet"))


def run_query(label: str, path: str, sql: str):
    if not has_files(path):
        logger.warning(f"No parquet files found at {path}, skipping {label}")
        return
    logger.info(f"Querying {label}")

    query = sql.format(path=path)

    df = con.execute(query).df()
    print(df.to_string(index=False))


# ---------- RAW ----------
run_query("RAW DATA", BRONZE_PATH, """
    SELECT *
    FROM read_parquet('{path}/**/*.parquet', hive_partitioning=true)
    WHERE symbol = 'BTCUSDT'
    AND date = '2026-05-19'
    ORDER BY event_time DESC
    LIMIT 10
""")


# ---------- SILVER ----------
run_query("1-MIN AVG", SILVER_PATH, """
    SELECT
        symbol,
        window_start,
        window_end,
        avg_price
    FROM read_parquet('{path}/**/*.parquet', hive_partitioning=true)
    WHERE symbol = 'BTCUSDT'
    AND date = '2026-05-19'
    ORDER BY window_start DESC
    LIMIT 10
""")


# ---------- GOLD ----------
run_query("ALERTS", GOLD_PATH, """
    SELECT *
    FROM read_parquet('{path}/**/*.parquet', hive_partitioning=true)
    ORDER BY event_time DESC
    LIMIT 10
""")