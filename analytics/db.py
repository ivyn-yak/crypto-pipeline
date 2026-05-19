import glob
import logging
import duckdb
from streaming.config import BRONZE_PATH, SILVER_PATH, GOLD_PATH

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

con = duckdb.connect()

def has_files(path) -> bool:
    return len(glob.glob(f"{path}/*.parquet")) > 0

def query_layer(label: str, path: str, sql: str):
    if not has_files(path):
        logger.warning(f"No parquet files found at {path}, skipping {label}")
        return
    logger.info(f"Querying {label}")
    con.execute(sql, [f"{path}/*.parquet"]).df().to_string()
    print(con.execute(sql, [f"{path}/*.parquet"]).df().to_string())

query_layer("RAW DATA", BRONZE_PATH, """
    SELECT * FROM read_parquet(?)
    ORDER BY event_time DESC
    LIMIT 10
""")

query_layer("1-MIN AVG", SILVER_PATH, """
    SELECT
        symbol,
        window_start,
        window_end,
        avg_price
    FROM read_parquet(?)
    ORDER BY window_start DESC
    LIMIT 10
""")

query_layer("ALERTS", GOLD_PATH, """
    SELECT * FROM read_parquet(?)
    ORDER BY event_time DESC
    LIMIT 10
""")