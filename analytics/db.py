from pathlib import Path
import duckdb

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_PATH = BASE_DIR / "storage/crypto/*.parquet"

duckdb.query(f"""
SELECT * FROM '{DATA_PATH}'
LIMIT 10
""").show()