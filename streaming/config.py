import os
from pathlib import Path

BASE_DIR = Path(os.getcwd())

KAFKA_TOPIC = "crypto-prices"
KAFKA_SERVER = "localhost:9092"

BRONZE_PATH = str(BASE_DIR / "storage/bronze/crypto")
SILVER_PATH = str(BASE_DIR / "storage/silver/crypto_1min")
GOLD_PATH = str(BASE_DIR / "storage/gold/alerts")

CHECKPOINT_BRONZE = str(BASE_DIR / "storage/checkpoints/bronze")
CHECKPOINT_SILVER = str(BASE_DIR / "storage/checkpoints/silver")
CHECKPOINT_GOLD = str(BASE_DIR / "storage/checkpoints/gold")