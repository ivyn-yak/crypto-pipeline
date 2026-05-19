import requests
import time
import json
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

URL = "https://api.binance.com/api/v3/ticker/price"

def fetch_prices():
    response = requests.get(URL)
    return response.json()

while True:
    data = fetch_prices()

    for item in data:
        event = {
            "symbol": item["symbol"],
            "price": float(item["price"]),
            "event_time": datetime.now().isoformat()
        }

        producer.send("crypto-prices", event)

    print("sent batch")
    time.sleep(5)