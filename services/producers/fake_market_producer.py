import json
import random
import time
from datetime import datetime, timezone
from uuid import uuid4

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "raw_market_ticks"

SYMBOLS = ["AAPL", "NVDA", "MSFT", "TSLA", "AMZN"]

BASE_PRICES = {
    "AAPL": 195.0,
    "NVDA": 920.0,
    "MSFT": 430.0,
    "TSLA": 175.0,
    "AMZN": 185.0,
}


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Sent topic={msg.topic()} partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


def build_event(symbol: str) -> dict:
    base = BASE_PRICES[symbol]
    price = round(base + random.uniform(-2.5, 2.5), 2)
    volume = random.randint(100, 5000)

    return {
        "event_id": str(uuid4()),
        "payload_version": "1.0",
        "event_type": "market_tick",
        "symbol": symbol,
        "source": "fake_market_producer",
        "event_ts": now_utc(),
        "ingest_ts": now_utc(),
        "price": price,
        "volume": volume,
    }


def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    print(f"Producing fake market ticks to Kafka topic: {TOPIC}")

    while True:
        symbol = random.choice(SYMBOLS)
        event = build_event(symbol)

        producer.produce(
            TOPIC,
            key=symbol,
            value=json.dumps(event),
            callback=delivery_report,
        )
        producer.poll(0)

        print(event)
        time.sleep(1)


if __name__ == "__main__":
    main()
