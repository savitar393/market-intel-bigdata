import json
import os

from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "raw_market_ticks")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "print-market-consumer-v2")

consumer = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    }
)

consumer.subscribe([TOPIC])

print(f"Consuming from Kafka topic: {TOPIC}")
print(f"Consumer group: {GROUP_ID}")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        try:
            event = json.loads(msg.value().decode("utf-8"))
        except Exception as exc:
            print(f"Failed to parse message: {exc}")
            continue

        event_type = event.get("event_type", "unknown")
        symbol = event.get("symbol", "UNKNOWN")
        source = event.get("source", "unknown")
        event_ts = event.get("event_ts", "unknown_time")

        display_price = (
            event.get("price")
            if event.get("price") is not None
            else event.get("close")
        )

        open_price = event.get("open")
        high = event.get("high")
        low = event.get("low")
        close = event.get("close")
        volume = event.get("volume")

        if event_type == "market_bar":
            print(
                f"[{event_ts}] {symbol} "
                f"type={event_type} "
                f"OHLC=({open_price}, {high}, {low}, {close}) "
                f"volume={volume} "
                f"source={source}"
            )
        else:
            print(
                f"[{event_ts}] {symbol} "
                f"type={event_type} "
                f"price={display_price} "
                f"volume={volume} "
                f"source={source}"
            )

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    consumer.close()
