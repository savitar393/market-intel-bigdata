import json
import os

from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_NEWS_TOPIC", "raw_news_events")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "print-news-consumer-v1")

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

        print(
            f"[{event.get('event_ts')}] "
            f"{event.get('symbol')} "
            f"type={event.get('event_type')} "
            f"source={event.get('source')} "
            f"headline={event.get('headline')}"
        )

except KeyboardInterrupt:
    print("Stopping news consumer...")

finally:
    consumer.close()
