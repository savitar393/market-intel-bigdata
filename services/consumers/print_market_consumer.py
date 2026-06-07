import json

from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "raw_market_ticks"

consumer = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "print-market-consumer",
        "auto.offset.reset": "earliest",
    }
)

consumer.subscribe([TOPIC])

print(f"Consuming from Kafka topic: {TOPIC}")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        event = json.loads(msg.value().decode("utf-8"))

        print(
            f"[{event['event_ts']}] "
            f"{event['symbol']} price={event['price']} "
            f"volume={event['volume']} source={event['source']}"
        )

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    consumer.close()
