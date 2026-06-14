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

        price = event.get("price")
        close = event.get("close")
        display_price = price if price is not None else close

        print(
            f"[{event.get('event_ts')}] "
            f"{event.get('symbol')} "
            f"type={event.get('event_type')} "
            f"price_or_close={display_price} "
            f"volume={event.get('volume')} "
            f"source={event.get('source')}"
        )

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    consumer.close()
