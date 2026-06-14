import argparse
import json
import os
from datetime import datetime, timezone
from uuid import uuid4

import yfinance as yf
from confluent_kafka import Producer
from dotenv import load_dotenv


DEFAULT_TOPIC = "raw_market_ticks"


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean_value(value):
    if value is None:
        return None

    try:
        if hasattr(value, "item"):
            return value.item()
    except Exception:
        pass

    return value


def normalize_event_ts(value) -> str:
    if value is None:
        return now_utc()

    try:
        value_str = str(value)

        if value_str.isdigit():
            ts = int(value_str)

            # Epoch milliseconds
            if ts > 10_000_000_000:
                return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()

            # Epoch seconds
            return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

        return value_str

    except Exception:
        return now_utc()


def normalize_yfinance_message(message: dict) -> dict:
    symbol = (
        message.get("id")
        or message.get("symbol")
        or message.get("ticker")
        or "UNKNOWN"
    )

    price = (
        message.get("price")
        or message.get("regularMarketPrice")
        or message.get("lastPrice")
    )

    raw_event_ts = (
        message.get("time")
        or message.get("timestamp")
        or message.get("ts")
    )

    return {
        "event_id": str(uuid4()),
        "payload_version": "1.0",
        "event_type": "market_tick",
        "symbol": str(symbol),
        "source": "yfinance_websocket",
        "dataset": "yahoo_finance",
        "schema": "websocket_tick",
        "event_ts": normalize_event_ts(raw_event_ts),
        "ingest_ts": now_utc(),
        "price": clean_value(price),
        "volume": clean_value(message.get("dayVolume") or message.get("volume")),
        "change": clean_value(message.get("change")),
        "change_percent": clean_value(message.get("change_percent")),
        "exchange": message.get("exchange"),
        "market_hours": message.get("market_hours"),
        "raw": message,
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered topic={msg.topic()} partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Stream yfinance WebSocket ticks into Kafka."
    )
    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
        help="Kafka topic to publish to.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers.",
    )
    parser.add_argument(
        "--symbols",
        default=os.getenv("YFINANCE_SYMBOLS", "AAPL,MSFT,NVDA"),
        help="Comma-separated yfinance symbols.",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=int(os.getenv("YFINANCE_MAX_MESSAGES", "0")),
        help="Stop after N messages. Use 0 for unlimited.",
    )

    args = parser.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    producer = Producer({"bootstrap.servers": args.bootstrap_servers})

    print("Starting yfinance live Kafka producer")
    print(
        json.dumps(
            {
                "symbols": symbols,
                "topic": args.topic,
                "bootstrap_servers": args.bootstrap_servers,
                "max_messages": args.max_messages,
            },
            indent=2,
        )
    )

    seen = {"count": 0}

    def message_handler(message):
        event = normalize_yfinance_message(message)

        producer.produce(
            args.topic,
            key=event["symbol"],
            value=json.dumps(event, default=str),
            callback=delivery_report,
        )
        producer.poll(0)

        seen["count"] += 1

        print(
            f"[{seen['count']}] "
            f"{event['event_ts']} "
            f"{event['symbol']} price={event.get('price')} "
            f"source={event['source']}"
        )

        if args.max_messages > 0 and seen["count"] >= args.max_messages:
            print("Reached max messages. Stopping producer.")
            raise KeyboardInterrupt

    try:
        with yf.WebSocket(verbose=True) as ws:
            ws.subscribe(symbols)
            ws.listen(message_handler)

    except KeyboardInterrupt:
        print("\nStopping yfinance live producer.")

    finally:
        producer.flush()
        print(f"Producer stopped. Total messages sent: {seen['count']}")


if __name__ == "__main__":
    main()
