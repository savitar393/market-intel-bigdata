import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import databento as db
from confluent_kafka import Producer
from dotenv import load_dotenv


DEFAULT_TOPIC = "raw_market_ticks"


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_ts(value: str) -> datetime:
    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")
    return datetime.fromisoformat(value)


def clean_value(value):
    if value is None:
        return None

    if hasattr(value, "isoformat"):
        return value.isoformat()

    try:
        if hasattr(value, "item"):
            return value.item()
    except Exception:
        pass

    return value


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered topic={msg.topic()} partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


def row_to_event(row: dict, config: dict) -> dict:
    event_ts = (
        row.get("ts_event")
        or row.get("event_ts")
        or row.get("ts_recv")
        or row.get("index")
        or row.get("timestamp")
    )

    return {
        "event_id": str(uuid4()),
        "payload_version": "1.0",
        "event_type": "market_bar",
        "symbol": str(row.get("symbol", config["symbols"][0])),
        "source": "databento_historical_replay",
        "dataset": config["dataset"],
        "schema": config["schema"],
        "event_ts": clean_value(event_ts),
        "ingest_ts": now_utc(),
        "open": clean_value(row.get("open")),
        "high": clean_value(row.get("high")),
        "low": clean_value(row.get("low")),
        "close": clean_value(row.get("close")),
        "volume": clean_value(row.get("volume")),
    }


def load_events_from_jsonl(path: Path) -> list[dict]:
    events = []

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                event = json.loads(line)
                event["source"] = "databento_jsonl_replay"
                event["ingest_ts"] = now_utc()
                events.append(event)

    return events


def load_events_from_databento(config: dict) -> list[dict]:
    api_key = os.getenv("DATABENTO_API_KEY")
    if not api_key:
        raise RuntimeError("Missing DATABENTO_API_KEY. Set it in your .env file.")

    client = db.Historical(api_key)

    data = client.timeseries.get_range(
        dataset=config["dataset"],
        schema=config["schema"],
        symbols=config["symbols"],
        start=config["start"],
        end=config["end"],
    )

    df = data.to_df().reset_index()

    events = []
    for _, row in df.iterrows():
        events.append(row_to_event(row.to_dict(), config))

    return events


def replay_events(events: list[dict], bootstrap_servers: str, topic: str, speed: float):
    if not events:
        print("No events to replay.")
        return

    events = sorted(events, key=lambda e: e["event_ts"])

    producer = Producer({"bootstrap.servers": bootstrap_servers})

    print(f"Replaying {len(events)} events to Kafka topic={topic}")
    print(f"Bootstrap servers: {bootstrap_servers}")
    print(f"Replay speed: {speed}x")

    previous_ts = None

    for event in events:
        current_ts = parse_ts(event["event_ts"])

        if previous_ts is not None:
            delta_seconds = (current_ts - previous_ts).total_seconds()

            if delta_seconds > 0:
                sleep_seconds = delta_seconds / speed
                time.sleep(min(sleep_seconds, 2.0))

        event["ingest_ts"] = now_utc()

        producer.produce(
            topic,
            key=event["symbol"],
            value=json.dumps(event),
            callback=delivery_report,
        )
        producer.poll(0)

        print(
            f"REPLAYED {event['event_ts']} "
            f"{event['symbol']} close={event.get('close')} "
            f"volume={event.get('volume')}"
        )

        previous_ts = current_ts

    producer.flush()
    print("Replay completed.")


def main():
    parser = argparse.ArgumentParser(
        description="Replay Databento historical market events into Kafka."
    )

    parser.add_argument(
        "--input-jsonl",
        default=None,
        help="Optional JSONL file to replay instead of calling Databento API.",
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=60.0,
        help="Replay speed multiplier. Example: 60 means 1 minute becomes 1 second.",
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

    args = parser.parse_args()

    load_dotenv()

    config = {
        "dataset": os.getenv("DATABENTO_DATASET", "XNAS.ITCH"),
        "schema": os.getenv("DATABENTO_SCHEMA", "ohlcv-1m"),
        "symbols": [
            s.strip()
            for s in os.getenv("DATABENTO_SYMBOLS", "AAPL,MSFT,NVDA").split(",")
            if s.strip()
        ],
        "start": os.getenv("DATABENTO_START", "2024-05-21T15:00"),
        "end": os.getenv("DATABENTO_END", "2024-05-21T15:05"),
    }

    if args.input_jsonl:
        events = load_events_from_jsonl(Path(args.input_jsonl))
    else:
        events = load_events_from_databento(config)

    replay_events(
        events=events,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        speed=args.speed,
    )


if __name__ == "__main__":
    main()
