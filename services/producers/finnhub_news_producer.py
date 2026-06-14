import argparse
import json
import os
import time
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import finnhub
from confluent_kafka import Producer
from dotenv import load_dotenv


DEFAULT_TOPIC = "raw_news_events"


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def unix_seconds_to_iso(value) -> str:
    if value is None:
        return now_utc()

    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc).isoformat()
    except Exception:
        return now_utc()


def normalize_company_news(symbol: str, item: dict) -> dict:
    return {
        "event_id": str(uuid4()),
        "payload_version": "1.0",
        "event_type": "news_event",
        "symbol": symbol,
        "source": "finnhub_company_news",
        "dataset": "finnhub",
        "schema": "company_news",
        "event_ts": unix_seconds_to_iso(item.get("datetime")),
        "ingest_ts": now_utc(),
        "headline": item.get("headline"),
        "summary": item.get("summary"),
        "category": item.get("category"),
        "url": item.get("url"),
        "image": item.get("image"),
        "related": item.get("related"),
        "raw": item,
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
        description="Poll Finnhub company news and publish normalized events to Kafka."
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
        default=os.getenv("FINNHUB_SYMBOLS", "AAPL,MSFT,NVDA"),
        help="Comma-separated symbols.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=int(os.getenv("FINNHUB_LOOKBACK_DAYS", "7")),
        help="Lookback window for company news.",
    )
    parser.add_argument(
        "--max-events-per-symbol",
        type=int,
        default=10,
        help="Maximum news events to publish per symbol.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run once and stop.",
    )
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=60,
        help="Polling interval if not using --once.",
    )

    args = parser.parse_args()

    api_key = os.getenv("FINNHUB_API_KEY")
    if not api_key:
        raise RuntimeError("Missing FINNHUB_API_KEY. Set it in .env.")

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    client = finnhub.Client(api_key=api_key)
    producer = Producer({"bootstrap.servers": args.bootstrap_servers})

    seen_news_ids = set()

    print("Starting Finnhub news Kafka producer")
    print(
        json.dumps(
            {
                "symbols": symbols,
                "topic": args.topic,
                "bootstrap_servers": args.bootstrap_servers,
                "lookback_days": args.lookback_days,
                "max_events_per_symbol": args.max_events_per_symbol,
                "once": args.once,
            },
            indent=2,
        )
    )

    while True:
        to_date = datetime.now(timezone.utc).date()
        from_date = to_date - timedelta(days=args.lookback_days)

        total_sent = 0

        for symbol in symbols:
            try:
                news_items = client.company_news(
                    symbol,
                    _from=str(from_date),
                    to=str(to_date),
                )
            except Exception as exc:
                print(f"Failed to fetch news for {symbol}: {exc}")
                continue

            for item in news_items[: args.max_events_per_symbol]:
                news_id = str(item.get("id") or item.get("url") or item.get("headline"))

                dedupe_key = f"{symbol}:{news_id}"
                if dedupe_key in seen_news_ids:
                    continue

                seen_news_ids.add(dedupe_key)

                event = normalize_company_news(symbol, item)

                producer.produce(
                    args.topic,
                    key=symbol,
                    value=json.dumps(event, default=str),
                    callback=delivery_report,
                )
                producer.poll(0)

                total_sent += 1

                print(
                    f"SENT {event['event_ts']} {symbol} "
                    f"headline={event.get('headline')}"
                )

        producer.flush()

        print(f"Poll completed. Sent {total_sent} new news events.")

        if args.once:
            break

        time.sleep(args.poll_seconds)

    print("Finnhub news producer stopped.")


if __name__ == "__main__":
    main()
