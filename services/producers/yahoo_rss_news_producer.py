import argparse
import json
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from uuid import uuid4

import requests
from confluent_kafka import Producer
from dotenv import load_dotenv


POSITIVE_WORDS = {
    "beat", "beats", "growth", "gain", "gains", "surge", "surges", "rally",
    "upgrade", "upgraded", "profit", "strong", "record", "bullish", "buy",
    "outperform", "rise", "rises", "higher", "positive", "launch", "expands",
}

NEGATIVE_WORDS = {
    "miss", "misses", "fall", "falls", "drop", "drops", "plunge", "plunges",
    "cut", "cuts", "downgrade", "downgraded", "loss", "weak", "bearish",
    "sell", "lawsuit", "probe", "risk", "lower", "negative", "pressure",
}


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def clean_text(value):
    if not value:
        return ""
    value = unescape(value)
    value = re.sub(r"<[^>]+>", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def parse_pub_date(value):
    if not value:
        return now_iso()

    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return now_iso()


def simple_sentiment(text):
    clean = re.sub(r"[^a-zA-Z\s]", " ", text or "").lower()
    words = set(clean.split())

    pos = len(words & POSITIVE_WORDS)
    neg = len(words & NEGATIVE_WORDS)

    score = pos - neg

    if score > 0:
        return "positive", 1
    if score < 0:
        return "negative", -1
    return "neutral", 0


def rss_url(symbol):
    return f"https://finance.yahoo.com/rss/headline?s={symbol}"


def normalize_item(symbol, item):
    title = clean_text(item.findtext("title"))
    description = clean_text(item.findtext("description"))
    link = clean_text(item.findtext("link"))
    pub_date = item.findtext("pubDate")

    label, score = simple_sentiment(f"{title} {description}")

    return {
        "event_id": str(uuid4()),
        "payload_version": "1.0",
        "event_type": "news_event",
        "symbol": symbol,
        "source": "yahoo_finance_rss",
        "dataset": "yahoo_finance",
        "schema": "rss_headline",
        "event_ts": parse_pub_date(pub_date),
        "ingest_ts": now_iso(),
        "headline": title,
        "summary": description or title,
        "category": "company",
        "url": link,
        "image": "",
        "related": symbol,
        "simple_sentiment_label": label,
        "simple_sentiment_score": score,
        "raw": {
            "title": title,
            "description": description,
            "link": link,
            "pubDate": pub_date,
        },
    }


def fetch_rss(symbol, timeout=20):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0 Safari/537.36"
        ),
        "Accept": "application/rss+xml, application/xml, text/xml, */*",
    }

    response = requests.get(rss_url(symbol), headers=headers, timeout=timeout)
    response.raise_for_status()

    root = ET.fromstring(response.content)
    channel = root.find("channel")

    if channel is None:
        return []

    return channel.findall("item")


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description="Real Yahoo Finance RSS company news producer")
    parser.add_argument(
        "--symbols",
        default=os.getenv("YAHOO_RSS_NEWS_SYMBOLS", "AAPL,MSFT,NVDA,AMZN,TSLA"),
    )
    parser.add_argument(
        "--topic",
        default=os.getenv("KAFKA_NEWS_TOPIC", "raw_news_events"),
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument(
        "--max-events-per-symbol",
        type=int,
        default=int(os.getenv("YAHOO_RSS_MAX_EVENTS_PER_SYMBOL", "5")),
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=float(os.getenv("YAHOO_RSS_SLEEP_SECONDS", "1.0")),
    )
    parser.add_argument(
        "--save-path",
        default="data/samples/yahoo_rss_news_events.jsonl",
    )

    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    producer = Producer({"bootstrap.servers": args.bootstrap_servers})

    os.makedirs(os.path.dirname(args.save_path), exist_ok=True)

    total = 0

    print("Starting Yahoo Finance RSS news Kafka producer")
    print(
        json.dumps(
            {
                "symbols": symbols,
                "topic": args.topic,
                "bootstrap_servers": args.bootstrap_servers,
                "max_events_per_symbol": args.max_events_per_symbol,
            },
            indent=2,
        )
    )

    with open(args.save_path, "w", encoding="utf-8") as f:
        for symbol in symbols:
            print(f"\n=== Fetching {symbol} Yahoo RSS ===")
            print(rss_url(symbol))

            try:
                items = fetch_rss(symbol)
            except Exception as exc:
                print(f"Yahoo RSS failed for {symbol}: {exc}")
                continue

            print(f"RSS items found: {len(items)}")

            for item in items[: args.max_events_per_symbol]:
                event = normalize_item(symbol, item)

                if not event["headline"]:
                    continue

                producer.produce(
                    args.topic,
                    key=symbol.encode("utf-8"),
                    value=json.dumps(event).encode("utf-8"),
                    callback=delivery_report,
                )
                producer.poll(0)

                f.write(json.dumps(event) + "\n")

                print(
                    f"SENT {event['event_ts']} {symbol} "
                    f"sentiment={event['simple_sentiment_label']} "
                    f"headline={event['headline'][:110]}"
                )

                total += 1

            time.sleep(args.sleep_seconds)

    producer.flush()

    print("")
    print(f"Saved {total} events to {args.save_path}")
    print("Yahoo Finance RSS news producer completed.")


if __name__ == "__main__":
    main()
