import argparse
import json
import os
import re
from datetime import datetime, timezone
from urllib.parse import quote_plus
from uuid import uuid4

import requests
from confluent_kafka import Producer
from dotenv import load_dotenv


SYMBOL_QUERY_MAP = {
    "AAPL": '"Apple" OR "Apple Inc" OR AAPL',
    "MSFT": '"Microsoft" OR "Microsoft Corp" OR MSFT',
    "NVDA": '"NVIDIA" OR "Nvidia" OR NVDA',
    "AMZN": '"Amazon" OR "Amazon.com" OR AMZN',
    "TSLA": '"Tesla" OR "Tesla Inc" OR TSLA',
}

POSITIVE_WORDS = {
    "beat", "beats", "growth", "gain", "gains", "surge", "surges", "rally",
    "upgrade", "upgraded", "profit", "strong", "record", "bullish", "buy",
    "outperform", "rise", "rises", "higher", "positive"
}

NEGATIVE_WORDS = {
    "miss", "misses", "fall", "falls", "drop", "drops", "plunge", "plunges",
    "cut", "cuts", "downgrade", "downgraded", "loss", "weak", "bearish",
    "sell", "lawsuit", "probe", "risk", "lower", "negative"
}


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def parse_gdelt_datetime(value):
    if not value:
        return now_iso()

    # GDELT normally returns YYYYMMDDHHMMSS.
    text = str(value)

    try:
        dt = datetime.strptime(text[:14], "%Y%m%d%H%M%S")
        return dt.replace(tzinfo=timezone.utc).isoformat()
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


def build_url(query, timespan, max_records):
    encoded_query = quote_plus(f"({query}) sourcelang:english")
    return (
        "https://api.gdeltproject.org/api/v2/doc/doc"
        f"?query={encoded_query}"
        "&mode=artlist"
        "&format=json"
        "&sort=datedesc"
        f"&timespan={quote_plus(timespan)}"
        f"&maxrecords={max_records}"
    )


def normalize_article(symbol, article):
    title = article.get("title") or ""
    url = article.get("url") or ""
    source = article.get("sourceCommonName") or article.get("domain") or "gdelt"
    event_ts = parse_gdelt_datetime(article.get("seendate"))
    label, score = simple_sentiment(title)

    return {
        "event_id": str(uuid4()),
        "payload_version": "1.0",
        "event_type": "news_event",
        "symbol": symbol,
        "source": "gdelt_doc_api",
        "dataset": "gdelt",
        "schema": "article_list",
        "event_ts": event_ts,
        "ingest_ts": now_iso(),
        "headline": title,
        "summary": article.get("snippet") or title,
        "category": "company",
        "url": url,
        "image": article.get("socialimage") or "",
        "related": symbol,
        "simple_sentiment_label": label,
        "simple_sentiment_score": score,
        "raw": article,
    }


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

    parser = argparse.ArgumentParser(description="Free GDELT company news producer")
    parser.add_argument(
        "--symbols",
        default=os.getenv("GDELT_NEWS_SYMBOLS", "AAPL,MSFT,NVDA,AMZN,TSLA"),
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
        "--timespan",
        default=os.getenv("GDELT_NEWS_TIMESPAN", "7d"),
        help="Examples: 24h, 3d, 7d, 1m",
    )
    parser.add_argument(
        "--max-events-per-symbol",
        type=int,
        default=int(os.getenv("GDELT_MAX_EVENTS_PER_SYMBOL", "5")),
    )
    parser.add_argument(
        "--save-path",
        default="data/samples/gdelt_news_events.jsonl",
    )

    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    producer = Producer({"bootstrap.servers": args.bootstrap_servers})

    os.makedirs(os.path.dirname(args.save_path), exist_ok=True)

    total = 0

    print("Starting GDELT news Kafka producer")
    print(
        json.dumps(
            {
                "symbols": symbols,
                "topic": args.topic,
                "bootstrap_servers": args.bootstrap_servers,
                "timespan": args.timespan,
                "max_events_per_symbol": args.max_events_per_symbol,
            },
            indent=2,
        )
    )

    with open(args.save_path, "w", encoding="utf-8") as f:
        for symbol in symbols:
            query = SYMBOL_QUERY_MAP.get(symbol, symbol)
            url = build_url(query, args.timespan, args.max_events_per_symbol)

            print(f"\n=== Fetching {symbol} from GDELT ===")
            print(url)

            try:
                response = requests.get(url, timeout=20)
                response.raise_for_status()
                payload = response.json()
            except Exception as exc:
                print(f"GDELT failed for {symbol}: {exc}")
                continue

            articles = payload.get("articles", []) or []

            print(f"Articles found: {len(articles)}")

            for article in articles[: args.max_events_per_symbol]:
                event = normalize_article(symbol, article)

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
                    f"headline={event['headline'][:100]}"
                )

                total += 1

    producer.flush()

    print("")
    print(f"Saved {total} events to {args.save_path}")
    print("GDELT news producer completed.")


if __name__ == "__main__":
    main()
