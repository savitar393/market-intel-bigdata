import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import finnhub
from dotenv import load_dotenv


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def unix_seconds_to_iso(value) -> str:
    if value is None:
        return now_utc()

    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc).isoformat()
    except Exception:
        return now_utc()


def clean_value(value):
    if value is None:
        return None

    try:
        if hasattr(value, "item"):
            return value.item()
    except Exception:
        pass

    return value


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


def normalize_news_sentiment(symbol: str, item: dict) -> dict:
    buzz = item.get("buzz") or {}
    company_news_score = item.get("companyNewsScore")
    sector_average = item.get("sectorAverageBullishPercent")
    sentiment = item.get("sentiment") or {}

    return {
        "event_id": str(uuid4()),
        "payload_version": "1.0",
        "event_type": "news_sentiment",
        "symbol": symbol,
        "source": "finnhub_news_sentiment",
        "dataset": "finnhub",
        "schema": "news_sentiment",
        "event_ts": now_utc(),
        "ingest_ts": now_utc(),
        "company_news_score": clean_value(company_news_score),
        "sector_average_bullish_percent": clean_value(sector_average),
        "buzz_articles_in_last_week": clean_value(buzz.get("articlesInLastWeek")),
        "buzz_weekly_average": clean_value(buzz.get("weeklyAverage")),
        "sentiment_bearish_percent": clean_value(sentiment.get("bearishPercent")),
        "sentiment_bullish_percent": clean_value(sentiment.get("bullishPercent")),
        "raw": item,
    }


def normalize_quote(symbol: str, item: dict) -> dict:
    # Finnhub quote fields commonly include:
    # c=current, d=change, dp=percent change, h=high, l=low, o=open, pc=previous close, t=timestamp
    return {
        "event_id": str(uuid4()),
        "payload_version": "1.0",
        "event_type": "market_quote",
        "symbol": symbol,
        "source": "finnhub_quote",
        "dataset": "finnhub",
        "schema": "quote",
        "event_ts": unix_seconds_to_iso(item.get("t")),
        "ingest_ts": now_utc(),
        "price": clean_value(item.get("c")),
        "change": clean_value(item.get("d")),
        "change_percent": clean_value(item.get("dp")),
        "high": clean_value(item.get("h")),
        "low": clean_value(item.get("l")),
        "open": clean_value(item.get("o")),
        "previous_close": clean_value(item.get("pc")),
        "raw": item,
    }


def normalize_supply_chain(symbol: str, item) -> dict:
    return {
        "event_id": str(uuid4()),
        "payload_version": "1.0",
        "event_type": "supply_chain_relation",
        "symbol": symbol,
        "source": "finnhub_stock_supply_chain",
        "dataset": "finnhub",
        "schema": "stock_supply_chain",
        "event_ts": now_utc(),
        "ingest_ts": now_utc(),
        "raw": item,
    }


def main():
    load_dotenv()

    api_key = os.getenv("FINNHUB_API_KEY")
    if not api_key:
        raise RuntimeError("Missing FINNHUB_API_KEY. Set it in your .env file.")

    symbols = [
        s.strip()
        for s in os.getenv("FINNHUB_SYMBOLS", "AAPL,MSFT,NVDA").split(",")
        if s.strip()
    ]

    lookback_days = int(os.getenv("FINNHUB_LOOKBACK_DAYS", "7"))

    to_date = datetime.now(timezone.utc).date()
    from_date = to_date - timedelta(days=lookback_days)

    client = finnhub.Client(api_key=api_key)

    output_dir = Path("data/samples")
    output_dir.mkdir(parents=True, exist_ok=True)

    news_path = output_dir / "finnhub_news_events.jsonl"
    sentiment_path = output_dir / "finnhub_sentiment_events.jsonl"
    quote_path = output_dir / "finnhub_quote_events.jsonl"
    supply_chain_path = output_dir / "finnhub_supply_chain_events.jsonl"

    # Reset files each run.
    for path in [news_path, sentiment_path, quote_path, supply_chain_path]:
        path.write_text("", encoding="utf-8")

    print("Finnhub validation config:")
    print(
        json.dumps(
            {
                "symbols": symbols,
                "from": str(from_date),
                "to": str(to_date),
                "lookback_days": lookback_days,
            },
            indent=2,
        )
    )

    total_news = 0
    total_sentiment = 0
    total_quotes = 0
    total_supply_chain = 0

    for symbol in symbols:
        print(f"\n=== Validating {symbol} ===")

        # 1. Quote
        try:
            quote = client.quote(symbol)
            quote_event = normalize_quote(symbol, quote)

            with quote_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(quote_event, default=str) + "\n")

            total_quotes += 1

            print(
                f"Quote: price={quote_event.get('price')} "
                f"change_percent={quote_event.get('change_percent')}"
            )

        except Exception as exc:
            print(f"Quote failed for {symbol}: {exc}")

        # 2. Company news
        try:
            news_items = client.company_news(
                symbol,
                _from=str(from_date),
                to=str(to_date),
            )

            print(f"Company news count: {len(news_items)}")

            with news_path.open("a", encoding="utf-8") as f:
                for item in news_items[:20]:
                    event = normalize_company_news(symbol, item)
                    f.write(json.dumps(event, default=str) + "\n")
                    total_news += 1

            if news_items:
                first = news_items[0]
                print(f"First headline: {first.get('headline')}")

        except Exception as exc:
            print(f"Company news failed for {symbol}: {exc}")

        # 3. News sentiment
        try:
            sentiment = client.news_sentiment(symbol)
            sentiment_event = normalize_news_sentiment(symbol, sentiment)

            with sentiment_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(sentiment_event, default=str) + "\n")

            total_sentiment += 1

            print(
                "Sentiment: "
                f"bullish={sentiment_event.get('sentiment_bullish_percent')} "
                f"bearish={sentiment_event.get('sentiment_bearish_percent')} "
                f"score={sentiment_event.get('company_news_score')}"
            )

        except Exception as exc:
            print(f"News sentiment failed for {symbol}: {exc}")

        # 4. Supply chain metadata
        # This may not be available on every plan/account. Failure is acceptable.
        try:
            supply_chain = client.stock_supply_chain(symbol)
            count = len(supply_chain) if hasattr(supply_chain, "__len__") else 1

            print(f"Supply chain records: {count}")

            with supply_chain_path.open("a", encoding="utf-8") as f:
                if isinstance(supply_chain, list):
                    for item in supply_chain[:20]:
                        event = normalize_supply_chain(symbol, item)
                        f.write(json.dumps(event, default=str) + "\n")
                        total_supply_chain += 1
                else:
                    event = normalize_supply_chain(symbol, supply_chain)
                    f.write(json.dumps(event, default=str) + "\n")
                    total_supply_chain += 1

        except Exception as exc:
            print(f"Supply chain failed for {symbol}: {exc}")

    print("\nValidation summary:")
    print(
        json.dumps(
            {
                "quote_events": total_quotes,
                "news_events": total_news,
                "sentiment_events": total_sentiment,
                "supply_chain_events": total_supply_chain,
                "files": {
                    "quote": str(quote_path),
                    "news": str(news_path),
                    "sentiment": str(sentiment_path),
                    "supply_chain": str(supply_chain_path),
                },
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
