import json
import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import yfinance as yf
from dotenv import load_dotenv


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

        # yfinance WebSocket often sends epoch milliseconds as string/int.
        if value_str.isdigit():
            ts = int(value_str)

            # milliseconds
            if ts > 10_000_000_000:
                return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()

            # seconds
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
        "raw": message,
    }


def main():
    load_dotenv()

    symbols = [
        s.strip()
        for s in os.getenv("YFINANCE_SYMBOLS", "AAPL,MSFT,NVDA,AMZN").split(",")
        if s.strip()
    ]
    max_messages = int(os.getenv("YFINANCE_MAX_MESSAGES", "20"))

    output_dir = Path("data/samples")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "yfinance_live_events.jsonl"

    # Reset validation file each run.
    output_path.write_text("", encoding="utf-8")

    print("yfinance live validation config:")
    print(json.dumps({"symbols": symbols, "max_messages": max_messages}, indent=2))
    print(f"Saving normalized events to: {output_path}")
    print("Waiting for WebSocket messages...")

    seen = {"count": 0}

    def message_handler(message):
        event = normalize_yfinance_message(message)

        with output_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, default=str) + "\n")

        seen["count"] += 1

        print(
            f"[{seen['count']}/{max_messages}] "
            f"{event['event_ts']} "
            f"{event['symbol']} price={event.get('price')} "
            f"volume={event.get('volume')} "
            f"source={event['source']}"
        )

        if seen["count"] >= max_messages:
            print("Collected enough messages. Stop with Ctrl+C if it does not exit automatically.")
            raise KeyboardInterrupt

    try:
        with yf.WebSocket(verbose=True) as ws:
            ws.subscribe(symbols)
            ws.listen(message_handler)
    except KeyboardInterrupt:
        print("\nStopping yfinance validation.")

    print(f"Saved {seen['count']} events to {output_path}")


if __name__ == "__main__":
    main()
