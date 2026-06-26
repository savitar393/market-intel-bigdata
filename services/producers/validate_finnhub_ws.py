import argparse
import asyncio
import json
import os
from datetime import datetime, timezone
from uuid import uuid4

import websockets
from dotenv import load_dotenv


def normalize_symbol(raw_symbol: str) -> str:
    mapping = {
        "BINANCE:BTCUSDT": "BTC-USD",
    }
    return mapping.get(raw_symbol, raw_symbol)


def timestamp_ms_to_iso(timestamp_ms: int | float | None) -> str:
    if timestamp_ms is None:
        return datetime.now(timezone.utc).isoformat()

    return datetime.fromtimestamp(float(timestamp_ms) / 1000.0, tz=timezone.utc).isoformat()


def normalize_trade(trade: dict) -> dict:
    raw_symbol = trade.get("s")
    symbol = normalize_symbol(raw_symbol)

    return {
        "event_id": str(uuid4()),
        "payload_version": "1.0",
        "event_type": "market_tick",
        "symbol": symbol,
        "source": "finnhub_websocket",
        "dataset": "finnhub",
        "schema": "websocket_trade",
        "event_ts": timestamp_ms_to_iso(trade.get("t")),
        "ingest_ts": datetime.now(timezone.utc).isoformat(),
        "price": float(trade["p"]) if trade.get("p") is not None else None,
        "volume": float(trade["v"]) if trade.get("v") is not None else None,
        "raw": trade,
    }


async def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description="Validate Finnhub WebSocket trades.")
    parser.add_argument(
        "--symbols",
        default=os.getenv("FINNHUB_WS_SYMBOLS", "AAPL,MSFT,NVDA,BINANCE:BTCUSDT"),
        help="Comma-separated Finnhub symbols.",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=int(os.getenv("FINNHUB_WS_MAX_MESSAGES", "20")),
    )
    parser.add_argument(
        "--output",
        default="data/samples/finnhub_ws_events.jsonl",
    )

    args = parser.parse_args()

    token = os.getenv("FINNHUB_API_KEY")

    if not token:
        raise RuntimeError("Missing FINNHUB_API_KEY in .env")

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    uri = f"wss://ws.finnhub.io?token={token}"

    os.makedirs(os.path.dirname(args.output), exist_ok=True)

    print("Finnhub WebSocket validation config:")
    print(json.dumps({"symbols": symbols, "max_messages": args.max_messages}, indent=2))
    print(f"Writing normalized events to: {args.output}")

    collected = 0

    async with websockets.connect(uri, ping_interval=20, ping_timeout=20) as websocket:
        print("Connected to Finnhub WebSocket.")

        for symbol in symbols:
            subscribe_msg = {"type": "subscribe", "symbol": symbol}
            await websocket.send(json.dumps(subscribe_msg))
            print(f"Subscribed: {symbol}")

        with open(args.output, "w", encoding="utf-8") as f:
            while collected < args.max_messages:
                raw_message = await websocket.recv()

                try:
                    payload = json.loads(raw_message)
                except json.JSONDecodeError:
                    print(f"Non-JSON message: {raw_message}")
                    continue

                if payload.get("type") != "trade":
                    print(f"Control message: {payload}")
                    continue

                for trade in payload.get("data", []):
                    event = normalize_trade(trade)

                    if event["price"] is None:
                        continue

                    f.write(json.dumps(event) + "\n")
                    f.flush()

                    collected += 1

                    print(
                        f"[{collected}/{args.max_messages}] "
                        f"{event['event_ts']} {event['symbol']} "
                        f"price={event['price']} volume={event['volume']} "
                        f"source={event['source']}"
                    )

                    if collected >= args.max_messages:
                        break

    print(f"Saved {collected} events to {args.output}")


if __name__ == "__main__":
    asyncio.run(main())
