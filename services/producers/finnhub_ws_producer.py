import argparse
import asyncio
import json
import os
from datetime import datetime, timezone
from uuid import uuid4

import websockets
from confluent_kafka import Producer
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


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
        )


async def main():
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Produce Finnhub WebSocket trade ticks to Kafka."
    )
    parser.add_argument(
        "--symbols",
        default=os.getenv("FINNHUB_WS_SYMBOLS", "AAPL,MSFT,NVDA,BINANCE:BTCUSDT"),
        help="Comma-separated Finnhub symbols.",
    )
    parser.add_argument(
        "--topic",
        default=os.getenv("KAFKA_MARKET_TOPIC", "raw_market_ticks"),
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Stop after N normalized trade events. 0 means run forever.",
    )

    args = parser.parse_args()

    token = os.getenv("FINNHUB_API_KEY")

    if not token:
        raise RuntimeError("Missing FINNHUB_API_KEY in .env")

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "client.id": "finnhub-ws-producer",
        }
    )

    uri = f"wss://ws.finnhub.io?token={token}"

    print("Starting Finnhub WebSocket Kafka producer")
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

    sent = 0

    async with websockets.connect(uri, ping_interval=20, ping_timeout=20) as websocket:
        print("Connected to Finnhub WebSocket.")

        for symbol in symbols:
            await websocket.send(json.dumps({"type": "subscribe", "symbol": symbol}))
            print(f"Subscribed: {symbol}")

        while True:
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

                key = event["symbol"]
                value = json.dumps(event).encode("utf-8")

                producer.produce(
                    args.topic,
                    key=key,
                    value=value,
                    callback=delivery_report,
                )
                producer.poll(0)

                sent += 1

                print(
                    f"[{sent}] {event['event_ts']} {event['symbol']} "
                    f"price={event['price']} volume={event['volume']} "
                    f"source={event['source']}"
                )

                if args.max_messages and sent >= args.max_messages:
                    producer.flush()
                    print(f"Producer stopped. Total messages sent: {sent}")
                    return


if __name__ == "__main__":
    asyncio.run(main())
