import asyncio
import os
from datetime import date, datetime
from typing import Any

from cassandra.cluster import Cluster
from dotenv import load_dotenv
from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

load_dotenv()

CASSANDRA_HOSTS = [
    h.strip()
    for h in os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
    if h.strip()
]
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "market_intel")

cluster: Cluster | None = None
session = None

app = FastAPI(
    title="Market Intel Big Data API",
    description="REST and WebSocket gateway for market and news data served from Cassandra.",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # OK for local demo. Restrict later if deployed.
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_session():
    global cluster, session

    if session is None:
        cluster = Cluster(CASSANDRA_HOSTS, port=CASSANDRA_PORT)
        session = cluster.connect(CASSANDRA_KEYSPACE)

    return session


def serialize_value(value: Any):
    if isinstance(value, (datetime, date)):
        return value.isoformat()

    return value


def row_to_dict(row) -> dict:
    data = row._asdict()
    return {key: serialize_value(value) for key, value in data.items()}


def fetch_market_rows(symbol: str, limit: int = 20) -> list[dict]:
    safe_limit = max(1, min(limit, 100))
    sess = get_session()

    rows = sess.execute(
        f"""
        SELECT
            symbol,
            event_time,
            event_id,
            event_type,
            source,
            dataset,
            schema_name,
            market_price,
            price,
            open,
            high,
            low,
            close,
            volume,
            ingest_time,
            spark_process_time,
            ingest_latency_seconds
        FROM market_ticks_by_symbol
        WHERE symbol = %s
        LIMIT {safe_limit}
        """,
        (symbol,),
    )

    return [row_to_dict(row) for row in rows]


def fetch_news_rows(symbol: str, limit: int = 20) -> list[dict]:
    safe_limit = max(1, min(limit, 100))
    sess = get_session()

    rows = sess.execute(
        f"""
        SELECT
            symbol,
            event_time,
            event_id,
            source,
            dataset,
            schema_name,
            headline,
            summary,
            category,
            url,
            related,
            simple_sentiment_label,
            simple_sentiment_score,
            ingest_time,
            spark_process_time,
            ingest_latency_seconds
        FROM news_events_by_symbol
        WHERE symbol = %s
        LIMIT {safe_limit}
        """,
        (symbol,),
    )

    return [row_to_dict(row) for row in rows]


@app.get("/")
def root():
    return {
        "service": "market-intel-bigdata-api",
        "status": "ok",
        "docs": "/docs",
        "health": "/api/v1/health",
    }


@app.get("/api/v1/health")
def health():
    sess = get_session()
    row = sess.execute("SELECT release_version FROM system.local").one()

    return {
        "status": "ok",
        "cassandra": {
            "connected": True,
            "hosts": CASSANDRA_HOSTS,
            "port": CASSANDRA_PORT,
            "keyspace": CASSANDRA_KEYSPACE,
            "release_version": row.release_version if row else None,
        },
    }


@app.get("/api/v1/market/latest/{symbol}")
def latest_market(
    symbol: str,
    limit: int = Query(default=20, ge=1, le=100),
):
    items = fetch_market_rows(symbol.upper(), limit)

    return {
        "symbol": symbol.upper(),
        "count": len(items),
        "items": items,
    }


@app.get("/api/v1/news/latest/{symbol}")
def latest_news(
    symbol: str,
    limit: int = Query(default=20, ge=1, le=100),
):
    items = fetch_news_rows(symbol.upper(), limit)

    return {
        "symbol": symbol.upper(),
        "count": len(items),
        "items": items,
    }


@app.get("/api/v1/dashboard/snapshot/{symbol}")
def dashboard_snapshot(
    symbol: str,
    market_limit: int = Query(default=20, ge=1, le=100),
    news_limit: int = Query(default=10, ge=1, le=100),
):
    normalized_symbol = symbol.upper()

    return {
        "symbol": normalized_symbol,
        "market": fetch_market_rows(normalized_symbol, market_limit),
        "news": fetch_news_rows(normalized_symbol, news_limit),
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }


@app.websocket("/ws/market/{symbol}")
async def ws_market(
    websocket: WebSocket,
    symbol: str,
    interval_seconds: float = Query(default=2.0, ge=0.5, le=10.0),
):
    await websocket.accept()

    normalized_symbol = symbol.upper()

    try:
        while True:
            payload = {
                "type": "market_snapshot",
                "symbol": normalized_symbol,
                "items": fetch_market_rows(normalized_symbol, 10),
                "generated_at": datetime.utcnow().isoformat() + "Z",
            }

            await websocket.send_json(payload)
            await asyncio.sleep(interval_seconds)

    except WebSocketDisconnect:
        print(f"Market WebSocket disconnected: {normalized_symbol}")


@app.websocket("/ws/news/{symbol}")
async def ws_news(
    websocket: WebSocket,
    symbol: str,
    interval_seconds: float = Query(default=5.0, ge=1.0, le=30.0),
):
    await websocket.accept()

    normalized_symbol = symbol.upper()

    try:
        while True:
            payload = {
                "type": "news_snapshot",
                "symbol": normalized_symbol,
                "items": fetch_news_rows(normalized_symbol, 10),
                "generated_at": datetime.utcnow().isoformat() + "Z",
            }

            await websocket.send_json(payload)
            await asyncio.sleep(interval_seconds)

    except WebSocketDisconnect:
        print(f"News WebSocket disconnected: {normalized_symbol}")


@app.websocket("/ws/live/{symbol}")
async def ws_live(
    websocket: WebSocket,
    symbol: str,
    interval_seconds: float = Query(default=2.0, ge=0.5, le=10.0),
):
    await websocket.accept()

    normalized_symbol = symbol.upper()

    try:
        while True:
            payload = {
                "type": "dashboard_live_snapshot",
                "symbol": normalized_symbol,
                "market": fetch_market_rows(normalized_symbol, 10),
                "news": fetch_news_rows(normalized_symbol, 5),
                "generated_at": datetime.utcnow().isoformat() + "Z",
            }

            await websocket.send_json(payload)
            await asyncio.sleep(interval_seconds)

    except WebSocketDisconnect:
        print(f"Live WebSocket disconnected: {normalized_symbol}")


@app.on_event("shutdown")
def shutdown_event():
    global cluster

    if cluster is not None:
        cluster.shutdown()
