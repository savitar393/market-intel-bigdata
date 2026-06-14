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

def parse_dt(value):
    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def seconds_since(value) -> float | None:
    dt = parse_dt(value)

    if dt is None:
        return None

    now = datetime.utcnow()

    if dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)

    return (now - dt).total_seconds()


def average_numeric(items: list[dict], key: str) -> float | None:
    values = [
        float(item[key])
        for item in items
        if item.get(key) is not None
    ]

    if not values:
        return None

    return sum(values) / len(values)

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

def fetch_prediction_rows(symbol: str, limit: int = 20) -> list[dict]:
    safe_limit = max(1, min(int(limit), 100))
    sess = get_session()

    rows = sess.execute(
        f"""
        SELECT
            symbol,
            event_time,
            prediction_time,
            event_id,
            model_name,
            market_price,
            predicted_direction,
            probability_down,
            probability_up,
            target_direction,
            source
        FROM model_predictions_by_symbol
        WHERE symbol = %s
        LIMIT {safe_limit}
        """,
        (symbol,),
    )

    return [row_to_dict(row) for row in rows]

def fetch_alert_rows(symbol: str, limit: int = 20) -> list[dict]:
    safe_limit = max(1, min(int(limit), 100))
    sess = get_session()

    rows = sess.execute(
        f"""
        SELECT
            symbol,
            alert_time,
            alert_id,
            event_time,
            prediction_time,
            model_name,
            alert_type,
            severity,
            predicted_direction,
            probability_down,
            probability_up,
            confidence,
            message,
            source
        FROM alerts_by_symbol
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

@app.get("/api/v1/predictions/latest/{symbol}")
def latest_predictions(
    symbol: str,
    limit: int = Query(default=20, ge=1, le=100),
):
    normalized_symbol = symbol.upper()
    items = fetch_prediction_rows(normalized_symbol, int(limit))

    return {
        "symbol": normalized_symbol,
        "count": len(items),
        "items": items,
    }

@app.get("/api/v1/alerts/latest/{symbol}")
def latest_alerts(
    symbol: str,
    limit: int = Query(default=20, ge=1, le=100),
):
    normalized_symbol = symbol.upper()
    items = fetch_alert_rows(normalized_symbol, int(limit))

    return {
        "symbol": normalized_symbol,
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
        "predictions": fetch_prediction_rows(normalized_symbol, 10),
        "alerts": fetch_alert_rows(normalized_symbol, 10),
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }

@app.get("/api/v1/system/summary/{symbol}")
def system_summary(
    symbol: str,
    limit: int = Query(default=20, ge=1, le=100),
):
    normalized_symbol = symbol.upper()

    market = fetch_market_rows(normalized_symbol, limit)
    news = fetch_news_rows(normalized_symbol, limit)
    predictions = fetch_prediction_rows(normalized_symbol, limit)
    alerts = fetch_alert_rows(normalized_symbol, limit)

    latest_market = market[0] if market else None
    latest_news = news[0] if news else None
    latest_prediction = predictions[0] if predictions else None
    latest_alert = alerts[0] if alerts else None

    latest_prediction_confidence = None
    latest_prediction_direction = None

    if latest_prediction:
        latest_prediction_direction = latest_prediction.get("predicted_direction")

        if latest_prediction_direction == 1:
            latest_prediction_confidence = latest_prediction.get("probability_up")
        elif latest_prediction_direction == 0:
            latest_prediction_confidence = latest_prediction.get("probability_down")

    return {
        "symbol": normalized_symbol,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "window_limit": limit,
        "counts": {
            "market_rows": len(market),
            "news_rows": len(news),
            "prediction_rows": len(predictions),
            "alert_rows": len(alerts),
        },
        "market": {
            "latest_event_time": latest_market.get("event_time") if latest_market else None,
            "latest_ingest_time": latest_market.get("ingest_time") if latest_market else None,
            "latest_spark_process_time": latest_market.get("spark_process_time") if latest_market else None,
            "latest_source": latest_market.get("source") if latest_market else None,
            "latest_market_price": latest_market.get("market_price") if latest_market else None,
            "avg_ingest_latency_seconds": average_numeric(market, "ingest_latency_seconds"),
            "event_time_lag_seconds": seconds_since(latest_market.get("event_time")) if latest_market else None,
            "ingest_time_freshness_seconds": seconds_since(latest_market.get("ingest_time")) if latest_market else None,
            "spark_process_freshness_seconds": seconds_since(latest_market.get("spark_process_time")) if latest_market else None,
        },
        "news": {
            "latest_event_time": latest_news.get("event_time") if latest_news else None,
            "latest_ingest_time": latest_news.get("ingest_time") if latest_news else None,
            "latest_spark_process_time": latest_news.get("spark_process_time") if latest_news else None,
            "avg_ingest_latency_seconds": average_numeric(news, "ingest_latency_seconds"),
            "event_time_lag_seconds": seconds_since(latest_news.get("event_time")) if latest_news else None,
            "ingest_time_freshness_seconds": seconds_since(latest_news.get("ingest_time")) if latest_news else None,
        },
        "prediction": {
            "latest_event_time": latest_prediction.get("event_time") if latest_prediction else None,
            "latest_prediction_time": latest_prediction.get("prediction_time") if latest_prediction else None,
            "model_name": latest_prediction.get("model_name") if latest_prediction else None,
            "predicted_direction": latest_prediction_direction,
            "confidence": latest_prediction_confidence,
            "prediction_freshness_seconds": seconds_since(latest_prediction.get("prediction_time")) if latest_prediction else None,
        },
        "alert": {
            "latest_alert_time": latest_alert.get("alert_time") if latest_alert else None,
            "latest_severity": latest_alert.get("severity") if latest_alert else None,
            "latest_confidence": latest_alert.get("confidence") if latest_alert else None,
            "latest_message": latest_alert.get("message") if latest_alert else None,
            "alert_freshness_seconds": seconds_since(latest_alert.get("alert_time")) if latest_alert else None,
        },
        "note": (
            "For historical replay data, event_time_lag_seconds can be large because "
            "the source market event is historical. Ingest and Spark freshness are "
            "better indicators of pipeline freshness during replay."
        ),
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
                "predictions": fetch_prediction_rows(normalized_symbol, 10),
                "alerts": fetch_alert_rows(normalized_symbol, 10),
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
