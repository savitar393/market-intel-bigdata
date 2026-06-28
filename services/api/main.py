import asyncio
import json
import os
from pathlib import Path
from datetime import date, datetime, timezone
from typing import Any

from cassandra.cluster import Cluster
from dotenv import load_dotenv
from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

import time

from fastapi import Request, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

load_dotenv()

CASSANDRA_HOSTS = [
    h.strip()
    for h in os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
    if h.strip()
]
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "market_intel")

MONITOR_SYMBOLS = [
    s.strip().upper()
    for s in os.getenv("MONITOR_SYMBOLS", "AAPL,MSFT,NVDA,AMZN,TSLA,BTC-USD").split(",")
    if s.strip()
]
METRICS_REFRESH_SECONDS = float(os.getenv("METRICS_REFRESH_SECONDS", "10"))

CLASSIFICATION_METRICS_PATH = Path(
    os.getenv(
        "CLASSIFICATION_METRICS_PATH",
        "data/model_artifacts/baseline_models/model_metrics.json",
    )
)

DAILY_TREND_PATH = Path(
    os.getenv(
        "DAILY_TREND_PATH",
        "data/features/daily_stock_trend.json",
    )
)

HTTP_REQUESTS_TOTAL = Counter(
    "market_intel_api_requests_total",
    "Total FastAPI HTTP requests.",
    ["method", "path", "status_code"],
)

HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "market_intel_api_request_duration_seconds",
    "FastAPI HTTP request duration in seconds.",
    ["method", "path"],
)

WEBSOCKET_ACTIVE_CONNECTIONS = Gauge(
    "market_intel_websocket_active_connections",
    "Active WebSocket connections.",
    ["endpoint", "symbol"],
)

MARKET_ROWS_WINDOW = Gauge(
    "market_intel_market_rows_window",
    "Market rows returned in the latest monitoring window.",
    ["symbol"],
)

NEWS_ROWS_WINDOW = Gauge(
    "market_intel_news_rows_window",
    "News rows returned in the latest monitoring window.",
    ["symbol"],
)

PREDICTION_ROWS_WINDOW = Gauge(
    "market_intel_prediction_rows_window",
    "Prediction rows returned in the latest monitoring window.",
    ["symbol"],
)

ALERT_ROWS_WINDOW = Gauge(
    "market_intel_alert_rows_window",
    "Alert rows returned in the latest monitoring window.",
    ["symbol"],
)

MARKET_AVG_INGEST_LATENCY_SECONDS = Gauge(
    "market_intel_market_avg_ingest_latency_seconds",
    "Average market ingest latency in the latest monitoring window.",
    ["symbol"],
)

NEWS_AVG_INGEST_LATENCY_SECONDS = Gauge(
    "market_intel_news_avg_ingest_latency_seconds",
    "Average news ingest latency in the latest monitoring window.",
    ["symbol"],
)

MARKET_INGEST_FRESHNESS_SECONDS = Gauge(
    "market_intel_market_ingest_freshness_seconds",
    "Seconds since latest market ingest time.",
    ["symbol"],
)

NEWS_INGEST_FRESHNESS_SECONDS = Gauge(
    "market_intel_news_ingest_freshness_seconds",
    "Seconds since latest news ingest time.",
    ["symbol"],
)

PREDICTION_FRESHNESS_SECONDS = Gauge(
    "market_intel_prediction_freshness_seconds",
    "Seconds since latest prediction time.",
    ["symbol"],
)

ALERT_FRESHNESS_SECONDS = Gauge(
    "market_intel_alert_freshness_seconds",
    "Seconds since latest alert time.",
    ["symbol"],
)

LATEST_PREDICTION_CONFIDENCE = Gauge(
    "market_intel_latest_prediction_confidence",
    "Latest model prediction confidence.",
    ["symbol"],
)

LATEST_ALERT_CONFIDENCE = Gauge(
    "market_intel_latest_alert_confidence",
    "Latest alert confidence.",
    ["symbol"],
)

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

@app.middleware("http")
async def prometheus_http_middleware(request: Request, call_next):
    start_time = time.perf_counter()

    response = await call_next(request)

    duration = time.perf_counter() - start_time
    path = request.url.path

    HTTP_REQUESTS_TOTAL.labels(
        method=request.method,
        path=path,
        status_code=str(response.status_code),
    ).inc()

    HTTP_REQUEST_DURATION_SECONDS.labels(
        method=request.method,
        path=path,
    ).observe(duration)

    return response

def get_session():
    global cluster, session

    if session is None:
        cluster = Cluster(CASSANDRA_HOSTS, port=CASSANDRA_PORT)
        session = cluster.connect(CASSANDRA_KEYSPACE)

    return session


def read_json_file(path: Path, fallback):
    if not path.exists():
        return fallback

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"Failed to read JSON file {path}: {exc}")
        return fallback


def normalize_model_label(model_name: str) -> str:
    return (
        model_name
        .replace("_", " ")
        .replace("gradient boosted trees", "GBT")
        .title()
        .replace("Gbt", "GBT")
    )

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

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    now = datetime.now(timezone.utc)
    diff = (now - dt).total_seconds()

    # Some replay/live-provider timestamps can be ahead because of timezone or provider clock differences.
    # Clamp to zero so Grafana/system-analysis does not show negative freshness.
    return max(diff, 0.0)


def average_numeric(items: list[dict], key: str) -> float | None:
    values = [
        float(item[key])
        for item in items
        if item.get(key) is not None
    ]

    if not values:
        return None

    return sum(values) / len(values)

def initialize_websocket_metrics():
    for endpoint in ["market", "news", "live"]:
        for symbol in MONITOR_SYMBOLS:
            WEBSOCKET_ACTIVE_CONNECTIONS.labels(
                endpoint=endpoint,
                symbol=symbol,
            ).set(0)


def websocket_inc(endpoint: str, symbol: str):
    WEBSOCKET_ACTIVE_CONNECTIONS.labels(
        endpoint=endpoint,
        symbol=symbol,
    ).inc()


def websocket_dec(endpoint: str, symbol: str):
    WEBSOCKET_ACTIVE_CONNECTIONS.labels(
        endpoint=endpoint,
        symbol=symbol,
    ).dec()

def update_symbol_prometheus_metrics(symbol: str):
    market = fetch_market_rows(symbol, 20)
    news = fetch_news_rows(symbol, 20)
    predictions = fetch_prediction_rows(symbol, 20)
    alerts = fetch_alert_rows(symbol, 20)

    latest_market = market[0] if market else None
    latest_news = news[0] if news else None
    latest_prediction = predictions[0] if predictions else None
    latest_alert = alerts[0] if alerts else None

    MARKET_ROWS_WINDOW.labels(symbol=symbol).set(len(market))
    NEWS_ROWS_WINDOW.labels(symbol=symbol).set(len(news))
    PREDICTION_ROWS_WINDOW.labels(symbol=symbol).set(len(predictions))
    ALERT_ROWS_WINDOW.labels(symbol=symbol).set(len(alerts))

    market_avg_latency = average_numeric(market, "ingest_latency_seconds")
    news_avg_latency = average_numeric(news, "ingest_latency_seconds")

    if market_avg_latency is not None:
        MARKET_AVG_INGEST_LATENCY_SECONDS.labels(symbol=symbol).set(market_avg_latency)

    if news_avg_latency is not None:
        NEWS_AVG_INGEST_LATENCY_SECONDS.labels(symbol=symbol).set(news_avg_latency)

    if latest_market:
        market_freshness = seconds_since(latest_market.get("ingest_time"))
        if market_freshness is not None:
            MARKET_INGEST_FRESHNESS_SECONDS.labels(symbol=symbol).set(market_freshness)

    if latest_news:
        news_freshness = seconds_since(latest_news.get("ingest_time"))
        if news_freshness is not None:
            NEWS_INGEST_FRESHNESS_SECONDS.labels(symbol=symbol).set(news_freshness)

    if latest_prediction:
        prediction_freshness = seconds_since(latest_prediction.get("prediction_time"))
        if prediction_freshness is not None:
            PREDICTION_FRESHNESS_SECONDS.labels(symbol=symbol).set(prediction_freshness)

        predicted_direction = latest_prediction.get("predicted_direction")
        confidence = None

        if predicted_direction == 1:
            confidence = latest_prediction.get("probability_up")
        elif predicted_direction == 0:
            confidence = latest_prediction.get("probability_down")

        if confidence is not None:
            LATEST_PREDICTION_CONFIDENCE.labels(symbol=symbol).set(float(confidence))

    if latest_alert:
        alert_freshness = seconds_since(latest_alert.get("alert_time"))
        if alert_freshness is not None:
            ALERT_FRESHNESS_SECONDS.labels(symbol=symbol).set(alert_freshness)

        if latest_alert.get("confidence") is not None:
            LATEST_ALERT_CONFIDENCE.labels(symbol=symbol).set(float(latest_alert["confidence"]))

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

@app.get("/api/v1/evaluation/classification-summary")
def classification_summary():
    metrics = read_json_file(CLASSIFICATION_METRICS_PATH, [])

    items = []

    for item in metrics:
        if not isinstance(item, dict):
            continue

        items.append(
            {
                "model_name": item.get("model_name"),
                "model_label": normalize_model_label(str(item.get("model_name", ""))),
                "accuracy": item.get("accuracy"),
                "f1": item.get("f1"),
                "roc_auc": item.get("roc_auc"),
                "weighted_precision": item.get("weighted_precision"),
                "weighted_recall": item.get("weighted_recall"),
                "train_rows": item.get("train_rows"),
                "test_rows": item.get("test_rows"),
                "label_column": item.get("label_column"),
                "split_strategy": item.get("split_strategy"),
            }
        )

    return {
        "count": len(items),
        "source": str(CLASSIFICATION_METRICS_PATH),
        "items": items,
    }


@app.get("/api/v1/timeline/predictions/{symbol}")
def prediction_timeline(
    symbol: str,
    limit: int = Query(default=100, ge=1, le=100),
):
    normalized_symbol = symbol.upper()
    predictions = fetch_prediction_rows(normalized_symbol, int(limit))

    items = []

    for item in reversed(predictions):
        direction = item.get("predicted_direction")
        confidence = None

        if direction == 1:
            confidence = item.get("probability_up")
        elif direction == 0:
            confidence = item.get("probability_down")

        items.append(
            {
                "event_time": item.get("event_time"),
                "prediction_time": item.get("prediction_time"),
                "market_price": item.get("market_price"),
                "predicted_direction": direction,
                "prediction_label": "UP" if direction == 1 else "DOWN" if direction == 0 else "-",
                "probability_up": item.get("probability_up"),
                "probability_down": item.get("probability_down"),
                "confidence": confidence,
                "model_name": item.get("model_name"),
                "source": item.get("source"),
            }
        )

    return {
        "symbol": normalized_symbol,
        "count": len(items),
        "items": items,
    }


@app.get("/api/v1/daily-context/{symbol}")
def daily_context(
    symbol: str,
    limit: int = Query(default=252, ge=1, le=300),
):
    normalized_symbol = symbol.upper()
    payload = read_json_file(DAILY_TREND_PATH, {})

    items = payload.get(normalized_symbol, [])

    if not isinstance(items, list):
        items = []

    return {
        "symbol": normalized_symbol,
        "count": len(items[-limit:]),
        "source": str(DAILY_TREND_PATH),
        "items": items[-limit:],
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

@app.get("/metrics")
def prometheus_metrics():
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )

@app.websocket("/ws/market/{symbol}")
async def ws_market(
    websocket: WebSocket,
    symbol: str,
    interval_seconds: float = Query(default=2.0, ge=0.5, le=10.0),
):
    normalized_symbol = symbol.upper()

    await websocket.accept()
    websocket_inc("market", normalized_symbol)

    try:
        while True:
            payload = {
                "type": "market_snapshot",
                "symbol": normalized_symbol,
                "items": fetch_market_rows(normalized_symbol, 10),
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }

            await websocket.send_json(payload)
            await asyncio.sleep(interval_seconds)

    except WebSocketDisconnect:
        print(f"Market WebSocket disconnected: {normalized_symbol}")

    except Exception as exc:
        print(f"Market WebSocket error for {normalized_symbol}: {exc}")

    finally:
        websocket_dec("market", normalized_symbol)


@app.websocket("/ws/news/{symbol}")
async def ws_news(
    websocket: WebSocket,
    symbol: str,
    interval_seconds: float = Query(default=5.0, ge=1.0, le=30.0),
):
    normalized_symbol = symbol.upper()

    await websocket.accept()
    websocket_inc("news", normalized_symbol)

    try:
        while True:
            payload = {
                "type": "news_snapshot",
                "symbol": normalized_symbol,
                "items": fetch_news_rows(normalized_symbol, 10),
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }

            await websocket.send_json(payload)
            await asyncio.sleep(interval_seconds)

    except WebSocketDisconnect:
        print(f"News WebSocket disconnected: {normalized_symbol}")

    except Exception as exc:
        print(f"News WebSocket error for {normalized_symbol}: {exc}")

    finally:
        websocket_dec("news", normalized_symbol)


@app.websocket("/ws/live/{symbol}")
async def ws_live(
    websocket: WebSocket,
    symbol: str,
    interval_seconds: float = Query(default=2.0, ge=0.5, le=10.0),
):
    normalized_symbol = symbol.upper()

    await websocket.accept()
    websocket_inc("live", normalized_symbol)

    try:
        while True:
            payload = {
                "type": "dashboard_live_snapshot",
                "symbol": normalized_symbol,
                "market": fetch_market_rows(normalized_symbol, 10),
                "news": fetch_news_rows(normalized_symbol, 5),
                "predictions": fetch_prediction_rows(normalized_symbol, 10),
                "alerts": fetch_alert_rows(normalized_symbol, 10),
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }

            await websocket.send_json(payload)
            await asyncio.sleep(interval_seconds)

    except WebSocketDisconnect:
        print(f"Live WebSocket disconnected: {normalized_symbol}")

    except Exception as exc:
        print(f"Live WebSocket error for {normalized_symbol}: {exc}")

    finally:
        websocket_dec("live", normalized_symbol)


async def refresh_prometheus_metrics_loop():
    while True:
        for symbol in MONITOR_SYMBOLS:
            try:
                update_symbol_prometheus_metrics(symbol)
            except Exception as exc:
                print(f"Failed to update Prometheus metrics for {symbol}: {exc}")

        await asyncio.sleep(METRICS_REFRESH_SECONDS)


@app.on_event("startup")
async def startup_event():
    initialize_websocket_metrics()
    app.state.metrics_task = asyncio.create_task(refresh_prometheus_metrics_loop())

@app.on_event("shutdown")
def shutdown_event():
    global cluster

    metrics_task = getattr(app.state, "metrics_task", None)
    if metrics_task is not None:
        metrics_task.cancel()

    if cluster is not None:
        cluster.shutdown()
