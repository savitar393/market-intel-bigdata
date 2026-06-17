# Market Intel Big Data

Real-time multimodal stock intelligence system using historical market replay, live market ticks, company news, Spark feature engineering, ML prediction, Cassandra serving storage, FastAPI REST/WebSocket APIs, React dashboard, alerts, and Prometheus/Grafana monitoring.

## 1. Project overview

This project demonstrates an end-to-end big data architecture for short-horizon stock intelligence.

The current pipeline supports:

```text
Databento historical market replay
yfinance live WebSocket ticks
Finnhub company news
        ↓
Kafka topics
        ↓
Spark Structured Streaming
        ↓
Parquet / Cassandra
        ↓
Spark ML feature building and model training
        ↓
Predictions and alerts
        ↓
FastAPI REST + WebSocket gateway
        ↓
React dashboard
        ↓
Prometheus + Grafana monitoring
```

Main use case:

```text
For selected symbols such as AAPL, MSFT, NVDA, AMZN, and BTC-USD,
the system ingests market/news data, builds features, trains ML models,
serves predictions and alerts, and visualizes both business output and
system performance.
```

This is an academic/demo system, not a financial trading system.

## 2. Current technology stack

| Layer                     | Technology                                                                                          |
| ------------------------- | --------------------------------------------------------------------------------------------------- |
| Market historical data    | Databento historical API                                                                            |
| Live market data          | yfinance WebSocket                                                                                  |
| News data                 | Finnhub company news                                                                                |
| Message broker            | Apache Kafka                                                                                        |
| Stream processing         | Apache Spark Structured Streaming / PySpark                                                         |
| Batch feature engineering | PySpark                                                                                             |
| ML models                 | Spark MLlib Logistic Regression, Random Forest, GBT, Linear Regression, RF Regressor, GBT Regressor |
| Serving database          | Cassandra                                                                                           |
| Backend API               | FastAPI                                                                                             |
| Live browser updates      | FastAPI WebSocket                                                                                   |
| Dashboard                 | React Vite + Recharts                                                                               |
| Monitoring                | Prometheus + Grafana                                                                                |
| Notification              | Telegram bot sender                                                                                 |
| Local orchestration       | Docker Compose                                                                                      |

## 3. Repository structure

Expected important folders:

```text
.
├── data/
│   ├── samples/
│   ├── processed/
│   ├── features/
│   └── model_artifacts/
├── docs/
├── infra/
│   ├── cassandra/
│   ├── docker/
│   ├── grafana/
│   └── prometheus/
├── ml/
│   ├── evaluation/
│   ├── serving/
│   └── training/
├── services/
│   ├── alerts/
│   ├── api/
│   ├── consumers/
│   ├── dashboard/
│   ├── notifications/
│   ├── producers/
│   └── storage/
└── spark/
    └── jobs/
```

Generated runtime data should not be committed:

```text
data/checkpoints/
data/processed/
data/features/
data/model_artifacts/
spark-warehouse/
metastore_db/
derby.log
```

## 4. Prerequisites

Recommended development environment:

```text
WSL2 Ubuntu
Python 3.11 or 3.12
Java 17
Docker Desktop with WSL integration
Node.js and npm
Git
```

Check versions:

```bash
python --version
java -version
docker --version
docker compose version
node -v
npm -v
```

Install Java if needed:

```bash
sudo apt update
sudo apt install -y openjdk-17-jdk
```

## 5. Python setup

From repository root:

```bash
cd ~/dev/market-intel-bigdata

python3 -m venv .venv
source .venv/bin/activate

pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

If PySpark ML fails with `No module named 'distutils'`, run:

```bash
pip install --upgrade setuptools wheel
python -c "import distutils; print('distutils ok')"
```

## 6. Environment variables

Copy the example environment file:

```bash
cp .env.example .env
nano .env
```

Minimum useful local variables:

```env
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_MARKET_TOPIC=raw_market_ticks
KAFKA_NEWS_TOPIC=raw_news_events

# Databento
DATABENTO_API_KEY=your_databento_api_key
DATABENTO_DATASET=XNAS.ITCH
DATABENTO_SCHEMA=ohlcv-1m
DATABENTO_SYMBOLS=AAPL,MSFT,NVDA
DATABENTO_START=2024-05-20T13:30
DATABENTO_END=2024-05-24T20:00

# Finnhub
FINNHUB_API_KEY=your_finnhub_api_key

# Cassandra
CASSANDRA_HOSTS=localhost
CASSANDRA_PORT=9042
CASSANDRA_KEYSPACE=market_intel

# Monitoring
MONITOR_SYMBOLS=AAPL,MSFT,NVDA,BTC-USD
METRICS_REFRESH_SECONDS=10

# Telegram, optional
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=your_telegram_chat_id
```

Never commit `.env`.

## 7. Start infrastructure with Docker Compose

Start the local stack:

```bash
docker compose -f infra/docker/compose.local.yml up -d --build
```

Check containers:

```bash
docker ps
```

Expected containers include:

```text
market_kafka
market_cassandra
market_api
market_dashboard
market_prometheus
market_grafana
```

Check logs:

```bash
docker logs market_kafka --tail 50
docker logs market_cassandra --tail 50
docker logs market_api --tail 50
docker logs market_dashboard --tail 50
docker logs market_prometheus --tail 50
docker logs market_grafana --tail 50
```

Cassandra can take 30–90 seconds to become ready.

## 8. Initialize Cassandra schema

Apply the Cassandra schema:

```bash
docker cp infra/cassandra/schema.cql market_cassandra:/schema.cql
docker exec -it market_cassandra cqlsh -f /schema.cql
```

Verify keyspace:

```bash
docker exec -it market_cassandra cqlsh -e "DESCRIBE KEYSPACE market_intel;"
```

Run Cassandra smoke test:

```bash
source .venv/bin/activate
python services/storage/check_cassandra.py
```

Expected:

```text
Cassandra smoke test passed.
```

## 9. Create Kafka topics

If Kafka topics are missing or Spark shows `UnknownTopicOrPartitionException`, recreate topics:

```bash
docker exec -it market_kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic raw_market_ticks \
  --partitions 3 \
  --replication-factor 1

docker exec -it market_kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic raw_news_events \
  --partitions 3 \
  --replication-factor 1
```

Verify:

```bash
docker exec -it market_kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list

docker exec -it market_kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic raw_market_ticks
```

## 10. Validate data sources

### 10.1 Validate Databento historical data

```bash
source .venv/bin/activate

python services/producers/validate_databento.py
```

Expected outputs:

```text
data/samples/databento_sample.csv
data/samples/databento_events.jsonl
```

### 10.2 Validate yfinance live WebSocket

```bash
python services/producers/validate_yfinance_live.py
```

Expected output:

```text
data/samples/yfinance_live_events.jsonl
```

### 10.3 Validate Finnhub quote/news

```bash
python services/producers/validate_finnhub.py
```

Expected outputs:

```text
data/samples/finnhub_quote_events.jsonl
data/samples/finnhub_news_events.jsonl
```

Some Finnhub endpoints may return `403` depending on account entitlement. Company news and quote should still be enough for the current pipeline.

## 11. Test Kafka producers and consumers

### 11.1 Run a market consumer

Terminal 1:

```bash
source .venv/bin/activate

KAFKA_GROUP_ID=print-market-consumer-$(date +%s) \
python services/consumers/print_market_consumer.py
```

### 11.2 Send Databento replay into Kafka

Terminal 2:

```bash
source .venv/bin/activate

python services/producers/databento_replay_producer.py \
  --input-jsonl data/samples/databento_events.jsonl \
  --speed 600
```

### 11.3 Send yfinance live ticks into Kafka

```bash
python services/producers/yfinance_live_producer.py \
  --symbols AAPL,MSFT,NVDA,AMZN,BTC-USD \
  --max-messages 20
```

Expected consumer output includes:

```text
type=market_bar source=databento_jsonl_replay
type=market_tick source=yfinance_websocket
```

### 11.4 Send Finnhub news into Kafka

Terminal 1:

```bash
KAFKA_GROUP_ID=print-news-consumer-$(date +%s) \
python services/consumers/print_news_consumer.py
```

Terminal 2:

```bash
python services/producers/finnhub_news_producer.py \
  --symbols AAPL,MSFT,NVDA \
  --max-events-per-symbol 5 \
  --once
```

## 12. Run Spark market stream to Parquet

Terminal 1:

```bash
source .venv/bin/activate

rm -rf data/checkpoints/spark_market_ticks_parquet
rm -rf data/processed/market_ticks

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  spark/jobs/stream_market_ticks_parquet.py
```

Terminal 2:

```bash
python services/producers/databento_replay_producer.py --speed 3600
```

After replay completes, wait 10–20 seconds, then stop Spark with `Ctrl+C`.

Check output:

```bash
find data/processed/market_ticks -type f | head
```

Read Parquet:

```bash
python - <<'PY'
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("check-market").master("local[*]").getOrCreate()
df = spark.read.parquet("data/processed/market_ticks")

print("Market rows:", df.count())
df.groupBy("symbol").count().show()
df.select("symbol", "event_time", "market_price", "volume", "source").orderBy("symbol", "event_time").show(20, truncate=False)

spark.stop()
PY
```

## 13. Run Spark news stream to Parquet

Terminal 1:

```bash
source .venv/bin/activate

rm -rf data/checkpoints/spark_news_events_parquet
rm -rf data/processed/news_events

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  spark/jobs/stream_news_events_parquet.py
```

Terminal 2:

```bash
python services/producers/finnhub_news_producer.py \
  --symbols AAPL,MSFT,NVDA \
  --max-events-per-symbol 5 \
  --once
```

Check output:

```bash
find data/processed/news_events -type f | head
```

Read Parquet:

```bash
python - <<'PY'
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("check-news").master("local[*]").getOrCreate()
df = spark.read.parquet("data/processed/news_events")

print("News rows:", df.count())
df.groupBy("symbol").count().show()
df.select("symbol", "event_time", "headline", "simple_sentiment_label", "simple_sentiment_score").show(20, truncate=80)

spark.stop()
PY
```

## 14. Write streams to Cassandra

### 14.1 Market stream to Cassandra

Terminal 1:

```bash
source .venv/bin/activate

rm -rf data/checkpoints/spark_market_ticks_cassandra

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  spark/jobs/stream_market_ticks_cassandra.py
```

Terminal 2:

```bash
python services/producers/databento_replay_producer.py \
  --input-jsonl data/samples/databento_events.jsonl \
  --speed 600
```

Check Cassandra:

```bash
docker exec -it market_cassandra cqlsh -e "SELECT symbol, event_time, event_type, market_price, source FROM market_intel.market_ticks_by_symbol WHERE symbol = 'AAPL' LIMIT 10;"
```

### 14.2 News stream to Cassandra

Terminal 1:

```bash
source .venv/bin/activate

rm -rf data/checkpoints/spark_news_events_cassandra

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  spark/jobs/stream_news_events_cassandra.py
```

Terminal 2:

```bash
python services/producers/finnhub_news_producer.py \
  --symbols AAPL,MSFT,NVDA \
  --max-events-per-symbol 5 \
  --once
```

Check Cassandra:

```bash
docker exec -it market_cassandra cqlsh -e "SELECT symbol, event_time, simple_sentiment_label, simple_sentiment_score, headline FROM market_intel.news_events_by_symbol WHERE symbol = 'AAPL' LIMIT 10;"
```

## 15. Build feature dataset

After market and news Parquet exist:

```bash
source .venv/bin/activate

rm -rf data/features/model_training_dataset

spark-submit spark/jobs/build_market_news_features.py
```

Check features:

```bash
python - <<'PY'
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("check-features").master("local[*]").getOrCreate()
df = spark.read.parquet("data/features/model_training_dataset")

print("Feature rows:", df.count())
df.groupBy("symbol").count().show()
df.select(
    "symbol",
    "event_minute",
    "market_price",
    "return_1",
    "return_10",
    "rolling_volatility_10",
    "volume_surprise_5",
    "target_next_return",
    "target_direction",
).show(30, truncate=False)

spark.stop()
PY
```

## 16. Train classification models

Classification target:

```text
target_direction
1 = next return is positive
0 = next return is not positive
```

Run:

```bash
source .venv/bin/activate

rm -rf data/model_artifacts/baseline_models

spark-submit ml/training/train_spark_baseline_models.py
```

Check metrics:

```bash
cat data/model_artifacts/baseline_models/model_metrics.json
```

Run champion selection:

```bash
python ml/evaluation/select_champion_model.py
cat docs/model_evaluation_summary.md
```

Current model family:

```text
Logistic Regression
Random Forest Classifier
Gradient-Boosted Trees Classifier
```

## 17. Train regression models

Regression target:

```text
target_next_return
```

Run:

```bash
source .venv/bin/activate

rm -rf data/model_artifacts/regression_models

spark-submit ml/training/train_spark_regression_models.py
```

Check metrics:

```bash
cat data/model_artifacts/regression_models/regression_model_metrics.json
```

Current model family:

```text
Linear Regression
Random Forest Regressor
Gradient-Boosted Tree Regressor
```

## 18. Load predictions to Cassandra

After classification training and champion selection:

```bash
source .venv/bin/activate

python ml/serving/load_predictions_to_cassandra.py
```

Check API after FastAPI is running:

```bash
curl "http://localhost:8000/api/v1/predictions/latest/AAPL?limit=5" | python -m json.tool
```

Or query Cassandra:

```bash
docker exec -it market_cassandra cqlsh -e "SELECT symbol, event_time, model_name, market_price, predicted_direction, probability_up, target_direction FROM market_intel.model_predictions_by_symbol WHERE symbol = 'AAPL' LIMIT 10;"
```

## 19. Generate prediction alerts

```bash
source .venv/bin/activate

python services/alerts/generate_prediction_alerts.py \
  --symbols AAPL,MSFT,NVDA \
  --threshold 0.55
```

Check API:

```bash
curl "http://localhost:8000/api/v1/alerts/latest/AAPL?limit=5" | python -m json.tool
```

Or query Cassandra:

```bash
docker exec -it market_cassandra cqlsh -e "SELECT symbol, alert_time, severity, confidence, message FROM market_intel.alerts_by_symbol WHERE symbol = 'AAPL' LIMIT 10;"
```

## 20. Optional: send Telegram notifications

Set in `.env`:

```env
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=your_telegram_chat_id
```

Dry-run first:

```bash
source .venv/bin/activate

python services/notifications/send_telegram_alerts.py \
  --symbols AAPL,MSFT,NVDA \
  --limit 3 \
  --dry-run
```

Send real Telegram messages:

```bash
python services/notifications/send_telegram_alerts.py \
  --symbols AAPL,MSFT,NVDA \
  --limit 3
```

Check notification log:

```bash
docker exec -it market_cassandra cqlsh -e "SELECT alert_id, symbol, sent_at, status FROM market_intel.telegram_notifications_by_alert LIMIT 10;"
```

Important: dry-run logs alert IDs too, so those same alerts may be skipped in a real send. Generate new alerts if needed.

## 21. Run FastAPI locally

For development:

```bash
source .venv/bin/activate

uvicorn services.api.main:app \
  --reload \
  --host 0.0.0.0 \
  --port 8000
```

Open:

```text
http://localhost:8000/docs
```

Useful REST endpoints:

```bash
curl http://localhost:8000/api/v1/health | python -m json.tool

curl "http://localhost:8000/api/v1/market/latest/AAPL?limit=5" | python -m json.tool

curl "http://localhost:8000/api/v1/news/latest/AAPL?limit=5" | python -m json.tool

curl "http://localhost:8000/api/v1/predictions/latest/AAPL?limit=5" | python -m json.tool

curl "http://localhost:8000/api/v1/alerts/latest/AAPL?limit=5" | python -m json.tool

curl "http://localhost:8000/api/v1/dashboard/snapshot/AAPL" | python -m json.tool

curl "http://localhost:8000/api/v1/system/summary/AAPL?limit=20" | python -m json.tool
```

Prometheus metrics endpoint:

```bash
curl http://localhost:8000/metrics | grep market_intel | head -n 30
```

## 22. Test WebSocket API

```bash
source .venv/bin/activate

python - <<'PY'
import asyncio
import json
import websockets

async def main():
    uri = "ws://localhost:8000/ws/live/AAPL?interval_seconds=2"
    async with websockets.connect(uri) as websocket:
        for _ in range(3):
            payload = json.loads(await websocket.recv())
            print(json.dumps(payload, indent=2)[:2500])

asyncio.run(main())
PY
```

Expected payload contains:

```text
market
news
predictions
alerts
generated_at
```

## 23. Run React dashboard locally

Terminal 1: run FastAPI.

```bash
source .venv/bin/activate

uvicorn services.api.main:app \
  --reload \
  --host 0.0.0.0 \
  --port 8000
```

Terminal 2: run React.

```bash
cd services/dashboard
npm install
npm run dev -- --host 0.0.0.0
```

Open:

```text
http://localhost:5173
```

Dashboard should show:

```text
Latest market price
Market price stream
Company news and sentiment
Latest model prediction
Prediction table
Prediction alerts
System performance summary
WebSocket connection status
```

## 24. Run API and dashboard with Docker

Build and run API/dashboard:

```bash
docker compose -f infra/docker/compose.local.yml up -d --build api dashboard
```

Open:

```text
http://localhost:8000/docs
http://localhost:5173
```

Test:

```bash
curl http://localhost:8000/api/v1/health | python -m json.tool
curl "http://localhost:8000/api/v1/dashboard/snapshot/AAPL" | python -m json.tool
```

## 25. Run Prometheus and Grafana

Start monitoring stack:

```bash
docker compose -f infra/docker/compose.local.yml up -d --build prometheus grafana
```

Open Prometheus:

```text
http://localhost:9090/targets
```

Open Grafana:

```text
http://localhost:3000
```

Default login:

```text
username: admin
password: admin
```

Grafana dashboard path:

```text
Dashboards → Market Intel → Market Intel - System Overview
```

Useful Prometheus queries:

```text
market_intel_api_requests_total
market_intel_api_request_duration_seconds_bucket
market_intel_market_avg_ingest_latency_seconds
market_intel_market_ingest_freshness_seconds
market_intel_prediction_freshness_seconds
market_intel_latest_prediction_confidence
market_intel_alert_rows_window
market_intel_alert_freshness_seconds
```

Curl Prometheus query:

```bash
curl "http://localhost:9090/api/v1/query?query=market_intel_api_requests_total" | python -m json.tool
```

## 26. Suggested full demo run order

Use this for presentation rehearsal.

### Step 1 — Start infrastructure

```bash
docker compose -f infra/docker/compose.local.yml up -d --build
docker ps
```

### Step 2 — Apply Cassandra schema

```bash
docker cp infra/cassandra/schema.cql market_cassandra:/schema.cql
docker exec -it market_cassandra cqlsh -f /schema.cql
```

### Step 3 — Confirm API and dashboard

```bash
curl http://localhost:8000/api/v1/health | python -m json.tool
```

Open:

```text
http://localhost:5173
http://localhost:3000
```

### Step 4 — Replay market data

Terminal 1:

```bash
rm -rf data/checkpoints/spark_market_ticks_cassandra

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  spark/jobs/stream_market_ticks_cassandra.py
```

Terminal 2:

```bash
python services/producers/databento_replay_producer.py --speed 3600
```

### Step 5 — Ingest news

Terminal 1:

```bash
rm -rf data/checkpoints/spark_news_events_cassandra

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  spark/jobs/stream_news_events_cassandra.py
```

Terminal 2:

```bash
python services/producers/finnhub_news_producer.py \
  --symbols AAPL,MSFT,NVDA \
  --max-events-per-symbol 5 \
  --once
```

### Step 6 — Show API output

```bash
curl "http://localhost:8000/api/v1/market/latest/AAPL?limit=5" | python -m json.tool
curl "http://localhost:8000/api/v1/news/latest/AAPL?limit=5" | python -m json.tool
curl "http://localhost:8000/api/v1/predictions/latest/AAPL?limit=5" | python -m json.tool
curl "http://localhost:8000/api/v1/alerts/latest/AAPL?limit=5" | python -m json.tool
```

### Step 7 — Show React dashboard

Open:

```text
http://localhost:5173
```

Switch symbols:

```text
AAPL
MSFT
NVDA
BTC-USD
```

### Step 8 — Show Grafana monitoring

Open:

```text
http://localhost:3000
```

Show:

```text
API requests
API latency
Market ingest latency
Market freshness
Prediction freshness
Alert freshness
Prediction confidence
```

## 27. Common troubleshooting

### 27.1 Spark says `UnknownTopicOrPartitionException`

Recreate Kafka topics:

```bash
docker exec -it market_kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete \
  --topic raw_market_ticks

sleep 5

docker exec -it market_kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic raw_market_ticks \
  --partitions 3 \
  --replication-factor 1
```

Also recreate news topic if needed:

```bash
docker exec -it market_kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic raw_news_events \
  --partitions 3 \
  --replication-factor 1
```

### 27.2 FastAPI cannot connect to Cassandra

Check Cassandra container:

```bash
docker ps
docker logs market_cassandra --tail 50
```

When running FastAPI inside Docker, Cassandra host should be:

```env
CASSANDRA_HOSTS=cassandra
```

When running FastAPI locally from WSL, Cassandra host should usually be:

```env
CASSANDRA_HOSTS=localhost
```

### 27.3 Dashboard cannot reach API

If running React locally:

```ts
VITE_API_BASE=http://localhost:8000
VITE_WS_BASE=ws://localhost:8000
```

If running through Docker Compose, rebuild dashboard after changing environment args:

```bash
docker compose -f infra/docker/compose.local.yml up -d --build dashboard
```

### 27.4 PySpark cannot import `distutils`

```bash
source .venv/bin/activate
pip install --upgrade setuptools wheel
python -c "import distutils; print('distutils ok')"
```

### 27.5 No prediction rows

Run:

```bash
spark-submit ml/training/train_spark_baseline_models.py
python ml/evaluation/select_champion_model.py
python ml/serving/load_predictions_to_cassandra.py
```

Then test:

```bash
curl "http://localhost:8000/api/v1/predictions/latest/AAPL?limit=5" | python -m json.tool
```

### 27.6 No alerts

Run:

```bash
python services/alerts/generate_prediction_alerts.py \
  --symbols AAPL,MSFT,NVDA \
  --threshold 0.55
```

Then test:

```bash
curl "http://localhost:8000/api/v1/alerts/latest/AAPL?limit=5" | python -m json.tool
```

### 27.7 Prometheus target is down

Open:

```text
http://localhost:9090/targets
```

Check API logs:

```bash
docker logs market_api --tail 100
```

Test metrics directly:

```bash
curl http://localhost:8000/metrics | head
```

Restart monitoring stack:

```bash
docker compose -f infra/docker/compose.local.yml up -d --build api prometheus grafana
```

## 28. Git workflow

Use `dev` as the integration branch:

```bash
git checkout dev
git pull origin dev
git checkout -b feat/your-feature-name
```

After finishing:

```bash
git status
git add .
git commit -m "Your commit message"
git push -u origin feat/your-feature-name

git checkout dev
git pull origin dev
git merge --no-ff feat/your-feature-name -m "Merge your feature into dev"
git push origin dev
```

## 29. Current demo checklist

Before presenting, verify:

```bash
docker ps
curl http://localhost:8000/api/v1/health | python -m json.tool
curl "http://localhost:8000/api/v1/dashboard/snapshot/AAPL" | python -m json.tool
curl "http://localhost:8000/api/v1/system/summary/AAPL?limit=20" | python -m json.tool
curl http://localhost:8000/metrics | grep market_intel | head
```

Open:

```text
http://localhost:5173
http://localhost:8000/docs
http://localhost:9090/targets
http://localhost:3000
```

The project is considered locally demo-ready when:

```text
React dashboard loads
WebSocket status is connected
AAPL/MSFT/NVDA show market data
News table shows company news
Prediction table shows model output
Alert panel shows generated alerts
System summary panel shows freshness/latency
Prometheus target for market-api is UP
Grafana dashboard shows metrics
```

## 30. Notes and limitations

This is an academic prototype.

Known limitations:

```text
1. yfinance is suitable for educational/demo use, not production trading.
2. Finnhub endpoint access depends on account entitlement.
3. Databento historical data depends on trial balance and selected dataset/schema.
4. Historical replay means event_time can be old; ingest_time and spark_process_time are better for pipeline freshness.
5. Current models are baseline models. They validate the ML pipeline but should not be interpreted as profitable trading signals.
6. Generated data and model artifacts are local runtime outputs and are intentionally ignored by Git.
```

Recommended next improvements:

```text
1. Add Kafka topic creation script under infra/kafka/.
2. Add MLflow experiment tracking.
3. Add HDFS archival path if required by the lecturer.
4. Add richer news sentiment model.
5. Add Finnhub WebSocket fallback.
6. Add optional NASA image feature proof-of-concept.
7. Package cluster/VM deployment instructions.
```
