# Distributed Deployment Topology

This document describes how the **Market Intel Big Data** system can be distributed across 2–3 machines for the final project demonstration.

The system is designed to support both:

1. Local development on one WSL2 machine.
2. Distributed deployment across multiple machines.

---

## 1. Local Development Mode

During development, all services can run on one WSL2 machine.

```text
WSL2 / Local Machine
├── Python Producers
│   ├── Databento historical replay
│   ├── yfinance WebSocket producer
│   ├── Finnhub REST news producer
│   └── Finnhub WebSocket producer
├── Kafka
├── Spark jobs
├── Cassandra
├── HDFS
├── FastAPI
├── React Dashboard
├── Prometheus
├── Grafana
└── MLflow
```

This mode is useful for implementation, testing, debugging, and presentation rehearsal.

---

## 2. Recommended 3-Machine Deployment

For the final project demonstration, the system can be distributed into three machines.

```text
Machine 1 — Ingestion and Processing
├── Python Producers
│   ├── Databento historical replay
│   ├── yfinance WebSocket producer
│   ├── Finnhub REST news producer
│   └── Finnhub WebSocket producer
└── Spark Jobs
    ├── stream_market_ticks_parquet.py
    ├── stream_news_events_parquet.py
    ├── stream_market_ticks_cassandra.py
    ├── stream_news_events_cassandra.py
    ├── build_market_news_features.py
    ├── train_spark_baseline_models.py
    └── train_spark_regression_models.py

Machine 2 — Broker, Storage, and Model Tracking
├── Kafka
├── Cassandra
├── HDFS NameNode
├── HDFS DataNode
└── MLflow Tracking Server

Machine 3 — Serving, Dashboard, and Monitoring
├── FastAPI REST + WebSocket API
├── React Dashboard
├── Prometheus
└── Grafana
```

---

## 3. Network Flow

Main data flow:

```text
External Data Providers
        ↓
Machine 1 Producers
        ↓
Machine 2 Kafka
        ↓
Machine 1 Spark Jobs
        ↓
Machine 2 Cassandra + HDFS
        ↓
Machine 3 FastAPI
        ↓
Machine 3 React Dashboard
```

Monitoring flow:

```text
FastAPI /metrics
        ↓
Prometheus
        ↓
Grafana
```

ML experiment tracking flow:

```text
Spark ML Training
        ↓
MLflow Tracking Server
```

---

## 4. 2-Machine Fallback Deployment

If only two machines are available, use this simpler distribution.

```text
Machine 1 — Ingestion and Processing
├── Python Producers
├── Spark jobs
└── Optional local development tools

Machine 2 — Infrastructure, Serving, and Monitoring
├── Kafka
├── Cassandra
├── HDFS
├── MLflow
├── FastAPI
├── React Dashboard
├── Prometheus
└── Grafana
```

This is less distributed than the 3-machine design, but it still separates the ingestion/processing layer from the infrastructure and serving layer.

---

## 5. Machine Roles

### 5.1 Machine 1 — Ingestion and Processing

Machine 1 is responsible for collecting and processing data.

Main responsibilities:

* Run market data producers.
* Run news data producers.
* Run Spark Structured Streaming jobs.
* Build training features.
* Train classification and regression models.
* Load predictions to Cassandra.
* Generate prediction alerts.

Important services/scripts:

```text
services/producers/databento_replay_producer.py
services/producers/yfinance_live_producer.py
services/producers/finnhub_news_producer.py
services/producers/finnhub_ws_producer.py

spark/jobs/stream_market_ticks_cassandra.py
spark/jobs/stream_news_events_cassandra.py
spark/jobs/build_market_news_features.py

ml/training/train_spark_baseline_models.py
ml/training/train_spark_regression_models.py
ml/evaluation/select_champion_model.py
ml/serving/load_predictions_to_cassandra.py

services/alerts/generate_prediction_alerts.py
```

---

### 5.2 Machine 2 — Broker, Storage, and Model Tracking

Machine 2 stores and manages the core big data infrastructure.

Main responsibilities:

* Receive events through Kafka.
* Store low-latency serving data in Cassandra.
* Store analytical/archive data in HDFS.
* Store ML experiment logs in MLflow.

Main services:

```text
Kafka
Cassandra
HDFS NameNode
HDFS DataNode
MLflow
```

---

### 5.3 Machine 3 — Serving, Dashboard, and Monitoring

Machine 3 exposes the processed results to users and provides monitoring.

Main responsibilities:

* Serve REST API and WebSocket data through FastAPI.
* Display market/news/prediction/alert data through React.
* Expose system metrics through FastAPI `/metrics`.
* Scrape metrics with Prometheus.
* Visualize operational metrics with Grafana.

Main services:

```text
FastAPI
React Dashboard
Prometheus
Grafana
```

---

## 6. Environment Variable Examples

### 6.1 Machine 1 — Processing Node

Machine 1 connects to Kafka, Cassandra, HDFS, and MLflow on Machine 2.

```env
KAFKA_BOOTSTRAP_SERVERS=<MACHINE_2_IP>:9092
KAFKA_MARKET_TOPIC=raw_market_ticks
KAFKA_NEWS_TOPIC=raw_news_events

CASSANDRA_HOSTS=<MACHINE_2_IP>
CASSANDRA_PORT=9042
CASSANDRA_KEYSPACE=market_intel

HDFS_NAMENODE=hdfs://<MACHINE_2_IP>:9000

MLFLOW_TRACKING_URI=http://<MACHINE_2_IP>:5000
MLFLOW_EXPERIMENT_NAME=market-intel-baselines

DATABENTO_API_KEY=your_databento_api_key
DATABENTO_DATASET=XNAS.ITCH
DATABENTO_SCHEMA=ohlcv-1m
DATABENTO_SYMBOLS=AAPL,MSFT,NVDA
DATABENTO_START=2024-05-20T13:30
DATABENTO_END=2024-05-24T20:00

FINNHUB_API_KEY=your_finnhub_api_key
FINNHUB_WS_SYMBOLS=AAPL,MSFT,NVDA,BINANCE:BTCUSDT
FINNHUB_WS_MAX_MESSAGES=20
```

---

### 6.2 Machine 2 — Storage and Broker Node

Machine 2 hosts Kafka, Cassandra, HDFS, and MLflow.

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

CASSANDRA_HOSTS=localhost
CASSANDRA_PORT=9042
CASSANDRA_KEYSPACE=market_intel

MLFLOW_TRACKING_URI=http://localhost:5000
MLFLOW_EXPERIMENT_NAME=market-intel-baselines
```

---

### 6.3 Machine 3 — Serving and Monitoring Node

Machine 3 connects to Cassandra and MLflow on Machine 2.

```env
CASSANDRA_HOSTS=<MACHINE_2_IP>
CASSANDRA_PORT=9042
CASSANDRA_KEYSPACE=market_intel

MONITOR_SYMBOLS=AAPL,MSFT,NVDA,BTC-USD
METRICS_REFRESH_SECONDS=10

MLFLOW_TRACKING_URI=http://<MACHINE_2_IP>:5000
MLFLOW_EXPERIMENT_NAME=market-intel-baselines

VITE_API_BASE=http://<MACHINE_3_IP>:8000
VITE_WS_BASE=ws://<MACHINE_3_IP>:8000
```

---

## 7. Required Open Ports

| Service           | Port | Host Machine | Purpose                                     |
| ----------------- | ---: | ------------ | ------------------------------------------- |
| Kafka             | 9092 | Machine 2    | Producers and Spark jobs send/read events   |
| Cassandra         | 9042 | Machine 2    | Spark writes and FastAPI reads serving data |
| HDFS NameNode RPC | 9000 | Machine 2    | HDFS write/read access                      |
| HDFS NameNode UI  | 9870 | Machine 2    | HDFS web interface                          |
| MLflow            | 5000 | Machine 2    | ML experiment tracking                      |
| FastAPI           | 8000 | Machine 3    | REST and WebSocket API                      |
| React Dashboard   | 5173 | Machine 3    | User-facing dashboard                       |
| Prometheus        | 9090 | Machine 3    | Metrics scraping and storage                |
| Grafana           | 3000 | Machine 3    | Monitoring dashboard                        |

---

## 8. Deployment Order

### Step 1 — Start Machine 2 Infrastructure

On Machine 2:

```bash
docker compose -f infra/docker/compose.local.yml up -d --build kafka cassandra hadoop-namenode hadoop-datanode mlflow
make topics
make schema
make hdfs-init
```

Check services:

```bash
docker ps
make hdfs-ls
curl http://localhost:5000
```

Expected services:

```text
market_kafka
market_cassandra
market_hadoop_namenode
market_hadoop_datanode
market_mlflow
```

---

### Step 2 — Start Machine 3 Serving Layer

On Machine 3:

```bash
docker compose -f infra/docker/compose.local.yml up -d --build api dashboard prometheus grafana
```

Check API:

```bash
curl http://localhost:8000/api/v1/health
curl http://localhost:8000/metrics | grep market_intel | head
```

Open in browser:

```text
React Dashboard: http://<MACHINE_3_IP>:5173
FastAPI Docs:    http://<MACHINE_3_IP>:8000/docs
Prometheus:      http://<MACHINE_3_IP>:9090/targets
Grafana:         http://<MACHINE_3_IP>:3000
```

---

### Step 3 — Start Machine 1 Ingestion and Spark Processing

On Machine 1:

```bash
source .venv/bin/activate
```

Run market replay producer:

```bash
python services/producers/databento_replay_producer.py --speed 3600
```

Run yfinance live producer:

```bash
python services/producers/yfinance_live_producer.py \
  --symbols AAPL,MSFT,NVDA,AMZN,BTC-USD \
  --max-messages 20
```

Run Finnhub news producer:

```bash
python services/producers/finnhub_news_producer.py \
  --symbols AAPL,MSFT,NVDA \
  --max-events-per-symbol 5 \
  --once
```

Run Finnhub WebSocket fallback producer:

```bash
python services/producers/finnhub_ws_producer.py \
  --symbols AAPL,MSFT,NVDA,BINANCE:BTCUSDT \
  --max-messages 20
```

Run Spark market-to-Cassandra job:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  spark/jobs/stream_market_ticks_cassandra.py
```

Run Spark news-to-Cassandra job:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  spark/jobs/stream_news_events_cassandra.py
```

---

## 9. ML Workflow

After market and news Parquet data are available, build features:

```bash
spark-submit spark/jobs/build_market_news_features.py
```

Train classification models:

```bash
spark-submit ml/training/train_spark_baseline_models.py
```

Select champion model:

```bash
python ml/evaluation/select_champion_model.py
```

Train regression models:

```bash
spark-submit ml/training/train_spark_regression_models.py
```

Log results to MLflow:

```bash
python ml/tracking/log_mlflow_runs.py --all
```

Optional: log model artifacts too.

```bash
python ml/tracking/log_mlflow_runs.py --all --log-model-artifacts
```

Load predictions to Cassandra:

```bash
python ml/serving/load_predictions_to_cassandra.py
```

Generate alerts:

```bash
python services/alerts/generate_prediction_alerts.py \
  --symbols AAPL,MSFT,NVDA \
  --threshold 0.55
```

---

## 10. HDFS Archive Workflow

Initialize HDFS directories:

```bash
make hdfs-init
```

Archive local Parquet outputs to HDFS:

```bash
make archive-hdfs
```

List HDFS archive:

```bash
make hdfs-ls
```

Expected HDFS layout:

```text
/market-intel/raw/market/market_ticks
/market-intel/raw/news/news_events
/market-intel/features/model_training_dataset
/market-intel/model-artifacts
```

HDFS UI:

```text
http://<MACHINE_2_IP>:9870
```

---

## 11. Demo Verification

From Machine 3, test REST API outputs:

```bash
curl "http://localhost:8000/api/v1/market/latest/AAPL?limit=5" | python -m json.tool
curl "http://localhost:8000/api/v1/news/latest/AAPL?limit=5" | python -m json.tool
curl "http://localhost:8000/api/v1/predictions/latest/AAPL?limit=5" | python -m json.tool
curl "http://localhost:8000/api/v1/alerts/latest/AAPL?limit=5" | python -m json.tool
curl "http://localhost:8000/api/v1/system/summary/AAPL?limit=20" | python -m json.tool
```

Open browser dashboards:

```text
React Dashboard: http://<MACHINE_3_IP>:5173
FastAPI Docs:    http://<MACHINE_3_IP>:8000/docs
Grafana:         http://<MACHINE_3_IP>:3000
Prometheus:      http://<MACHINE_3_IP>:9090/targets
MLflow:          http://<MACHINE_2_IP>:5000
HDFS UI:         http://<MACHINE_2_IP>:9870
```

---

## 12. System Analysis Metrics

The system supports operational analysis through FastAPI, Prometheus, and Grafana.

Important monitored metrics include:

```text
API request count
API request duration
Active WebSocket connections
Market ingest latency
News ingest latency
Market freshness
Prediction freshness
Alert freshness
Prediction confidence
Alert count
```

Useful API endpoint:

```bash
curl "http://localhost:8000/api/v1/system/summary/AAPL?limit=20" | python -m json.tool
```

Useful Prometheus endpoint:

```bash
curl http://localhost:8000/metrics | grep market_intel | head
```

Useful Grafana dashboard:

```text
Dashboards → Market Intel → Market Intel - System Overview
```

---

## 13. Notes

The distributed design separates system responsibilities:

```text
Machine 1 = ingestion and compute
Machine 2 = broker, storage, and model tracking
Machine 3 = API, dashboard, and monitoring
```

This improves the final project architecture because it demonstrates:

1. Distributed data ingestion.
2. Stream processing.
3. Low-latency serving storage.
4. Long-term HDFS archive storage.
5. REST and WebSocket serving.
6. User-facing dashboard.
7. System monitoring.
8. ML experiment tracking.
9. Alert generation and optional external notification.

---

## 14. Limitations

This is an academic prototype.

Known limitations:

1. yfinance is used for educational/demo live data.
2. Finnhub access depends on API account entitlement.
3. Databento historical access depends on trial balance.
4. Historical replay means market `event_time` can be old.
5. During replay, `ingest_time` and `spark_process_time` are better indicators of pipeline freshness.
6. Current ML models are baseline models and should not be interpreted as financial advice.
7. The distributed topology may require firewall and Docker network adjustments depending on the machines used.
