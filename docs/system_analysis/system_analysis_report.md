# System Analysis Report

Generated at: `2026-06-30T10:40:02.014507+00:00`

## 1. Test Configuration

- API base: `http://localhost:8000`
- Symbols: `AAPL, MSFT, NVDA, AMZN, TSLA, BTC-USD`
- Iterations: `5`
- Started at: `2026-06-30T10:39:22.983090+00:00`
- Finished at: `2026-06-30T10:40:02.014507+00:00`
- Total measured requests: `185`
- Successful requests: `185`
- Approximate API request throughput during test: `4.82 requests/second`

## 2. API Latency Summary

| Endpoint | Samples | Avg ms | Median ms | P95 ms | Min ms | Max ms |
|---|---:|---:|---:|---:|---:|---:|
| alerts_latest | 30 | 5.32 | 5.10 | 7.09 | 3.54 | 8.20 |
| dashboard_snapshot | 30 | 10.03 | 9.78 | 11.71 | 8.47 | 13.15 |
| health | 5 | 5.56 | 5.23 | 5.37 | 5.14 | 6.89 |
| market_latest | 30 | 5.64 | 5.64 | 6.65 | 4.52 | 7.08 |
| news_latest | 30 | 5.32 | 5.09 | 7.64 | 4.05 | 7.92 |
| predictions_latest | 30 | 5.62 | 5.22 | 7.23 | 4.03 | 11.20 |
| system_summary | 30 | 10.24 | 9.36 | 12.00 | 7.58 | 29.74 |

## 3. Freshness and Data Availability Snapshot

| Symbol | Market Rows | News Rows | Prediction Rows | Alert Rows | Market Freshness s | Prediction Freshness s | Alert Freshness s | Model | Confidence |
|---|---:|---:|---:|---:|---:|---:|---:|---|---:|
| AAPL | 20 | 5 | 20 | 0 | 224815.41 | 246862.28 | - | logistic_regression | 0.4990 |
| MSFT | 20 | 5 | 20 | 0 | 224815.61 | 246862.48 | - | logistic_regression | 0.5338 |
| NVDA | 20 | 5 | 20 | 6 | 224815.83 | 246862.69 | 246835.33 | logistic_regression | 0.5711 |
| AMZN | 20 | 0 | 20 | 0 | 224816.03 | 246862.90 | - | logistic_regression | 0.5043 |
| TSLA | 20 | 0 | 20 | 2 | 224816.24 | 246863.11 | 246835.75 | logistic_regression | 0.5088 |
| BTC-USD | 20 | 0 | 0 | 0 | 215875.62 | - | - | - | - |

## 4. Prometheus Metrics Check

- `/metrics` status code: `200`
- `/metrics` response latency: `9.93 ms`
- Available expected metrics: `market_intel_api_requests_total, market_intel_api_request_duration_seconds, market_intel_websocket_active_connections, market_intel_market_rows_window, market_intel_news_rows_window, market_intel_prediction_rows_window, market_intel_alert_rows_window, market_intel_market_ingest_freshness_seconds, market_intel_prediction_freshness_seconds, market_intel_alert_freshness_seconds`
- Missing expected metrics: `-`

## 5. Interpretation

The system analysis evaluates the serving layer from the perspective of API responsiveness, data availability, freshness, and monitoring readiness. Low API latency indicates that the FastAPI and Cassandra serving path can return market, news, prediction, alert, and dashboard data quickly for the demo workload. Freshness values should be interpreted carefully: for historical replay data, market event time may be old, but ingest time and Spark processing time represent the freshness of the replay pipeline. Prediction and alert freshness show how recently the offline model outputs and alert rules were refreshed.

## 6. Output Files

- `docs/system_analysis/api_latency_results.csv`
- `docs/system_analysis/api_latency_summary.csv`
- `docs/system_analysis/system_summary_table.csv`
- `docs/system_analysis/system_summary_snapshot.json`
- `docs/system_analysis/prometheus_check.json`