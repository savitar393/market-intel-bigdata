# System Analysis Report

Generated at: `2026-06-27T14:56:30.004847+00:00`

## 1. Test Configuration

- API base: `http://localhost:8000`
- Symbols: `AAPL, MSFT, NVDA, AMZN, TSLA, BTC-USD`
- Iterations: `5`
- Started at: `2026-06-27T14:55:50.322262+00:00`
- Finished at: `2026-06-27T14:56:30.004847+00:00`
- Total measured requests: `185`
- Successful requests: `185`
- Approximate API request throughput during test: `4.66 requests/second`

## 2. API Latency Summary

| Endpoint | Samples | Avg ms | Median ms | P95 ms | Min ms | Max ms |
|---|---:|---:|---:|---:|---:|---:|
| alerts_latest | 30 | 7.90 | 7.06 | 11.31 | 4.71 | 12.63 |
| dashboard_snapshot | 30 | 19.98 | 18.28 | 26.31 | 13.30 | 42.45 |
| health | 5 | 20.48 | 8.45 | 9.16 | 5.98 | 70.59 |
| market_latest | 30 | 10.93 | 9.17 | 21.29 | 6.11 | 22.68 |
| news_latest | 30 | 9.06 | 8.16 | 12.70 | 5.63 | 18.75 |
| predictions_latest | 30 | 11.45 | 7.62 | 17.25 | 5.23 | 83.64 |
| system_summary | 30 | 24.02 | 18.24 | 47.84 | 12.49 | 61.55 |

## 3. Freshness and Data Availability Snapshot

| Symbol | Market Rows | News Rows | Prediction Rows | Alert Rows | Market Freshness s | Prediction Freshness s | Alert Freshness s | Model | Confidence |
|---|---:|---:|---:|---:|---:|---:|---:|---|---:|
| AAPL | 20 | 5 | 20 | 0 | -18996.65 | 3050.22 | - | logistic_regression | 0.4990 |
| MSFT | 20 | 5 | 20 | 0 | -18996.43 | 3050.44 | - | logistic_regression | 0.5338 |
| NVDA | 20 | 5 | 20 | 6 | -18996.21 | 3050.66 | 3023.30 | logistic_regression | 0.5711 |
| AMZN | 20 | 0 | 20 | 0 | -18995.99 | 3050.88 | - | logistic_regression | 0.5043 |
| TSLA | 20 | 0 | 20 | 2 | -18995.77 | 3051.10 | 3023.73 | logistic_regression | 0.5088 |
| BTC-USD | 20 | 0 | 0 | 0 | -23777.69 | - | - | - | - |

## 4. Prometheus Metrics Check

- `/metrics` status code: `200`
- `/metrics` response latency: `8.78 ms`
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