# System Analysis Report

Generated at: `2026-06-28T10:58:46.309722+00:00`

## 1. Test Configuration

- API base: `http://localhost:8000`
- Symbols: `AAPL, MSFT, NVDA, AMZN, TSLA, BTC-USD`
- Iterations: `5`
- Started at: `2026-06-28T10:58:07.841470+00:00`
- Finished at: `2026-06-28T10:58:46.309722+00:00`
- Total measured requests: `185`
- Successful requests: `185`
- Approximate API request throughput during test: `4.81 requests/second`

## 2. API Latency Summary

| Endpoint | Samples | Avg ms | Median ms | P95 ms | Min ms | Max ms |
|---|---:|---:|---:|---:|---:|---:|
| alerts_latest | 30 | 5.67 | 5.66 | 6.90 | 4.42 | 7.34 |
| dashboard_snapshot | 30 | 10.55 | 10.52 | 11.44 | 8.21 | 14.66 |
| health | 5 | 7.32 | 6.96 | 7.38 | 6.19 | 9.67 |
| market_latest | 30 | 5.93 | 5.94 | 6.85 | 4.46 | 7.82 |
| news_latest | 30 | 5.62 | 5.57 | 7.06 | 4.35 | 7.51 |
| predictions_latest | 30 | 5.92 | 5.94 | 6.73 | 4.63 | 7.09 |
| system_summary | 30 | 9.99 | 9.75 | 11.98 | 7.99 | 13.95 |

## 3. Freshness and Data Availability Snapshot

| Symbol | Market Rows | News Rows | Prediction Rows | Alert Rows | Market Freshness s | Prediction Freshness s | Alert Freshness s | Model | Confidence |
|---|---:|---:|---:|---:|---:|---:|---:|---|---:|
| AAPL | 20 | 5 | 20 | 0 | 53139.69 | 75186.56 | - | logistic_regression | 0.4990 |
| MSFT | 20 | 5 | 20 | 0 | 53139.90 | 75186.77 | - | logistic_regression | 0.5338 |
| NVDA | 20 | 5 | 20 | 6 | 53140.12 | 75186.98 | 75159.63 | logistic_regression | 0.5711 |
| AMZN | 20 | 0 | 20 | 0 | 53140.33 | 75187.20 | - | logistic_regression | 0.5043 |
| TSLA | 20 | 0 | 20 | 2 | 53140.54 | 75187.41 | 75160.04 | logistic_regression | 0.5088 |
| BTC-USD | 20 | 0 | 0 | 0 | 44199.91 | - | - | - | - |

## 4. Prometheus Metrics Check

- `/metrics` status code: `200`
- `/metrics` response latency: `10.08 ms`
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