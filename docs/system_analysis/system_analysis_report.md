# System Analysis Report

Generated at: `2026-06-27T14:07:02.789813+00:00`

## 1. Test Configuration

- API base: `http://localhost:8000`
- Symbols: `AAPL, MSFT, NVDA, AMZN, TSLA, BTC-USD`
- Iterations: `5`
- Started at: `2026-06-27T14:06:23.995423+00:00`
- Finished at: `2026-06-27T14:07:02.789813+00:00`
- Total measured requests: `185`
- Successful requests: `185`
- Approximate API request throughput during test: `4.77 requests/second`

## 2. API Latency Summary

| Endpoint | Samples | Avg ms | Median ms | P95 ms | Min ms | Max ms |
|---|---:|---:|---:|---:|---:|---:|
| alerts_latest | 30 | 11.14 | 6.39 | 8.45 | 4.59 | 146.64 |
| dashboard_snapshot | 30 | 13.01 | 12.92 | 16.65 | 9.75 | 18.96 |
| health | 5 | 9.35 | 6.82 | 7.12 | 6.09 | 20.43 |
| market_latest | 30 | 6.89 | 6.86 | 8.44 | 5.47 | 8.92 |
| news_latest | 30 | 6.55 | 6.42 | 8.10 | 4.76 | 8.55 |
| predictions_latest | 30 | 6.54 | 6.55 | 7.65 | 5.07 | 8.36 |
| system_summary | 30 | 11.39 | 11.31 | 13.27 | 9.03 | 14.81 |

## 3. Freshness and Data Availability Snapshot

| Symbol | Market Rows | News Rows | Prediction Rows | Alert Rows | Market Freshness s | Prediction Freshness s | Alert Freshness s | Model | Confidence |
|---|---:|---:|---:|---:|---:|---:|---:|---|---:|
| AAPL | 20 | 5 | 20 | 0 | 0.00 | 83.04 | - | logistic_regression | 0.4990 |
| MSFT | 20 | 5 | 20 | 0 | 0.00 | 83.25 | - | logistic_regression | 0.5338 |
| NVDA | 20 | 5 | 20 | 6 | 0.00 | 83.46 | 56.10 | logistic_regression | 0.5711 |
| AMZN | 20 | 0 | 20 | 0 | 0.00 | 83.67 | - | logistic_regression | 0.5043 |
| TSLA | 20 | 0 | 20 | 2 | 0.00 | 83.88 | 56.52 | logistic_regression | 0.5088 |
| BTC-USD | 20 | 0 | 0 | 0 | 0.00 | - | - | - | - |

## 4. Prometheus Metrics Check

- `/metrics` status code: `200`
- `/metrics` response latency: `8.67 ms`
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