# System Analysis Report

Generated at: `2026-06-28T11:53:06.854301+00:00`

## 1. Test Configuration

- API base: `http://localhost:8000`
- Symbols: `AAPL, MSFT, NVDA, AMZN, TSLA, BTC-USD`
- Iterations: `5`
- Started at: `2026-06-28T11:52:28.334841+00:00`
- Finished at: `2026-06-28T11:53:06.854301+00:00`
- Total measured requests: `185`
- Successful requests: `185`
- Approximate API request throughput during test: `4.80 requests/second`

## 2. API Latency Summary

| Endpoint | Samples | Avg ms | Median ms | P95 ms | Min ms | Max ms |
|---|---:|---:|---:|---:|---:|---:|
| alerts_latest | 30 | 5.23 | 4.98 | 6.91 | 4.17 | 8.03 |
| dashboard_snapshot | 30 | 11.22 | 11.20 | 13.24 | 8.23 | 14.31 |
| health | 5 | 6.49 | 5.66 | 5.88 | 4.65 | 10.71 |
| market_latest | 30 | 6.32 | 5.74 | 7.63 | 4.18 | 23.17 |
| news_latest | 30 | 5.49 | 5.33 | 6.65 | 4.55 | 7.74 |
| predictions_latest | 30 | 5.37 | 5.37 | 6.39 | 4.37 | 7.15 |
| system_summary | 30 | 12.51 | 10.51 | 16.98 | 8.34 | 45.83 |

## 3. Freshness and Data Availability Snapshot

| Symbol | Market Rows | News Rows | Prediction Rows | Alert Rows | Market Freshness s | Prediction Freshness s | Alert Freshness s | Model | Confidence |
|---|---:|---:|---:|---:|---:|---:|---:|---|---:|
| AAPL | 20 | 5 | 20 | 0 | 56400.23 | 78447.09 | - | logistic_regression | 0.4990 |
| MSFT | 20 | 5 | 20 | 0 | 56400.44 | 78447.31 | - | logistic_regression | 0.5338 |
| NVDA | 20 | 5 | 20 | 6 | 56400.65 | 78447.52 | 78420.16 | logistic_regression | 0.5711 |
| AMZN | 20 | 0 | 20 | 0 | 56400.86 | 78447.73 | - | logistic_regression | 0.5043 |
| TSLA | 20 | 0 | 20 | 2 | 56401.07 | 78447.94 | 78420.58 | logistic_regression | 0.5088 |
| BTC-USD | 20 | 0 | 0 | 0 | 47460.45 | - | - | - | - |

## 4. Prometheus Metrics Check

- `/metrics` status code: `200`
- `/metrics` response latency: `8.70 ms`
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