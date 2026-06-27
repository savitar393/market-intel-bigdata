# System Analysis Report

Generated at: `2026-06-27T13:22:47.935886+00:00`

## 1. Test Configuration

- API base: `http://localhost:8000`
- Symbols: `AAPL, MSFT, NVDA, AMZN, TSLA, BTC-USD`
- Iterations: `5`
- Started at: `2026-06-27T13:22:09.151596+00:00`
- Finished at: `2026-06-27T13:22:47.935886+00:00`
- Total measured requests: `185`
- Successful requests: `185`
- Approximate API request throughput during test: `4.77 requests/second`

## 2. API Latency Summary

| Endpoint | Samples | Avg ms | Median ms | P95 ms | Min ms | Max ms |
|---|---:|---:|---:|---:|---:|---:|
| alerts_latest | 30 | 6.66 | 6.45 | 8.51 | 5.26 | 9.32 |
| dashboard_snapshot | 30 | 14.10 | 12.90 | 16.21 | 9.22 | 30.21 |
| health | 5 | 11.02 | 5.94 | 6.54 | 5.64 | 31.12 |
| market_latest | 30 | 7.97 | 7.02 | 9.61 | 5.35 | 32.54 |
| news_latest | 30 | 6.95 | 6.65 | 8.96 | 4.92 | 10.26 |
| predictions_latest | 30 | 7.02 | 7.22 | 8.88 | 4.70 | 10.94 |
| system_summary | 30 | 12.80 | 12.81 | 15.35 | 10.15 | 15.69 |

## 3. Freshness and Data Availability Snapshot

| Symbol | Market Rows | News Rows | Prediction Rows | Alert Rows | Market Freshness s | Prediction Freshness s | Alert Freshness s | Model | Confidence |
|---|---:|---:|---:|---:|---:|---:|---:|---|---:|
| AAPL | 20 | 5 | 20 | 20 | -24618.69 | 97770.98 | 1498.41 | gradient_boosted_trees | 0.5804 |
| MSFT | 20 | 5 | 20 | 20 | -24618.48 | 1134015.13 | 1498.61 | gradient_boosted_trees | 0.5491 |
| NVDA | 20 | 5 | 20 | 20 | -24618.26 | 1134015.34 | 1498.80 | gradient_boosted_trees | 0.6111 |
| AMZN | 20 | 0 | 20 | 6 | -24618.05 | 97771.62 | 1499.00 | gradient_boosted_trees | 0.5066 |
| TSLA | 20 | 0 | 20 | 2 | -24617.84 | 1515.85 | 1499.20 | logistic_regression | 0.5088 |
| BTC-USD | 20 | 0 | 0 | 0 | -24727.53 | - | - | - | - |

## 4. Prometheus Metrics Check

- `/metrics` status code: `200`
- `/metrics` response latency: `10.51 ms`
- Available expected metrics: `-`
- Missing expected metrics: `market_intel_api_requests_total, market_intel_api_request_duration_seconds, market_intel_market_ingest_freshness_seconds, market_intel_prediction_freshness_seconds, market_intel_alert_freshness_seconds`

## 5. Interpretation

The system analysis evaluates the serving layer from the perspective of API responsiveness, data availability, freshness, and monitoring readiness. Low API latency indicates that the FastAPI and Cassandra serving path can return market, news, prediction, alert, and dashboard data quickly for the demo workload. Freshness values should be interpreted carefully: for historical replay data, market event time may be old, but ingest time and Spark processing time represent the freshness of the replay pipeline. Prediction and alert freshness show how recently the offline model outputs and alert rules were refreshed.

## 6. Output Files

- `docs/system_analysis/api_latency_results.csv`
- `docs/system_analysis/api_latency_summary.csv`
- `docs/system_analysis/system_summary_table.csv`
- `docs/system_analysis/system_summary_snapshot.json`
- `docs/system_analysis/prometheus_check.json`