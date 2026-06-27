import argparse
import csv
import json
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


OUTPUT_DIR = Path("docs/system_analysis")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_get_json(url: str, timeout: float = 10.0) -> tuple[int | None, Any | None, str | None, float]:
    start = time.perf_counter()

    try:
        response = requests.get(url, timeout=timeout)
        elapsed_ms = (time.perf_counter() - start) * 1000.0

        try:
            payload = response.json()
        except Exception:
            payload = response.text[:500]

        return response.status_code, payload, None, elapsed_ms

    except Exception as exc:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return None, None, str(exc), elapsed_ms


def summarize_latencies(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[float]] = {}

    for row in rows:
        if row["status_code"] == 200 and row["latency_ms"] is not None:
            grouped.setdefault(row["endpoint_name"], []).append(float(row["latency_ms"]))

    summaries = []

    for endpoint_name, values in grouped.items():
        values_sorted = sorted(values)

        p95_index = int(0.95 * (len(values_sorted) - 1)) if values_sorted else 0

        summaries.append(
            {
                "endpoint_name": endpoint_name,
                "samples": len(values),
                "avg_ms": statistics.mean(values),
                "median_ms": statistics.median(values),
                "min_ms": min(values),
                "max_ms": max(values),
                "p95_ms": values_sorted[p95_index],
            }
        )

    return sorted(summaries, key=lambda item: item["endpoint_name"])


def fmt_num(value: Any, digits: int = 2) -> str:
    if value is None:
        return "-"

    try:
        return f"{float(value):.{digits}f}"
    except Exception:
        return str(value)


def write_csv(path: Path, rows: list[dict[str, Any]]):
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def extract_summary_row(symbol: str, payload: dict[str, Any]) -> dict[str, Any]:
    market = payload.get("market", {}) or {}
    news = payload.get("news", {}) or {}
    prediction = payload.get("prediction", {}) or {}
    alert = payload.get("alert", {}) or {}
    counts = payload.get("counts", {}) or {}

    return {
        "symbol": symbol,
        "market_rows": counts.get("market_rows"),
        "news_rows": counts.get("news_rows"),
        "prediction_rows": counts.get("prediction_rows"),
        "alert_rows": counts.get("alert_rows"),
        "market_avg_ingest_latency_seconds": market.get("avg_ingest_latency_seconds"),
        "market_ingest_freshness_seconds": market.get("ingest_time_freshness_seconds"),
        "news_ingest_freshness_seconds": news.get("ingest_time_freshness_seconds"),
        "prediction_freshness_seconds": prediction.get("prediction_freshness_seconds"),
        "alert_freshness_seconds": alert.get("alert_freshness_seconds"),
        "prediction_model": prediction.get("model_name"),
        "prediction_confidence": prediction.get("confidence"),
        "latest_alert_severity": alert.get("latest_severity"),
    }


def prometheus_check(api_base: str) -> dict[str, Any]:
    url = f"{api_base}/metrics"
    start = time.perf_counter()

    try:
        response = requests.get(url, timeout=10.0)
        latency_ms = (time.perf_counter() - start) * 1000.0
        text = response.text
        status_code = response.status_code
        error = None
    except Exception as exc:
        latency_ms = (time.perf_counter() - start) * 1000.0
        text = ""
        status_code = None
        error = str(exc)

    metric_names = [
        "market_intel_api_requests_total",
        "market_intel_api_request_duration_seconds",
        "market_intel_websocket_active_connections",
        "market_intel_market_rows_window",
        "market_intel_news_rows_window",
        "market_intel_prediction_rows_window",
        "market_intel_alert_rows_window",
        "market_intel_market_ingest_freshness_seconds",
        "market_intel_prediction_freshness_seconds",
        "market_intel_alert_freshness_seconds",
    ]

    return {
        "status_code": status_code,
        "latency_ms": latency_ms,
        "error": error,
        "available_metrics": [name for name in metric_names if name in text],
        "missing_metrics": [name for name in metric_names if name not in text],
    }


def main():
    parser = argparse.ArgumentParser(
        description="Run system analysis checks for API latency, freshness, and monitoring readiness."
    )
    parser.add_argument(
        "--api-base",
        default="http://localhost:8000",
        help="FastAPI base URL.",
    )
    parser.add_argument(
        "--symbols",
        default="AAPL,MSFT,NVDA,AMZN,TSLA,BTC-USD",
        help="Comma-separated symbols to test.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=5,
        help="Number of latency samples per endpoint.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.2,
        help="Delay between requests.",
    )

    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    api_base = args.api_base.rstrip("/")

    endpoint_templates = [
        ("health", "/api/v1/health", None),
        ("market_latest", "/api/v1/market/latest/{symbol}?limit=5", "symbol"),
        ("news_latest", "/api/v1/news/latest/{symbol}?limit=5", "symbol"),
        ("predictions_latest", "/api/v1/predictions/latest/{symbol}?limit=5", "symbol"),
        ("alerts_latest", "/api/v1/alerts/latest/{symbol}?limit=5", "symbol"),
        ("dashboard_snapshot", "/api/v1/dashboard/snapshot/{symbol}", "symbol"),
        ("system_summary", "/api/v1/system/summary/{symbol}?limit=20", "symbol"),
    ]

    latency_rows = []
    summary_payloads = {}
    summary_rows = []

    run_started_at = now_iso()
    wall_start = time.perf_counter()

    for i in range(args.iterations):
        print(f"Iteration {i + 1}/{args.iterations}")

        for endpoint_name, template, mode in endpoint_templates:
            target_symbols = symbols if mode == "symbol" else [None]

            for symbol in target_symbols:
                path = template.format(symbol=symbol) if symbol else template
                url = f"{api_base}{path}"

                status_code, payload, error, latency_ms = safe_get_json(url)

                latency_rows.append(
                    {
                        "timestamp": now_iso(),
                        "iteration": i + 1,
                        "endpoint_name": endpoint_name,
                        "symbol": symbol or "",
                        "url": url,
                        "status_code": status_code,
                        "latency_ms": latency_ms,
                        "error": error or "",
                    }
                )

                if endpoint_name == "system_summary" and status_code == 200 and isinstance(payload, dict):
                    summary_payloads[symbol] = payload

                time.sleep(args.sleep_seconds)

    total_wall_seconds = time.perf_counter() - wall_start
    run_finished_at = now_iso()

    for symbol, payload in summary_payloads.items():
        summary_rows.append(extract_summary_row(symbol, payload))

    latency_summary = summarize_latencies(latency_rows)
    prom_check = prometheus_check(api_base)

    successful_requests = sum(1 for row in latency_rows if row["status_code"] == 200)
    total_requests = len(latency_rows)
    approximate_throughput = total_requests / total_wall_seconds if total_wall_seconds > 0 else None

    write_csv(OUTPUT_DIR / "api_latency_results.csv", latency_rows)
    write_csv(OUTPUT_DIR / "api_latency_summary.csv", latency_summary)
    write_csv(OUTPUT_DIR / "system_summary_table.csv", summary_rows)

    (OUTPUT_DIR / "system_summary_snapshot.json").write_text(
        json.dumps(summary_payloads, indent=2, default=str),
        encoding="utf-8",
    )

    (OUTPUT_DIR / "prometheus_check.json").write_text(
        json.dumps(prom_check, indent=2, default=str),
        encoding="utf-8",
    )

    report = []

    report.append("# System Analysis Report\n")
    report.append(f"Generated at: `{run_finished_at}`\n")
    report.append("## 1. Test Configuration\n")
    report.append(f"- API base: `{api_base}`")
    report.append(f"- Symbols: `{', '.join(symbols)}`")
    report.append(f"- Iterations: `{args.iterations}`")
    report.append(f"- Started at: `{run_started_at}`")
    report.append(f"- Finished at: `{run_finished_at}`")
    report.append(f"- Total measured requests: `{total_requests}`")
    report.append(f"- Successful requests: `{successful_requests}`")
    report.append(f"- Approximate API request throughput during test: `{fmt_num(approximate_throughput)} requests/second`\n")

    report.append("## 2. API Latency Summary\n")
    report.append("| Endpoint | Samples | Avg ms | Median ms | P95 ms | Min ms | Max ms |")
    report.append("|---|---:|---:|---:|---:|---:|---:|")

    for row in latency_summary:
        report.append(
            "| {endpoint} | {samples} | {avg} | {median} | {p95} | {minv} | {maxv} |".format(
                endpoint=row["endpoint_name"],
                samples=row["samples"],
                avg=fmt_num(row["avg_ms"]),
                median=fmt_num(row["median_ms"]),
                p95=fmt_num(row["p95_ms"]),
                minv=fmt_num(row["min_ms"]),
                maxv=fmt_num(row["max_ms"]),
            )
        )

    report.append("\n## 3. Freshness and Data Availability Snapshot\n")
    report.append(
        "| Symbol | Market Rows | News Rows | Prediction Rows | Alert Rows | Market Freshness s | Prediction Freshness s | Alert Freshness s | Model | Confidence |"
    )
    report.append("|---|---:|---:|---:|---:|---:|---:|---:|---|---:|")

    for row in summary_rows:
        report.append(
            "| {symbol} | {market_rows} | {news_rows} | {prediction_rows} | {alert_rows} | {market_fresh} | {pred_fresh} | {alert_fresh} | {model} | {confidence} |".format(
                symbol=row["symbol"],
                market_rows=row.get("market_rows", "-"),
                news_rows=row.get("news_rows", "-"),
                prediction_rows=row.get("prediction_rows", "-"),
                alert_rows=row.get("alert_rows", "-"),
                market_fresh=fmt_num(row.get("market_ingest_freshness_seconds")),
                pred_fresh=fmt_num(row.get("prediction_freshness_seconds")),
                alert_fresh=fmt_num(row.get("alert_freshness_seconds")),
                model=row.get("prediction_model") or "-",
                confidence=fmt_num(row.get("prediction_confidence"), 4),
            )
        )

    report.append("\n## 4. Prometheus Metrics Check\n")
    report.append(f"- `/metrics` status code: `{prom_check.get('status_code')}`")
    report.append(f"- `/metrics` response latency: `{fmt_num(prom_check.get('latency_ms'))} ms`")
    report.append(f"- Available expected metrics: `{', '.join(prom_check.get('available_metrics', [])) or '-'}`")
    report.append(f"- Missing expected metrics: `{', '.join(prom_check.get('missing_metrics', [])) or '-'}`\n")

    report.append("## 5. Interpretation\n")
    report.append(
        "The system analysis evaluates the serving layer from the perspective of API responsiveness, "
        "data availability, freshness, and monitoring readiness. Low API latency indicates that the "
        "FastAPI and Cassandra serving path can return market, news, prediction, alert, and dashboard "
        "data quickly for the demo workload. Freshness values should be interpreted carefully: for "
        "historical replay data, market event time may be old, but ingest time and Spark processing time "
        "represent the freshness of the replay pipeline. Prediction and alert freshness show how recently "
        "the offline model outputs and alert rules were refreshed."
    )

    report.append("\n## 6. Output Files\n")
    report.append("- `docs/system_analysis/api_latency_results.csv`")
    report.append("- `docs/system_analysis/api_latency_summary.csv`")
    report.append("- `docs/system_analysis/system_summary_table.csv`")
    report.append("- `docs/system_analysis/system_summary_snapshot.json`")
    report.append("- `docs/system_analysis/prometheus_check.json`")

    report_path = OUTPUT_DIR / "system_analysis_report.md"
    report_path.write_text("\n".join(report), encoding="utf-8")

    print(f"Wrote {report_path}")
    print(f"Wrote outputs to {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
