import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    lit,
    sum as spark_sum,
    when,
)


PREDICTION_ROOT = Path("data/model_artifacts/baseline_models")
METRICS_PATH = PREDICTION_ROOT / "model_metrics.json"
CHAMPION_PATH = PREDICTION_ROOT / "champion_model.json"
OUTPUT_DIR = Path("docs/model_evaluation")


def safe_div(num, den):
    if den in (0, None):
        return None
    return num / den


def load_json(path: Path):
    if not path.exists():
        return None

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def collect_counts(df):
    row = (
        df.agg(
            count("*").alias("n"),
            spark_sum(when((col("target") == 1) & (col("pred") == 1), 1).otherwise(0)).alias("tp"),
            spark_sum(when((col("target") == 0) & (col("pred") == 0), 1).otherwise(0)).alias("tn"),
            spark_sum(when((col("target") == 0) & (col("pred") == 1), 1).otherwise(0)).alias("fp"),
            spark_sum(when((col("target") == 1) & (col("pred") == 0), 1).otherwise(0)).alias("fn"),
            avg(when(col("target") == col("pred"), 1.0).otherwise(0.0)).alias("accuracy"),
            avg(col("target").cast("double")).alias("actual_up_rate"),
            avg(col("pred").cast("double")).alias("predicted_up_rate"),
        )
        .first()
    )

    tp = int(row.tp or 0)
    tn = int(row.tn or 0)
    fp = int(row.fp or 0)
    fn = int(row.fn or 0)

    precision_up = safe_div(tp, tp + fp)
    recall_up = safe_div(tp, tp + fn)
    f1_up = (
        safe_div(2 * precision_up * recall_up, precision_up + recall_up)
        if precision_up is not None and recall_up is not None
        else None
    )

    precision_down = safe_div(tn, tn + fn)
    recall_down = safe_div(tn, tn + fp)
    f1_down = (
        safe_div(2 * precision_down * recall_down, precision_down + recall_down)
        if precision_down is not None and recall_down is not None
        else None
    )

    return {
        "n": int(row.n or 0),
        "tp": tp,
        "tn": tn,
        "fp": fp,
        "fn": fn,
        "accuracy": float(row.accuracy or 0.0),
        "actual_up_rate": float(row.actual_up_rate or 0.0),
        "predicted_up_rate": float(row.predicted_up_rate or 0.0),
        "precision_up": precision_up,
        "recall_up": recall_up,
        "f1_up": f1_up,
        "precision_down": precision_down,
        "recall_down": recall_down,
        "f1_down": f1_down,
    }


def fmt(value, digits=4):
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def main():
    spark = (
        SparkSession.builder
        .appName("build-model-evaluation-pack")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    metrics_json = load_json(METRICS_PATH) or []
    champion_json = load_json(CHAMPION_PATH) or {}

    prediction_dirs = sorted(PREDICTION_ROOT.glob("*_predictions"))

    if not prediction_dirs:
        raise RuntimeError(f"No prediction directories found in {PREDICTION_ROOT}")

    overall_rows = []
    per_symbol_rows = []
    confusion_rows = []

    markdown = []
    markdown.append("# Model Evaluation Diagnostics\n")
    markdown.append("This report summarizes classification model prediction behavior using saved Spark prediction outputs.\n")

    champion_model = champion_json.get("champion_model")
    if champion_model:
        markdown.append(f"**Champion model:** `{champion_model}`\n")

    markdown.append("## 1. Metrics from training script\n")

    if metrics_json:
        markdown.append("| Model | Accuracy | F1 | Weighted Precision | Weighted Recall | ROC AUC | Split |")
        markdown.append("|---|---:|---:|---:|---:|---:|---|")

        for item in metrics_json:
            markdown.append(
                "| {model} | {acc} | {f1} | {prec} | {rec} | {auc} | {split} |".format(
                    model=item.get("model_name"),
                    acc=fmt(item.get("accuracy")),
                    f1=fmt(item.get("f1")),
                    prec=fmt(item.get("weighted_precision")),
                    rec=fmt(item.get("weighted_recall")),
                    auc=fmt(item.get("roc_auc")),
                    split=item.get("split_strategy", "-"),
                )
            )

        markdown.append("")

    markdown.append("## 2. Confusion and directional diagnostics\n")
    markdown.append("| Model | Rows | Accuracy | TP | TN | FP | FN | Actual UP Rate | Predicted UP Rate | F1 UP | F1 DOWN |")
    markdown.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")

    for pred_dir in prediction_dirs:
        model_name = pred_dir.name.replace("_predictions", "")

        print(f"Reading predictions: {pred_dir}")
        raw = spark.read.parquet(str(pred_dir))

        required_columns = {"symbol", "event_minute", "target_direction", "prediction"}
        missing = required_columns - set(raw.columns)

        if missing:
            print(f"Skipping {model_name}; missing columns: {missing}")
            continue

        df = (
            raw
            .withColumn("model_name", lit(model_name))
            .withColumn("target", col("target_direction").cast("int"))
            .withColumn("pred", col("prediction").cast("int"))
            .where(col("target").isNotNull())
            .where(col("pred").isNotNull())
            .cache()
        )

        overall = collect_counts(df)
        overall["model_name"] = model_name
        overall_rows.append(overall)

        markdown.append(
            "| {model} | {n} | {acc} | {tp} | {tn} | {fp} | {fn} | {actual_up} | {pred_up} | {f1_up} | {f1_down} |".format(
                model=model_name,
                n=overall["n"],
                acc=fmt(overall["accuracy"]),
                tp=overall["tp"],
                tn=overall["tn"],
                fp=overall["fp"],
                fn=overall["fn"],
                actual_up=fmt(overall["actual_up_rate"]),
                pred_up=fmt(overall["predicted_up_rate"]),
                f1_up=fmt(overall["f1_up"]),
                f1_down=fmt(overall["f1_down"]),
            )
        )

        symbol_values = [r.symbol for r in df.select("symbol").distinct().collect()]

        for symbol in sorted(symbol_values):
            symbol_df = df.where(col("symbol") == symbol)
            symbol_metrics = collect_counts(symbol_df)
            symbol_metrics["model_name"] = model_name
            symbol_metrics["symbol"] = symbol
            per_symbol_rows.append(symbol_metrics)

        confusion = (
            df.groupBy("model_name", "target", "pred")
            .count()
            .orderBy("model_name", "target", "pred")
        )

        for row in confusion.collect():
            confusion_rows.append(
                {
                    "model_name": row.model_name,
                    "target": int(row.target),
                    "prediction": int(row.pred),
                    "count": int(row["count"]),
                }
            )

        errors_path = OUTPUT_DIR / f"{model_name}_errors.csv"

        (
            df
            .where(col("target") != col("pred"))
            .select(
                "model_name",
                "symbol",
                "event_minute",
                "market_price",
                "target",
                "pred",
            )
            .orderBy("symbol", "event_minute")
            .limit(200)
            .toPandas()
            .to_csv(errors_path, index=False)
        )

        print(f"Wrote errors: {errors_path}")

    markdown.append("")

    markdown.append("## 3. Per-symbol diagnostics\n")
    markdown.append("| Model | Symbol | Rows | Accuracy | TP | TN | FP | FN | Actual UP Rate | Predicted UP Rate |")
    markdown.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|")

    for item in per_symbol_rows:
        markdown.append(
            "| {model} | {symbol} | {n} | {acc} | {tp} | {tn} | {fp} | {fn} | {actual_up} | {pred_up} |".format(
                model=item["model_name"],
                symbol=item["symbol"],
                n=item["n"],
                acc=fmt(item["accuracy"]),
                tp=item["tp"],
                tn=item["tn"],
                fp=item["fp"],
                fn=item["fn"],
                actual_up=fmt(item["actual_up_rate"]),
                pred_up=fmt(item["predicted_up_rate"]),
            )
        )

    markdown.append("")

    markdown.append("## 4. Interpretation notes\n")
    markdown.append(
        "- `TP` means the model predicted UP and the next return was actually UP.\n"
        "- `TN` means the model predicted DOWN and the next return was actually DOWN.\n"
        "- `FP` means the model predicted UP but the next return was DOWN.\n"
        "- `FN` means the model predicted DOWN but the next return was UP.\n"
        "- For short-horizon market prediction, near-random performance is common. The purpose of this report is to make model limitations visible instead of hiding them.\n"
        "- Model quality should be interpreted together with data volume, prediction horizon, class balance, and time-based validation.\n"
    )

    import pandas as pd

    pd.DataFrame(overall_rows).to_csv(OUTPUT_DIR / "overall_diagnostics.csv", index=False)
    pd.DataFrame(per_symbol_rows).to_csv(OUTPUT_DIR / "per_symbol_diagnostics.csv", index=False)
    pd.DataFrame(confusion_rows).to_csv(OUTPUT_DIR / "confusion_matrix_long.csv", index=False)

    report_path = OUTPUT_DIR / "model_diagnostics.md"
    report_path.write_text("\n".join(markdown), encoding="utf-8")

    print(f"Wrote report: {report_path}")
    print(f"Wrote CSV outputs to: {OUTPUT_DIR}")

    spark.stop()


if __name__ == "__main__":
    main()
