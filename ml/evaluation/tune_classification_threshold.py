import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import DoubleType, IntegerType


PREDICTION_ROOT = Path("data/model_artifacts/baseline_models")
OUTPUT_DIR = Path("docs/model_evaluation/threshold_tuning")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def probability_up(probability):
    if probability is None:
        return None

    try:
        arr = probability.toArray().tolist()
    except Exception:
        try:
            arr = list(probability)
        except Exception:
            return None

    if len(arr) < 2:
        return None

    return float(arr[1])


probability_up_udf = udf(probability_up, DoubleType())


def safe_div(num, den):
    if den == 0:
        return None
    return num / den


def compute_metrics(rows):
    tp = sum(1 for r in rows if r["target"] == 1 and r["pred"] == 1)
    tn = sum(1 for r in rows if r["target"] == 0 and r["pred"] == 0)
    fp = sum(1 for r in rows if r["target"] == 0 and r["pred"] == 1)
    fn = sum(1 for r in rows if r["target"] == 1 and r["pred"] == 0)

    n = tp + tn + fp + fn
    accuracy = safe_div(tp + tn, n)

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

    weighted_f1 = None

    if f1_up is not None and f1_down is not None and n > 0:
        up_count = tp + fn
        down_count = tn + fp
        weighted_f1 = ((up_count * f1_up) + (down_count * f1_down)) / n

    return {
        "n": n,
        "tp": tp,
        "tn": tn,
        "fp": fp,
        "fn": fn,
        "accuracy": accuracy,
        "precision_up": precision_up,
        "recall_up": recall_up,
        "f1_up": f1_up,
        "precision_down": precision_down,
        "recall_down": recall_down,
        "f1_down": f1_down,
        "weighted_f1": weighted_f1,
        "predicted_up_rate": safe_div(tp + fp, n),
    }


def main():
    spark = (
        SparkSession.builder
        .appName("tune-classification-threshold")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    prediction_dirs = sorted(PREDICTION_ROOT.glob("*_predictions"))

    if not prediction_dirs:
        raise RuntimeError(f"No prediction directories found in {PREDICTION_ROOT}")

    all_results = []

    thresholds = [round(x / 100, 2) for x in range(35, 66, 1)]

    for pred_dir in prediction_dirs:
        model_name = pred_dir.name.replace("_predictions", "")

        df = spark.read.parquet(str(pred_dir))

        if "probability" not in df.columns:
            print(f"Skipping {model_name}: no probability column")
            continue

        scored = (
            df
            .withColumn("target", col("target_direction").cast(IntegerType()))
            .withColumn("probability_up", probability_up_udf(col("probability")))
            .select("symbol", "event_minute", "target", "probability_up")
            .where(col("target").isNotNull())
            .where(col("probability_up").isNotNull())
            .cache()
        )

        base_rows = [
            {
                "target": int(r.target),
                "probability_up": float(r.probability_up),
            }
            for r in scored.collect()
        ]

        for threshold in thresholds:
            rows = [
                {
                    "target": r["target"],
                    "pred": 1 if r["probability_up"] >= threshold else 0,
                }
                for r in base_rows
            ]

            metrics = compute_metrics(rows)
            metrics["model_name"] = model_name
            metrics["threshold"] = threshold
            all_results.append(metrics)

    import pandas as pd

    results_df = pd.DataFrame(all_results)

    csv_path = OUTPUT_DIR / "threshold_search_results.csv"
    results_df.to_csv(csv_path, index=False)

    best_by_f1 = (
        results_df
        .sort_values(["weighted_f1", "accuracy"], ascending=False)
        .head(10)
    )

    best_path = OUTPUT_DIR / "best_thresholds.csv"
    best_by_f1.to_csv(best_path, index=False)

    md = []
    md.append("# Classification Threshold Tuning\n")
    md.append("This report tunes the probability threshold for predicting UP direction.\n")
    md.append("Default Spark classification uses a 0.50 threshold. This search tests thresholds from 0.35 to 0.65.\n")
    md.append("## Top thresholds by weighted F1\n")
    md.append("| Rank | Model | Threshold | Accuracy | Weighted F1 | Precision UP | Recall UP | Predicted UP Rate |")
    md.append("|---:|---|---:|---:|---:|---:|---:|---:|")

    for idx, row in enumerate(best_by_f1.to_dict("records"), start=1):
        md.append(
            "| {rank} | {model} | {threshold:.2f} | {acc:.4f} | {f1:.4f} | {prec_up:.4f} | {rec_up:.4f} | {pred_up:.4f} |".format(
                rank=idx,
                model=row["model_name"],
                threshold=row["threshold"],
                acc=row["accuracy"] if row["accuracy"] is not None else 0,
                f1=row["weighted_f1"] if row["weighted_f1"] is not None else 0,
                prec_up=row["precision_up"] if row["precision_up"] is not None else 0,
                rec_up=row["recall_up"] if row["recall_up"] is not None else 0,
                pred_up=row["predicted_up_rate"] if row["predicted_up_rate"] is not None else 0,
            )
        )

    report_path = OUTPUT_DIR / "threshold_tuning_report.md"
    report_path.write_text("\n".join(md), encoding="utf-8")

    print(f"Wrote {csv_path}")
    print(f"Wrote {best_path}")
    print(f"Wrote {report_path}")
    print(best_by_f1.to_string(index=False))

    spark.stop()


if __name__ == "__main__":
    main()
