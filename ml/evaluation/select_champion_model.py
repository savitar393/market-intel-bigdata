import json
import os
from pathlib import Path


METRICS_PATH = Path(
    os.getenv(
        "MODEL_METRICS_PATH",
        "data/model_artifacts/baseline_models/model_metrics.json",
    )
)

CHAMPION_PATH = Path(
    os.getenv(
        "CHAMPION_MODEL_PATH",
        "data/model_artifacts/baseline_models/champion_model.json",
    )
)

SUMMARY_PATH = Path(
    os.getenv(
        "MODEL_SUMMARY_PATH",
        "docs/model_evaluation_summary.md",
    )
)


def fmt(value):
    if value is None:
        return "N/A"
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def main():
    if not METRICS_PATH.exists():
        raise FileNotFoundError(f"Metrics file not found: {METRICS_PATH}")

    with METRICS_PATH.open("r", encoding="utf-8") as f:
        metrics = json.load(f)

    successful = [
        m for m in metrics
        if "status" not in m or m.get("status") != "failed"
    ]

    if not successful:
        raise RuntimeError("No successful model metrics found.")

    # Main selection metric: F1.
    # Tie-breaker: accuracy, then ROC-AUC.
    champion = sorted(
        successful,
        key=lambda m: (
            m.get("f1") if m.get("f1") is not None else -1,
            m.get("accuracy") if m.get("accuracy") is not None else -1,
            m.get("roc_auc") if m.get("roc_auc") is not None else -1,
        ),
        reverse=True,
    )[0]

    CHAMPION_PATH.parent.mkdir(parents=True, exist_ok=True)

    champion_payload = {
        "champion_model": champion["model_name"],
        "selection_metric": "f1",
        "selection_reason": (
            "Selected by highest F1 score, with accuracy and ROC-AUC as tie-breakers."
        ),
        "metrics": champion,
        "all_models": metrics,
        "note": (
            "Baseline results are close to random-level performance and should be "
            "interpreted as pipeline validation, not as a production trading signal."
        ),
    }

    with CHAMPION_PATH.open("w", encoding="utf-8") as f:
        json.dump(champion_payload, f, indent=2)

    lines = []
    lines.append("# Model Evaluation Summary")
    lines.append("")
    lines.append("## Experiment context")
    lines.append("")
    lines.append(
        "This experiment trains three baseline classification models to predict "
        "the next-interval price direction from market and news-derived features."
    )
    lines.append("")
    lines.append("The target variable is `target_direction`:")
    lines.append("")
    lines.append("- `1` = next interval return is positive")
    lines.append("- `0` = next interval return is not positive")
    lines.append("")
    lines.append("## Model comparison")
    lines.append("")
    lines.append(
        "| Model | Train rows | Test rows | Accuracy | F1 | Precision | Recall | ROC-AUC |"
    )
    lines.append(
        "|---|---:|---:|---:|---:|---:|---:|---:|"
    )

    for m in successful:
        lines.append(
            f"| {m.get('model_name')} "
            f"| {m.get('train_rows')} "
            f"| {m.get('test_rows')} "
            f"| {fmt(m.get('accuracy'))} "
            f"| {fmt(m.get('f1'))} "
            f"| {fmt(m.get('weighted_precision'))} "
            f"| {fmt(m.get('weighted_recall'))} "
            f"| {fmt(m.get('roc_auc'))} |"
        )

    lines.append("")
    lines.append("## Champion model")
    lines.append("")
    lines.append(f"Champion model: **{champion['model_name']}**")
    lines.append("")
    lines.append(
        "The champion model is selected using F1 score as the primary metric. "
        "Accuracy and ROC-AUC are used as secondary tie-breakers."
    )
    lines.append("")
    lines.append("## Interpretation")
    lines.append("")
    lines.append(
        "The current baseline models perform close to random-level prediction. "
        "This result is acceptable at this milestone because the purpose is to "
        "validate the full big-data ML pipeline: data ingestion, Spark processing, "
        "feature generation, model training, and metric reporting."
    )
    lines.append("")
    lines.append(
        "The current dataset uses a limited historical window and a simple target. "
        "Future improvements should include a longer historical window, richer "
        "rolling features, better time alignment between market and news events, "
        "and additional sequence-based models."
    )
    lines.append("")
    lines.append("## Feature columns")
    lines.append("")
    for feature in champion.get("feature_columns", []):
        lines.append(f"- `{feature}`")

    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)

    with SUMMARY_PATH.open("w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("Champion model:")
    print(json.dumps(champion_payload, indent=2))
    print(f"\nSaved champion metadata to: {CHAMPION_PATH}")
    print(f"Saved model evaluation summary to: {SUMMARY_PATH}")


if __name__ == "__main__":
    main()
