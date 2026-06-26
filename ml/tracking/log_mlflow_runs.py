import argparse
import json
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
import mlflow


CLASSIFICATION_METRICS_PATH = Path(
    "data/model_artifacts/baseline_models/model_metrics.json"
)
REGRESSION_METRICS_PATH = Path(
    "data/model_artifacts/regression_models/regression_model_metrics.json"
)
CHAMPION_PATH = Path(
    "data/model_artifacts/baseline_models/champion_model.json"
)
MODEL_SUMMARY_PATH = Path(
    "docs/model_evaluation_summary.md"
)

DEFAULT_EXPERIMENT_NAME = "market-intel-baselines"


def is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def log_artifact_if_exists(path: Path):
    if path.exists():
        mlflow.log_artifact(str(path))


def log_artifacts_dir_if_exists(path: Path, artifact_path: str):
    if path.exists() and path.is_dir():
        mlflow.log_artifacts(str(path), artifact_path=artifact_path)


def log_metrics_file(
    metrics_path: Path,
    task_name: str,
    model_root: Path,
    log_model_artifacts: bool,
):
    if not metrics_path.exists():
        print(f"Skipping {task_name}: metrics file not found: {metrics_path}")
        return

    metrics_list = load_json(metrics_path)

    for metrics in metrics_list:
        model_name = metrics.get("model_name", "unknown_model")

        if metrics.get("status") == "failed":
            print(f"Skipping failed run: {task_name}/{model_name}")
            continue

        run_name = f"{task_name}_{model_name}"

        with mlflow.start_run(run_name=run_name):
            mlflow.set_tag("project", "market-intel-bigdata")
            mlflow.set_tag("task", task_name)
            mlflow.set_tag("model_name", model_name)
            mlflow.set_tag("source", "posthoc_metric_logger")

            for key, value in metrics.items():
                if key in {"feature_columns"}:
                    continue

                if is_number(value):
                    mlflow.log_metric(key, float(value))
                elif value is not None:
                    mlflow.log_param(key, str(value)[:500])

            feature_columns = metrics.get("feature_columns", [])
            mlflow.log_param("feature_count", len(feature_columns))
            mlflow.log_text(
                json.dumps(feature_columns, indent=2),
                artifact_file="feature_columns.json",
            )

            mlflow.log_artifact(str(metrics_path))

            if CHAMPION_PATH.exists():
                mlflow.log_artifact(str(CHAMPION_PATH))

            if MODEL_SUMMARY_PATH.exists():
                mlflow.log_artifact(str(MODEL_SUMMARY_PATH))

            if log_model_artifacts:
                model_path = model_root / model_name
                prediction_path = model_root / f"{model_name}_predictions"

                log_artifacts_dir_if_exists(
                    model_path,
                    artifact_path=f"{model_name}_spark_model",
                )
                log_artifacts_dir_if_exists(
                    prediction_path,
                    artifact_path=f"{model_name}_predictions",
                )

        print(f"Logged MLflow run: {run_name}")


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Log classification and regression metrics to MLflow."
    )
    parser.add_argument(
        "--classification",
        action="store_true",
        help="Log classification baseline metrics.",
    )
    parser.add_argument(
        "--regression",
        action="store_true",
        help="Log regression model metrics.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Log classification and regression metrics.",
    )
    parser.add_argument(
        "--log-model-artifacts",
        action="store_true",
        help="Also upload Spark model directories and prediction Parquet outputs as MLflow artifacts.",
    )

    args = parser.parse_args()

    if not (args.classification or args.regression or args.all):
        args.all = True

    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    experiment_name = os.getenv("MLFLOW_EXPERIMENT_NAME", DEFAULT_EXPERIMENT_NAME)

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment_name)

    print(f"MLflow tracking URI: {tracking_uri}")
    print(f"MLflow experiment: {experiment_name}")

    if args.all or args.classification:
        log_metrics_file(
            metrics_path=CLASSIFICATION_METRICS_PATH,
            task_name="classification_direction",
            model_root=Path("data/model_artifacts/baseline_models"),
            log_model_artifacts=args.log_model_artifacts,
        )

    if args.all or args.regression:
        log_metrics_file(
            metrics_path=REGRESSION_METRICS_PATH,
            task_name="regression_next_return",
            model_root=Path("data/model_artifacts/regression_models"),
            log_model_artifacts=args.log_model_artifacts,
        )

    print("Done logging MLflow runs.")


if __name__ == "__main__":
    main()
