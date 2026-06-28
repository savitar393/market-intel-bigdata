import os
import subprocess
from pathlib import Path

from prefect import flow, task


PROJECT_ROOT = Path(__file__).resolve().parents[1]

FINAL_STOCK_SYMBOLS = os.getenv(
    "MODEL_SYMBOLS",
    "AAPL,MSFT,NVDA,AMZN,TSLA",
)

CLASSIFICATION_LABEL_COLUMN = os.getenv(
    "CLASSIFICATION_LABEL_COLUMN",
    "target_direction",
)

PREDICTION_ALERT_THRESHOLD = os.getenv(
    "PREDICTION_ALERT_THRESHOLD",
    "0.47",
)

FINAL_MODEL_SERVING_CONFIG = os.getenv(
    "FINAL_MODEL_SERVING_CONFIG",
    "config/final_model_serving.json",
)


def run_command(
    command: list[str],
    task_name: str,
    extra_env: dict[str, str] | None = None,
):
    print(f"\n=== {task_name} ===")
    print(" ".join(command))

    env = os.environ.copy()

    env.update(
        {
            "MODEL_SYMBOLS": FINAL_STOCK_SYMBOLS,
            "CLASSIFICATION_LABEL_COLUMN": CLASSIFICATION_LABEL_COLUMN,
            "PREDICTION_ALERT_THRESHOLD": PREDICTION_ALERT_THRESHOLD,
            "FINAL_MODEL_SERVING_CONFIG": FINAL_MODEL_SERVING_CONFIG,
        }
    )

    if extra_env:
        env.update(extra_env)

    result = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        env=env,
    )

    if result.stdout:
        print(result.stdout)

    if result.stderr:
        print(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(
            f"{task_name} failed with exit code {result.returncode}"
        )


@task(retries=1, retry_delay_seconds=5)
def build_features():
    run_command(
        ["spark-submit", "spark/jobs/build_market_news_features.py"],
        "Build market-news features",
    )


@task(retries=1, retry_delay_seconds=5)
def train_classification_models():
    run_command(
        ["spark-submit", "ml/training/train_spark_baseline_models.py"],
        "Train classification models",
        extra_env={
            "CLASSIFICATION_LABEL_COLUMN": CLASSIFICATION_LABEL_COLUMN,
        },
    )


@task(retries=1, retry_delay_seconds=5)
def select_champion_model():
    run_command(
        ["python", "ml/evaluation/select_champion_model.py"],
        "Select champion model for evaluation summary",
    )


@task(retries=1, retry_delay_seconds=5)
def export_online_model():
    run_command(
        ["python", "ml/serving/export_logistic_regression_online.py"],
        "Export Logistic Regression model for online inference",
        extra_env={
            "FINAL_MODEL_SERVING_CONFIG": FINAL_MODEL_SERVING_CONFIG,
        },
    )


@task(retries=1, retry_delay_seconds=5)
def train_regression_models():
    run_command(
        ["spark-submit", "ml/training/train_spark_regression_models.py"],
        "Train regression models",
    )


@task(retries=1, retry_delay_seconds=5)
def load_predictions_to_cassandra():
    run_command(
        ["python", "ml/serving/load_predictions_to_cassandra.py"],
        "Load tuned batch predictions to Cassandra",
        extra_env={
            "FINAL_MODEL_SERVING_CONFIG": FINAL_MODEL_SERVING_CONFIG,
        },
    )


@task(retries=1, retry_delay_seconds=5)
def generate_prediction_alerts():
    run_command(
        [
            "python",
            "services/alerts/generate_prediction_alerts.py",
            "--symbols",
            FINAL_STOCK_SYMBOLS,
            "--threshold",
            PREDICTION_ALERT_THRESHOLD,
        ],
        "Generate prediction alerts",
    )


@task(retries=1, retry_delay_seconds=5)
def log_mlflow_runs():
    run_command(
        ["python", "ml/tracking/log_mlflow_runs.py", "--all", "--log-model-artifacts"],
        "Log MLflow runs and artifacts",
    )


@task(retries=1, retry_delay_seconds=5)
def archive_outputs_to_hdfs():
    run_command(
        ["bash", "scripts/archive_to_hdfs.sh"],
        "Archive outputs to HDFS",
    )


@flow(name="market-intel-ml-refresh")
def ml_refresh_flow():
    print("ML refresh configuration:")
    print(f"  MODEL_SYMBOLS                = {FINAL_STOCK_SYMBOLS}")
    print(f"  CLASSIFICATION_LABEL_COLUMN  = {CLASSIFICATION_LABEL_COLUMN}")
    print(f"  PREDICTION_ALERT_THRESHOLD   = {PREDICTION_ALERT_THRESHOLD}")
    print(f"  FINAL_MODEL_SERVING_CONFIG   = {FINAL_MODEL_SERVING_CONFIG}")

    build_features()

    train_classification_models()
    select_champion_model()
    export_online_model()

    train_regression_models()

    load_predictions_to_cassandra()
    generate_prediction_alerts()

    log_mlflow_runs()
    archive_outputs_to_hdfs()


if __name__ == "__main__":
    ml_refresh_flow()
