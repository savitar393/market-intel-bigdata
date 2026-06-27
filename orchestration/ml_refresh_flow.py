import subprocess
from pathlib import Path

from prefect import flow, task


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def run_command(command: list[str], task_name: str):
    print(f"\n=== {task_name} ===")
    print(" ".join(command))

    result = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
    )

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
    )


@task(retries=1, retry_delay_seconds=5)
def select_champion_model():
    run_command(
        ["python", "ml/evaluation/select_champion_model.py"],
        "Select champion model",
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
        "Load predictions to Cassandra",
    )


@task(retries=1, retry_delay_seconds=5)
def generate_prediction_alerts():
    run_command(
        [
            "python",
            "services/alerts/generate_prediction_alerts.py",
            "--symbols",
            "AAPL,MSFT,NVDA",
            "--threshold",
            "0.55",
        ],
        "Generate prediction alerts",
    )


@task(retries=1, retry_delay_seconds=5)
def log_mlflow_runs():
    run_command(
        ["python", "ml/tracking/log_mlflow_runs.py", "--all"],
        "Log MLflow runs",
    )


@task(retries=1, retry_delay_seconds=5)
def archive_outputs_to_hdfs():
    run_command(
        ["bash", "scripts/archive_to_hdfs.sh"],
        "Archive outputs to HDFS",
    )


@flow(name="market-intel-ml-refresh")
def ml_refresh_flow():
    build_features()

    train_classification_models()
    select_champion_model()

    train_regression_models()

    load_predictions_to_cassandra()
    generate_prediction_alerts()

    log_mlflow_runs()
    archive_outputs_to_hdfs()


if __name__ == "__main__":
    ml_refresh_flow()
