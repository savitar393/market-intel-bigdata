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
def validate_databento():
    run_command(
        ["python", "services/producers/validate_databento.py"],
        "Validate Databento",
    )


@task(retries=1, retry_delay_seconds=5)
def validate_finnhub():
    run_command(
        ["python", "services/producers/validate_finnhub.py"],
        "Validate Finnhub",
    )


@task(retries=1, retry_delay_seconds=5)
def validate_yfinance_live():
    run_command(
        ["python", "services/producers/validate_yfinance_live.py"],
        "Validate yfinance live WebSocket",
    )


@task(retries=1, retry_delay_seconds=5)
def validate_finnhub_ws():
    run_command(
        [
            "python",
            "services/producers/validate_finnhub_ws.py",
            "--symbols",
            "BINANCE:BTCUSDT",
            "--max-messages",
            "10",
        ],
        "Validate Finnhub WebSocket",
    )


@flow(name="market-intel-source-validation")
def source_validation_flow():
    validate_databento()
    validate_finnhub()
    validate_yfinance_live()
    validate_finnhub_ws()


if __name__ == "__main__":
    source_validation_flow()
