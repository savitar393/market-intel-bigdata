import os

from orchestration.ml_refresh_flow import ml_refresh_flow


if __name__ == "__main__":
    deployment_name = os.getenv(
        "PREFECT_ML_REFRESH_DEPLOYMENT_NAME",
        "scheduled-ml-refresh",
    )

    cron = os.getenv(
        "PREFECT_ML_REFRESH_CRON",
        "0 */6 * * *",
    )

    print("Starting Prefect scheduled ML refresh deployment")
    print(f"Deployment name: {deployment_name}")
    print(f"Cron schedule:    {cron}")

    ml_refresh_flow.serve(
        name=deployment_name,
        cron=cron,
    )
