import json
import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from cassandra.cluster import Cluster
from dotenv import load_dotenv
from pyspark.sql import SparkSession


MODEL_ARTIFACT_DIR = Path(
    os.getenv("MODEL_ARTIFACT_DIR", "data/model_artifacts/baseline_models")
)

FINAL_SERVING_CONFIG_PATH = Path(
    os.getenv("FINAL_MODEL_SERVING_CONFIG", "config/final_model_serving.json")
)

CASSANDRA_HOSTS = [
    h.strip()
    for h in os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
    if h.strip()
]
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "market_intel")


def load_serving_config() -> dict:
    if FINAL_SERVING_CONFIG_PATH.exists():
        with FINAL_SERVING_CONFIG_PATH.open("r", encoding="utf-8") as f:
            return json.load(f)

    return {
        "model_name": "logistic_regression",
        "threshold": 0.47,
        "decision_rule": "probability_up >= 0.47",
        "source": "default_fallback",
    }


def extract_probability(value):
    if value is None:
        return None, None

    try:
        arr = value.toArray().tolist()
    except Exception:
        try:
            arr = list(value)
        except Exception:
            return None, None

    probability_down = float(arr[0]) if len(arr) > 0 else None
    probability_up = float(arr[1]) if len(arr) > 1 else None

    return probability_down, probability_up


def main():
    load_dotenv()

    config = load_serving_config()

    model_name = config.get("model_name", "logistic_regression")
    threshold = float(config.get("threshold", 0.47))

    predictions_path = MODEL_ARTIFACT_DIR / f"{model_name}_predictions"

    if not predictions_path.exists():
        raise FileNotFoundError(
            f"Predictions parquet not found: {predictions_path}. "
            "Run CLASSIFICATION_LABEL_COLUMN=target_direction spark-submit "
            "ml/training/train_spark_baseline_models.py first."
        )

    print("Final serving config:")
    print(json.dumps(config, indent=2))
    print(f"Reading predictions from: {predictions_path}")

    spark = (
        SparkSession.builder
        .appName("load-final-tuned-predictions-to-cassandra")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(str(predictions_path))

    print("Prediction preview:")
    df.show(20, truncate=False)

    rows = list(df.toLocalIterator())

    cluster = Cluster(CASSANDRA_HOSTS, port=CASSANDRA_PORT)
    session = cluster.connect(CASSANDRA_KEYSPACE)

    insert_query = """
        INSERT INTO model_predictions_by_symbol (
            symbol,
            event_time,
            prediction_time,
            event_id,
            model_name,
            market_price,
            predicted_direction,
            probability_down,
            probability_up,
            target_direction,
            source
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    prediction_time = datetime.now(timezone.utc)
    written = 0

    for row in rows:
        probability_down, probability_up = extract_probability(row.probability)

        if probability_up is not None:
            tuned_prediction = 1 if probability_up >= threshold else 0
        elif getattr(row, "prediction", None) is not None:
            tuned_prediction = int(row.prediction)
        else:
            tuned_prediction = None

        session.execute(
            insert_query,
            (
                row.symbol,
                row.event_minute,
                prediction_time,
                str(uuid4()),
                model_name,
                float(row.market_price) if row.market_price is not None else None,
                tuned_prediction,
                probability_down,
                probability_up,
                int(row.target_direction) if row.target_direction is not None else None,
                "spark_batch_prediction_loader_tuned_threshold",
            ),
        )

        written += 1

    cluster.shutdown()
    spark.stop()

    print(f"Wrote {written} tuned predictions to Cassandra.")
    print(f"Served model: {model_name}")
    print(f"Decision threshold: probability_up >= {threshold}")


if __name__ == "__main__":
    main()
