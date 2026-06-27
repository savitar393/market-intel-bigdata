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

CHAMPION_PATH = MODEL_ARTIFACT_DIR / "champion_model.json"

FINAL_SERVING_CONFIG_PATH = Path(
    os.getenv("FINAL_SERVING_CONFIG_PATH", "config/final_model_serving.json")
)

CASSANDRA_HOSTS = [
    h.strip()
    for h in os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
    if h.strip()
]
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "market_intel")


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

def load_serving_config():
    if FINAL_SERVING_CONFIG_PATH.exists():
        with FINAL_SERVING_CONFIG_PATH.open("r", encoding="utf-8") as f:
            return json.load(f)

    return None

def tuned_prediction(row, serving_config):
    probability_down, probability_up = extract_probability(row.probability)

    if serving_config is None:
        predicted_direction = int(row.prediction) if row.prediction is not None else None
        return predicted_direction, probability_down, probability_up

    threshold = float(serving_config.get("threshold", 0.5))

    if probability_up is None:
        predicted_direction = int(row.prediction) if row.prediction is not None else None
    else:
        predicted_direction = 1 if probability_up >= threshold else 0

    return predicted_direction, probability_down, probability_up

def main():
    load_dotenv()

    serving_config = load_serving_config()

    if serving_config is not None:
        champion_model = serving_config["model_name"]
        threshold = serving_config.get("threshold")
        print(f"Using final serving config: model={champion_model}, threshold={threshold}")
    else:
        if not CHAMPION_PATH.exists():
            raise FileNotFoundError(
                f"Champion metadata not found: {CHAMPION_PATH}. "
                "Run ml/evaluation/select_champion_model.py first or create config/final_model_serving.json."
            )

        with CHAMPION_PATH.open("r", encoding="utf-8") as f:
            champion_payload = json.load(f)

        champion_model = champion_payload["champion_model"]
        print(f"Using champion metadata: model={champion_model}")

    predictions_path = MODEL_ARTIFACT_DIR / f"{champion_model}_predictions"

    serving_config = load_serving_config()

    if serving_config is not None:
        champion_model = serving_config["model_name"]
        threshold = serving_config.get("threshold")
        print(f"Using final serving config: model={champion_model}, threshold={threshold}")
    else:
        with CHAMPION_PATH.open("r", encoding="utf-8") as f:
            champion_payload = json.load(f)

        champion_model = champion_payload["champion_model"]
        print(f"Using champion metadata: model={champion_model}")

    predictions_path = MODEL_ARTIFACT_DIR / f"{champion_model}_predictions"

    if not predictions_path.exists():
        raise FileNotFoundError(
            f"Predictions parquet not found: {predictions_path}. "
            "Run ml/training/train_spark_baseline_models.py first."
        )

    print(f"Champion model: {champion_model}")
    print(f"Reading predictions from: {predictions_path}")

    spark = (
        SparkSession.builder
        .appName("load-predictions-to-cassandra")
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
        predicted_direction, probability_down, probability_up = tuned_prediction(
            row,
            serving_config,
        )

        session.execute(
            insert_query,
            (
                row.symbol,
                row.event_minute,
                prediction_time,
                str(uuid4()),
                champion_model,
                float(row.market_price) if row.market_price is not None else None,
                predicted_direction,
                probability_down,
                probability_up,
                int(row.target_direction) if row.target_direction is not None else None,
                "spark_batch_prediction_loader_tuned_threshold"
                if serving_config is not None
                else "spark_batch_prediction_loader",
            ),
        )

        written += 1

    cluster.shutdown()
    spark.stop()

    print(f"Wrote {written} predictions to Cassandra.")


if __name__ == "__main__":
    main()
