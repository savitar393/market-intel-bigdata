import argparse
import json
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.feature import StandardScalerModel, VectorAssembler
from pyspark.sql import SparkSession


DEFAULT_SPARK_MODEL_PATH = "data/model_artifacts/baseline_models/logistic_regression"
DEFAULT_OUTPUT_PATH = "data/model_artifacts/online_serving/logistic_regression_online.json"
DEFAULT_SERVING_CONFIG_PATH = "config/final_model_serving.json"


def vector_to_list(value):
    if value is None:
        return None

    if hasattr(value, "toArray"):
        return [float(x) for x in value.toArray().tolist()]

    return [float(x) for x in list(value)]


def main():
    parser = argparse.ArgumentParser(
        description="Export Spark Logistic Regression pipeline to lightweight JSON for online inference."
    )
    parser.add_argument(
        "--spark-model-path",
        default=DEFAULT_SPARK_MODEL_PATH,
    )
    parser.add_argument(
        "--output-path",
        default=DEFAULT_OUTPUT_PATH,
    )
    parser.add_argument(
        "--serving-config-path",
        default=DEFAULT_SERVING_CONFIG_PATH,
    )

    args = parser.parse_args()

    spark_model_path = Path(args.spark_model_path)
    output_path = Path(args.output_path)
    serving_config_path = Path(args.serving_config_path)

    if not spark_model_path.exists():
        raise FileNotFoundError(
            f"Spark model path not found: {spark_model_path}. "
            "Run CLASSIFICATION_LABEL_COLUMN=target_direction_1 spark-submit ml/training/train_spark_baseline_models.py first."
        )

    spark = (
        SparkSession.builder
        .appName("export-logistic-regression-online")
        .master("local[*]")
        .getOrCreate()
    )

    model = PipelineModel.load(str(spark_model_path))

    assembler = None
    scaler = None
    lr_model = None

    for stage in model.stages:
        if isinstance(stage, VectorAssembler):
            assembler = stage
        elif isinstance(stage, StandardScalerModel):
            scaler = stage
        elif isinstance(stage, LogisticRegressionModel):
            lr_model = stage

    if assembler is None:
        raise RuntimeError("VectorAssembler stage not found in Spark pipeline.")

    if lr_model is None:
        raise RuntimeError("LogisticRegressionModel stage not found in Spark pipeline.")

    feature_columns = list(assembler.getInputCols())
    coefficients = vector_to_list(lr_model.coefficients)
    intercept = float(lr_model.intercept)

    scaler_mean = None
    scaler_std = None

    if scaler is not None:
        scaler_mean = vector_to_list(scaler.mean)
        scaler_std = vector_to_list(scaler.std)

    threshold = 0.47

    if serving_config_path.exists():
        with serving_config_path.open("r", encoding="utf-8") as f:
            serving_config = json.load(f)
        threshold = float(serving_config.get("threshold", threshold))

    payload = {
        "model_name": "logistic_regression",
        "task": "online_direction_classification",
        "horizon_minutes": 1,
        "threshold": threshold,
        "feature_columns": feature_columns,
        "coefficients": coefficients,
        "intercept": intercept,
        "scaler": {
            "mean": scaler_mean,
            "std": scaler_std,
        },
        "notes": [
            "Exported from Spark ML Logistic Regression PipelineModel.",
            "Online service computes matching feature columns from live 1-minute bars.",
            "probability_up = sigmoid(dot(scaled_features, coefficients) + intercept).",
        ],
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"Exported online model to: {output_path}")
    print(f"Feature count: {len(feature_columns)}")
    print(f"Threshold: {threshold}")

    spark.stop()


if __name__ == "__main__":
    main()
