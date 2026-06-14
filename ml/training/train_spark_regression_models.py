import json
import os
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import (
    GBTRegressor,
    LinearRegression,
    RandomForestRegressor,
)
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    abs as spark_abs,
    avg,
    col,
    count,
    isnan,
    row_number,
    when,
)


FEATURE_PATH = os.getenv(
    "FEATURE_PATH",
    "data/features/model_training_dataset",
)

OUTPUT_DIR = Path(
    os.getenv(
        "REGRESSION_MODEL_OUTPUT_DIR",
        "data/model_artifacts/regression_models",
    )
)

FEATURE_COLUMNS = [
    "market_price",
    "volume",
    "return_1",
    "rolling_mean_3",
    "rolling_volatility_3",
    "news_count",
    "avg_sentiment_score",
    "positive_news_count",
    "negative_news_count",
]

LABEL_COLUMN = "target_next_return"


def safe_metric(evaluator, predictions):
    try:
        return float(evaluator.evaluate(predictions))
    except Exception:
        return None


def compute_mape(predictions):
    # MAPE can be unstable for near-zero returns, so exclude values too close to zero.
    valid = predictions.where(spark_abs(col(LABEL_COLUMN)) > 1e-9)

    if valid.count() == 0:
        return None

    value = (
        valid
        .select(
            (
                avg(
                    spark_abs(
                        (col(LABEL_COLUMN) - col("prediction")) / col(LABEL_COLUMN)
                    )
                )
                * 100.0
            ).alias("mape")
        )
        .first()
        .mape
    )

    return float(value) if value is not None else None


def compute_directional_accuracy(predictions):
    # Converts regression output into UP/DOWN direction and compares it to target_direction.
    evaluated = (
        predictions
        .withColumn(
            "predicted_direction_from_return",
            when(col("prediction") > 0, 1).otherwise(0),
        )
        .withColumn(
            "actual_direction_from_return",
            when(col(LABEL_COLUMN) > 0, 1).otherwise(0),
        )
        .withColumn(
            "direction_correct",
            when(
                col("predicted_direction_from_return")
                == col("actual_direction_from_return"),
                1.0,
            ).otherwise(0.0),
        )
    )

    value = evaluated.select(avg("direction_correct").alias("directional_accuracy")).first().directional_accuracy

    return float(value) if value is not None else None


def main():
    spark = (
        SparkSession.builder
        .appName("train-regression-market-models")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading feature dataset: {FEATURE_PATH}")
    df = spark.read.parquet(FEATURE_PATH)

    print("Raw feature schema:")
    df.printSchema()

    cleaned = df

    for c in FEATURE_COLUMNS:
        cleaned = cleaned.withColumn(
            c,
            when(col(c).isNull() | isnan(col(c)), 0.0).otherwise(col(c)),
        )

    cleaned = (
        cleaned
        .where(col(LABEL_COLUMN).isNotNull())
        .where(~isnan(col(LABEL_COLUMN)))
        .withColumn(LABEL_COLUMN, col(LABEL_COLUMN).cast("double"))
        .cache()
    )

    row_count = cleaned.count()
    print(f"Regression row count: {row_count}")

    if row_count < 30:
        raise RuntimeError(
            "Not enough rows for regression training. Generate more feature rows first."
        )

    print("Label summary:")
    cleaned.select(LABEL_COLUMN).summary().show()

    # Time-aware split per symbol to reduce future leakage compared with random split.
    symbol_window = Window.partitionBy("symbol").orderBy("event_minute")
    symbol_count_window = Window.partitionBy("symbol")

    indexed = (
        cleaned
        .withColumn("_row_num", row_number().over(symbol_window))
        .withColumn("_symbol_count", count("*").over(symbol_count_window))
        .withColumn("_train_cutoff", col("_symbol_count") * 0.7)
    )

    train_df = indexed.where(col("_row_num") <= col("_train_cutoff")).cache()
    test_df = indexed.where(col("_row_num") > col("_train_cutoff")).cache()

    train_rows = train_df.count()
    test_rows = test_df.count()

    print(f"Train rows: {train_rows}")
    print(f"Test rows: {test_rows}")

    if train_rows == 0 or test_rows == 0:
        raise RuntimeError("Time-based split produced empty train or test set.")

    print("Train rows by symbol:")
    train_df.groupBy("symbol").count().show()

    print("Test rows by symbol:")
    test_df.groupBy("symbol").count().show()

    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS,
        outputCol="features",
        handleInvalid="keep",
    )

    models = {
        "linear_regression_return": LinearRegression(
            featuresCol="features",
            labelCol=LABEL_COLUMN,
            maxIter=50,
            regParam=0.01,
            elasticNetParam=0.0,
        ),
        "random_forest_regression": RandomForestRegressor(
            featuresCol="features",
            labelCol=LABEL_COLUMN,
            numTrees=80,
            maxDepth=6,
            seed=42,
        ),
        "gradient_boosted_tree_regression": GBTRegressor(
            featuresCol="features",
            labelCol=LABEL_COLUMN,
            maxIter=50,
            maxDepth=4,
            seed=42,
        ),
    }

    rmse_eval = RegressionEvaluator(
        labelCol=LABEL_COLUMN,
        predictionCol="prediction",
        metricName="rmse",
    )
    mae_eval = RegressionEvaluator(
        labelCol=LABEL_COLUMN,
        predictionCol="prediction",
        metricName="mae",
    )
    r2_eval = RegressionEvaluator(
        labelCol=LABEL_COLUMN,
        predictionCol="prediction",
        metricName="r2",
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    results = []

    for model_name, estimator in models.items():
        print(f"\n=== Training {model_name} ===")

        pipeline = Pipeline(stages=[assembler, estimator])

        try:
            fitted = pipeline.fit(train_df)
            predictions = fitted.transform(test_df).cache()

            metrics = {
                "model_name": model_name,
                "task": "next_return_regression",
                "train_rows": train_rows,
                "test_rows": test_rows,
                "rmse": safe_metric(rmse_eval, predictions),
                "mae": safe_metric(mae_eval, predictions),
                "r2": safe_metric(r2_eval, predictions),
                "mape_percent": compute_mape(predictions),
                "directional_accuracy": compute_directional_accuracy(predictions),
                "feature_columns": FEATURE_COLUMNS,
                "label_column": LABEL_COLUMN,
                "split_strategy": "time_based_70_30_per_symbol",
                "note": (
                    "MAPE may be unstable for return targets close to zero; "
                    "RMSE, MAE, R2, and directional accuracy should be interpreted together."
                ),
            }

            print(json.dumps(metrics, indent=2))
            results.append(metrics)

            model_path = OUTPUT_DIR / model_name
            fitted.write().overwrite().save(str(model_path))
            print(f"Saved model to: {model_path}")

            pred_path = OUTPUT_DIR / f"{model_name}_predictions"
            (
                predictions
                .withColumn(
                    "predicted_direction_from_return",
                    when(col("prediction") > 0, 1).otherwise(0),
                )
                .select(
                    "symbol",
                    "event_minute",
                    "market_price",
                    LABEL_COLUMN,
                    "prediction",
                    "target_direction",
                    "predicted_direction_from_return",
                )
                .write
                .mode("overwrite")
                .parquet(str(pred_path))
            )
            print(f"Saved predictions to: {pred_path}")

        except Exception as exc:
            print(f"FAILED training {model_name}: {exc}")
            results.append(
                {
                    "model_name": model_name,
                    "task": "next_return_regression",
                    "status": "failed",
                    "error": str(exc),
                }
            )

    metrics_path = OUTPUT_DIR / "regression_model_metrics.json"

    with metrics_path.open("w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    print("\nFinal regression metrics:")
    print(json.dumps(results, indent=2))
    print(f"Saved metrics to: {metrics_path}")

    spark.stop()


if __name__ == "__main__":
    main()
