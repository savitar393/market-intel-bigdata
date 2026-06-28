import json
import os
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    GBTClassifier,
    LogisticRegression,
    RandomForestClassifier,
)
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import col, isnan, when, count, row_number


FEATURE_PATH = os.getenv(
    "FEATURE_PATH",
    "data/features/model_training_dataset",
)

OUTPUT_DIR = Path(
    os.getenv(
        "MODEL_OUTPUT_DIR",
        "data/model_artifacts/baseline_models",
    )
)

FEATURE_COLUMNS = [
    "market_price",
    "volume",
    "return_1",
    "return_2",
    "return_3",
    "return_5",
    "return_10",
    "rolling_mean_3",
    "rolling_mean_5",
    "rolling_mean_10",
    "rolling_mean_30",
    "rolling_price_std_5",
    "rolling_price_std_10",
    "rolling_price_std_30",
    "rolling_volatility_3",
    "rolling_volatility_5",
    "rolling_volatility_10",
    "rolling_volatility_30",
    "rolling_volume_mean_5",
    "rolling_volume_mean_10",
    "volume_surprise_5",
    "volume_surprise_10",
    "log_volume",
    "bar_range",
    "candle_body",
    "upper_shadow",
    "lower_shadow",
    "minute_of_day",
    "news_count",
    "avg_sentiment_score",
    "positive_news_count",
    "negative_news_count",
    "open_close",
    "low_high",
    "daily_return_1",
    "daily_return_5",
    "daily_return_20",
    "ma_20_ratio",
    "ma_50_ratio",
    "ma_100_ratio",
    "ma_200_ratio",
    "daily_volatility_20",
    "volume_surprise_20",
    "dividends",
    "stock_splits",
]

LABEL_COLUMN = os.getenv("CLASSIFICATION_LABEL_COLUMN", "target_direction")


def safe_metric(evaluator, predictions):
    try:
        return float(evaluator.evaluate(predictions))
    except Exception:
        return None


def main():
    spark = (
        SparkSession.builder
        .appName("train-baseline-market-models")
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

    # Make training robust for early small datasets.
    cleaned = df

    for c in FEATURE_COLUMNS:
        cleaned = cleaned.withColumn(
            c,
            when(col(c).isNull() | isnan(col(c)), 0.0).otherwise(col(c)),
        )

    cleaned = (
        cleaned
        .where(col(LABEL_COLUMN).isNotNull())
        .withColumn(LABEL_COLUMN, col(LABEL_COLUMN).cast("double"))
    )

    row_count = cleaned.count()
    print(f"Training row count: {row_count}")

    if row_count < 6:
        raise RuntimeError(
            "Not enough rows for training. Generate more Databento/yfinance data first."
        )

    print("Class distribution:")
    cleaned.groupBy(LABEL_COLUMN).count().show()

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

    if train_df.count() == 0 or test_df.count() == 0:
        raise RuntimeError("Train/test split produced empty data. Generate more rows.")

    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS,
        outputCol="features",
        handleInvalid="keep",
    )

    models = {
        "logistic_regression": LogisticRegression(
            featuresCol="features",
            labelCol=LABEL_COLUMN,
            maxIter=50,
        ),
        "random_forest": RandomForestClassifier(
            featuresCol="features",
            labelCol=LABEL_COLUMN,
            numTrees=50,
            maxDepth=5,
            seed=42,
        ),
        "gradient_boosted_trees": GBTClassifier(
            featuresCol="features",
            labelCol=LABEL_COLUMN,
            maxIter=30,
            maxDepth=3,
            seed=42,
        ),
    }

    accuracy_eval = MulticlassClassificationEvaluator(
        labelCol=LABEL_COLUMN,
        predictionCol="prediction",
        metricName="accuracy",
    )
    f1_eval = MulticlassClassificationEvaluator(
        labelCol=LABEL_COLUMN,
        predictionCol="prediction",
        metricName="f1",
    )
    precision_eval = MulticlassClassificationEvaluator(
        labelCol=LABEL_COLUMN,
        predictionCol="prediction",
        metricName="weightedPrecision",
    )
    recall_eval = MulticlassClassificationEvaluator(
        labelCol=LABEL_COLUMN,
        predictionCol="prediction",
        metricName="weightedRecall",
    )
    auc_eval = BinaryClassificationEvaluator(
        labelCol=LABEL_COLUMN,
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    results = []

    for model_name, estimator in models.items():
        print(f"\n=== Training {model_name} ===")

        pipeline = Pipeline(stages=[assembler, estimator])

        try:
            fitted = pipeline.fit(train_df)
            predictions = fitted.transform(test_df)

            metrics = {
                "model_name": model_name,
                "train_rows": train_df.count(),
                "test_rows": test_df.count(),
                "accuracy": safe_metric(accuracy_eval, predictions),
                "f1": safe_metric(f1_eval, predictions),
                "weighted_precision": safe_metric(precision_eval, predictions),
                "weighted_recall": safe_metric(recall_eval, predictions),
                "roc_auc": safe_metric(auc_eval, predictions),
                "feature_columns": FEATURE_COLUMNS,
                "label_column": LABEL_COLUMN,
                "split_strategy": "time_based_70_30_per_symbol",
            }

            print(json.dumps(metrics, indent=2))
            results.append(metrics)

            model_path = OUTPUT_DIR / model_name
            fitted.write().overwrite().save(str(model_path))
            print(f"Saved model to: {model_path}")

            pred_path = OUTPUT_DIR / f"{model_name}_predictions"
            (
                predictions
                .select(
                    "symbol",
                    "event_minute",
                    "market_price",
                    "target_direction",
                    "prediction",
                    "probability",
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
                    "status": "failed",
                    "error": str(exc),
                }
            )

    metrics_path = OUTPUT_DIR / "model_metrics.json"

    with metrics_path.open("w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    print("\nFinal metrics:")
    print(json.dumps(results, indent=2))
    print(f"Saved metrics to: {metrics_path}")

    spark.stop()


if __name__ == "__main__":
    main()
