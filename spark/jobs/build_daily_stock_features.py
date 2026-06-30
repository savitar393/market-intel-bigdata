import json
import os
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    avg,
    col,
    lag,
    lit,
    row_number,
    stddev,
    to_date,
    when,
)


DAILY_INPUT_PATH = os.getenv(
    "DAILY_STOCK_CSV_PATH",
    "data/processed/daily_stock_prices/daily_stock_prices.csv",
)

DAILY_FEATURE_OUTPUT_PATH = os.getenv(
    "DAILY_FEATURE_OUTPUT_PATH",
    "data/features/daily_stock_features",
)

DAILY_LATEST_CONTEXT_PATH = os.getenv(
    "DAILY_LATEST_CONTEXT_PATH",
    "data/features/daily_stock_context_latest.json",
)

DAILY_TREND_OUTPUT_PATH = os.getenv(
    "DAILY_TREND_OUTPUT_PATH",
    "data/features/daily_stock_trend.json",
)

DAILY_CONTEXT_COLUMNS = [
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


def main():
    spark = (
        SparkSession.builder
        .appName("daily-stock-feature-builder")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading daily CSV: {DAILY_INPUT_PATH}")

    raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(DAILY_INPUT_PATH)
    )

    for c in [
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
        "dividends",
        "stock_splits",
    ]:
        raw = raw.withColumn(c, col(c).cast("double"))

    daily = (
        raw
        .where(col("symbol").isNotNull())
        .where(col("date").isNotNull())
        .where(col("close").isNotNull())
        .withColumn("event_date", to_date(col("date")))
    )

    w = Window.partitionBy("symbol").orderBy("event_date")
    rolling_20 = w.rowsBetween(-19, 0)
    rolling_50 = w.rowsBetween(-49, 0)
    rolling_100 = w.rowsBetween(-99, 0)
    rolling_200 = w.rowsBetween(-199, 0)

    features = (
        daily
        .withColumn("prev_close_1", lag("close", 1).over(w))
        .withColumn("prev_close_5", lag("close", 5).over(w))
        .withColumn("prev_close_20", lag("close", 20).over(w))

        # Lecturer alignment:
        # open-close and low-high are normalized to reduce raw price-scale domination.
        .withColumn(
            "open_close",
            when(col("open") != 0, (col("close") - col("open")) / col("open"))
            .otherwise(0.0),
        )
        .withColumn(
            "low_high",
            when(col("close") != 0, (col("high") - col("low")) / col("close"))
            .otherwise(0.0),
        )

        .withColumn(
            "daily_return_1",
            when(col("prev_close_1") != 0, (col("close") - col("prev_close_1")) / col("prev_close_1"))
            .otherwise(0.0),
        )
        .withColumn(
            "daily_return_5",
            when(col("prev_close_5") != 0, (col("close") - col("prev_close_5")) / col("prev_close_5"))
            .otherwise(0.0),
        )
        .withColumn(
            "daily_return_20",
            when(col("prev_close_20") != 0, (col("close") - col("prev_close_20")) / col("prev_close_20"))
            .otherwise(0.0),
        )

        .withColumn("ma_20", avg("close").over(rolling_20))
        .withColumn("ma_50", avg("close").over(rolling_50))
        .withColumn("ma_100", avg("close").over(rolling_100))
        .withColumn("ma_200", avg("close").over(rolling_200))

        .withColumn(
            "ma_20_ratio",
            when(col("ma_20") != 0, col("close") / col("ma_20") - lit(1.0)).otherwise(0.0),
        )
        .withColumn(
            "ma_50_ratio",
            when(col("ma_50") != 0, col("close") / col("ma_50") - lit(1.0)).otherwise(0.0),
        )
        .withColumn(
            "ma_100_ratio",
            when(col("ma_100") != 0, col("close") / col("ma_100") - lit(1.0)).otherwise(0.0),
        )
        .withColumn(
            "ma_200_ratio",
            when(col("ma_200") != 0, col("close") / col("ma_200") - lit(1.0)).otherwise(0.0),
        )

        .withColumn("daily_volatility_20", stddev("daily_return_1").over(rolling_20))
        .withColumn("volume_ma_20", avg("volume").over(rolling_20))
        .withColumn(
            "volume_surprise_20",
            when(col("volume_ma_20") > 0, col("volume") / col("volume_ma_20"))
            .otherwise(0.0),
        )
        .fillna(
            {
                "open_close": 0.0,
                "low_high": 0.0,
                "daily_return_1": 0.0,
                "daily_return_5": 0.0,
                "daily_return_20": 0.0,
                "ma_20_ratio": 0.0,
                "ma_50_ratio": 0.0,
                "ma_100_ratio": 0.0,
                "ma_200_ratio": 0.0,
                "daily_volatility_20": 0.0,
                "volume_surprise_20": 0.0,
                "dividends": 0.0,
                "stock_splits": 0.0,
            }
        )
        .select(
            "symbol",
            "event_date",
            "open",
            "high",
            "low",
            "close",
            "adj_close",
            "volume",
            "dividends",
            "stock_splits",
            "open_close",
            "low_high",
            "daily_return_1",
            "daily_return_5",
            "daily_return_20",
            "ma_20",
            "ma_50",
            "ma_100",
            "ma_200",
            "ma_20_ratio",
            "ma_50_ratio",
            "ma_100_ratio",
            "ma_200_ratio",
            "daily_volatility_20",
            "volume_ma_20",
            "volume_surprise_20",
            "source",
            "ingest_ts",
        )
    )

    print("Daily feature preview:")
    features.orderBy("symbol", "event_date").show(30, truncate=False)

    print(f"Writing daily features to: {DAILY_FEATURE_OUTPUT_PATH}")
    (
        features
        .write
        .mode("overwrite")
        .partitionBy("symbol")
        .parquet(DAILY_FEATURE_OUTPUT_PATH)
    )

    latest_window = Window.partitionBy("symbol").orderBy(col("event_date").desc())

    latest_rows = (
        features
        .withColumn("_rn", row_number().over(latest_window))
        .where(col("_rn") == 1)
        .select("symbol", "event_date", *DAILY_CONTEXT_COLUMNS)
        .collect()
    )

    latest_payload = {}

    for row in latest_rows:
        data = row.asDict()
        symbol = data.pop("symbol")
        data["event_date"] = str(data["event_date"])
        latest_payload[symbol] = {
            key: float(value) if isinstance(value, (int, float)) and value is not None else value
            for key, value in data.items()
        }

    latest_path = Path(DAILY_LATEST_CONTEXT_PATH)
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(json.dumps(latest_payload, indent=2), encoding="utf-8")

    trend_window = Window.partitionBy("symbol").orderBy(col("event_date").desc())

    trend_rows = (
        features
        .withColumn("_rn", row_number().over(trend_window))
        .where(col("_rn") <= 300)
        .select(
            "symbol",
            "event_date",
            "close",
            "ma_100",
            "ma_200",
            "ma_100_ratio",
            "ma_200_ratio",
        )
        .orderBy("symbol", "event_date")
        .collect()
    )

    trend_payload = {}

    for row in trend_rows:
        item = row.asDict()
        symbol = item.pop("symbol")
        item["event_date"] = str(item["event_date"])

        for key in ["close", "ma_100", "ma_200", "ma_100_ratio", "ma_200_ratio"]:
            item[key] = float(item[key]) if item.get(key) is not None else None

        trend_payload.setdefault(symbol, []).append(item)

    trend_path = Path(DAILY_TREND_OUTPUT_PATH)
    trend_path.parent.mkdir(parents=True, exist_ok=True)
    trend_path.write_text(json.dumps(trend_payload, indent=2), encoding="utf-8")

    print(f"Latest daily context written to: {latest_path}")
    print(f"Daily trend chart data written to: {trend_path}")
    print(f"Daily feature row count: {features.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
