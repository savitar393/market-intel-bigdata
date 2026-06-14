import os

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    count,
    date_trunc,
    first,
    lag,
    lead,
    lit,
    stddev,
    sum as spark_sum,
    to_date,
    when,
)

MARKET_PATH = os.getenv("MARKET_PARQUET_PATH", "data/processed/market_ticks")
NEWS_PATH = os.getenv("NEWS_PARQUET_PATH", "data/processed/news_events")
OUTPUT_PATH = os.getenv("FEATURE_OUTPUT_PATH", "data/features/model_training_dataset")


def main():
    spark = (
        SparkSession.builder
        .appName("market-news-feature-builder")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("Reading market data:", MARKET_PATH)
    market = spark.read.parquet(MARKET_PATH)

    print("Reading news data:", NEWS_PATH)
    news = spark.read.parquet(NEWS_PATH)

    # Normalize market events to one row per symbol-minute.
    # For OHLCV bars this is already minute-like.
    # For yfinance ticks, this compresses multiple ticks into the same minute.
    market_minute = (
        market
        .where(col("symbol").isNotNull())
        .where(col("market_price").isNotNull())
        .withColumn("event_minute", date_trunc("minute", col("event_time")))
        .groupBy("symbol", "event_minute")
        .agg(
            first("event_type", ignorenulls=True).alias("event_type"),
            first("source", ignorenulls=True).alias("source"),
            avg("market_price").alias("market_price"),
            avg("open").alias("open"),
            avg("high").alias("high"),
            avg("low").alias("low"),
            avg("close").alias("close"),
            spark_sum(coalesce(col("volume"), lit(0))).alias("volume"),
        )
        .withColumn("event_date", to_date(col("event_minute")))
    )

    # Aggregate news by symbol and date.
    # This is coarse for now, but good enough to validate multimodal feature flow.
    news_daily = (
        news
        .where(col("symbol").isNotNull())
        .withColumn("news_date", to_date(col("event_time")))
        .groupBy("symbol", "news_date")
        .agg(
            count("*").alias("news_count"),
            avg("simple_sentiment_score").alias("avg_sentiment_score"),
            spark_sum(
                when(col("simple_sentiment_label") == "positive", 1).otherwise(0)
            ).alias("positive_news_count"),
            spark_sum(
                when(col("simple_sentiment_label") == "negative", 1).otherwise(0)
            ).alias("negative_news_count"),
        )
    )

    joined = (
        market_minute
        .join(
            news_daily,
            (market_minute.symbol == news_daily.symbol)
            & (market_minute.event_date == news_daily.news_date),
            "left",
        )
        .drop(news_daily.symbol)
        .drop("news_date")
        .fillna(
            {
                "news_count": 0,
                "avg_sentiment_score": 0.0,
                "positive_news_count": 0,
                "negative_news_count": 0,
            }
        )
    )

    w = Window.partitionBy("symbol").orderBy("event_minute")
    rolling_3 = w.rowsBetween(-2, 0)

    features = (
        joined
        .withColumn("prev_price", lag("market_price", 1).over(w))
        .withColumn(
            "return_1",
            (col("market_price") - col("prev_price")) / col("prev_price")
        )
        .withColumn("rolling_mean_3", avg("market_price").over(rolling_3))
        .withColumn("rolling_volatility_3", stddev("return_1").over(rolling_3))
        .withColumn("next_price", lead("market_price", 1).over(w))
        .withColumn(
            "target_next_return",
            (col("next_price") - col("market_price")) / col("market_price")
        )
        .withColumn(
            "target_direction",
            when(col("target_next_return") > 0, 1).otherwise(0)
        )
        .select(
            "symbol",
            "event_minute",
            "event_date",
            "event_type",
            "source",
            "market_price",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "return_1",
            "rolling_mean_3",
            "rolling_volatility_3",
            "news_count",
            "avg_sentiment_score",
            "positive_news_count",
            "negative_news_count",
            "target_next_return",
            "target_direction",
        )
        .where(col("prev_price").isNotNull())
        .where(col("next_price").isNotNull())
    )

    print("Feature preview:")
    features.orderBy("symbol", "event_minute").show(50, truncate=False)

    print("Writing features to:", OUTPUT_PATH)
    (
        features
        .write
        .mode("overwrite")
        .partitionBy("symbol")
        .parquet(OUTPUT_PATH)
    )

    print("Feature row count:", features.count())
    print("Done.")

    spark.stop()


if __name__ == "__main__":
    main()
