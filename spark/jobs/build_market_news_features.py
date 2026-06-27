import os

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    count,
    date_trunc,
    first,
    hour,
    lag,
    lead,
    lit,
    log1p,
    minute,
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
        .withColumn("open_filled", coalesce(col("open"), col("market_price")))
        .withColumn("high_filled", coalesce(col("high"), col("market_price")))
        .withColumn("low_filled", coalesce(col("low"), col("market_price")))
        .withColumn("close_filled", coalesce(col("close"), col("market_price")))
        .withColumn("volume_filled", coalesce(col("volume"), lit(0.0)))
    )

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
    rolling_5 = w.rowsBetween(-4, 0)
    rolling_10 = w.rowsBetween(-9, 0)
    rolling_30 = w.rowsBetween(-29, 0)

    features = (
        joined
        .withColumn("prev_price_1", lag("market_price", 1).over(w))
        .withColumn("prev_price_2", lag("market_price", 2).over(w))
        .withColumn("prev_price_3", lag("market_price", 3).over(w))
        .withColumn("prev_price_5", lag("market_price", 5).over(w))
        .withColumn("prev_price_10", lag("market_price", 10).over(w))

        .withColumn("return_1", (col("market_price") - col("prev_price_1")) / col("prev_price_1"))
        .withColumn("return_2", (col("market_price") - col("prev_price_2")) / col("prev_price_2"))
        .withColumn("return_3", (col("market_price") - col("prev_price_3")) / col("prev_price_3"))
        .withColumn("return_5", (col("market_price") - col("prev_price_5")) / col("prev_price_5"))
        .withColumn("return_10", (col("market_price") - col("prev_price_10")) / col("prev_price_10"))

        .withColumn("rolling_mean_3", avg("market_price").over(rolling_3))
        .withColumn("rolling_mean_5", avg("market_price").over(rolling_5))
        .withColumn("rolling_mean_10", avg("market_price").over(rolling_10))
        .withColumn("rolling_mean_30", avg("market_price").over(rolling_30))

        .withColumn("rolling_price_std_5", stddev("market_price").over(rolling_5))
        .withColumn("rolling_price_std_10", stddev("market_price").over(rolling_10))
        .withColumn("rolling_price_std_30", stddev("market_price").over(rolling_30))

        .withColumn("rolling_volatility_3", stddev("return_1").over(rolling_3))
        .withColumn("rolling_volatility_5", stddev("return_1").over(rolling_5))
        .withColumn("rolling_volatility_10", stddev("return_1").over(rolling_10))
        .withColumn("rolling_volatility_30", stddev("return_1").over(rolling_30))

        .withColumn("rolling_volume_mean_5", avg("volume_filled").over(rolling_5))
        .withColumn("rolling_volume_mean_10", avg("volume_filled").over(rolling_10))
        .withColumn(
            "volume_surprise_5",
            when(col("rolling_volume_mean_5") > 0, col("volume_filled") / col("rolling_volume_mean_5"))
            .otherwise(0.0)
        )
        .withColumn(
            "volume_surprise_10",
            when(col("rolling_volume_mean_10") > 0, col("volume_filled") / col("rolling_volume_mean_10"))
            .otherwise(0.0)
        )
        .withColumn("log_volume", log1p(col("volume_filled")))

        .withColumn(
            "bar_range",
            when(col("close_filled") != 0, (col("high_filled") - col("low_filled")) / col("close_filled"))
            .otherwise(0.0)
        )
        .withColumn(
            "candle_body",
            when(col("open_filled") != 0, (col("close_filled") - col("open_filled")) / col("open_filled"))
            .otherwise(0.0)
        )
        .withColumn(
            "upper_shadow",
            when(col("close_filled") != 0, (col("high_filled") - col("close_filled")) / col("close_filled"))
            .otherwise(0.0)
        )
        .withColumn(
            "lower_shadow",
            when(col("close_filled") != 0, (col("open_filled") - col("low_filled")) / col("close_filled"))
            .otherwise(0.0)
        )

        .withColumn("minute_of_day", hour(col("event_minute")) * 60 + minute(col("event_minute")))

        .withColumn("next_price_1", lead("market_price", 1).over(w))
        .withColumn("next_price_5", lead("market_price", 5).over(w))
        .withColumn("next_price_10", lead("market_price", 10).over(w))

        .withColumn("target_return_1", (col("next_price_1") - col("market_price")) / col("market_price"))
        .withColumn("target_return_5", (col("next_price_5") - col("market_price")) / col("market_price"))
        .withColumn("target_return_10", (col("next_price_10") - col("market_price")) / col("market_price"))

        .withColumn("target_direction_1", when(col("target_return_1") > 0, 1).otherwise(0))
        .withColumn("target_direction_5", when(col("target_return_5") > 0, 1).otherwise(0))
        .withColumn("target_direction_10", when(col("target_return_10") > 0, 1).otherwise(0))

        # Backward-compatible default labels.
        .withColumn("target_next_return", col("target_return_1"))
        .withColumn("target_direction", col("target_direction_1"))

        .select(
            "target_return_1",
            "target_return_5",
            "target_return_10",
            "target_direction_1",
            "target_direction_5",
            "target_direction_10",
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

            "target_next_return",
            "target_direction",
        )
        .where(col("prev_price_10").isNotNull())
        .where(col("next_price_10").isNotNull())
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
