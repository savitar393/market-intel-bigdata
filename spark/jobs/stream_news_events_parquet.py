import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    expr,
    from_json,
    length,
    lower,
    to_date,
    to_timestamp,
    when,
)
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_NEWS_TOPIC", "raw_news_events")
STARTING_OFFSETS = os.getenv("SPARK_STARTING_OFFSETS", "latest")

OUTPUT_PATH = os.getenv(
    "SPARK_NEWS_PARQUET_PATH",
    "data/processed/news_events",
)

CHECKPOINT_LOCATION = os.getenv(
    "SPARK_NEWS_PARQUET_CHECKPOINT",
    "data/checkpoints/spark_news_events_parquet",
)

news_schema = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("payload_version", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("source", StringType(), True),
        StructField("dataset", StringType(), True),
        StructField("schema", StringType(), True),
        StructField("event_ts", StringType(), True),
        StructField("ingest_ts", StringType(), True),
        StructField("headline", StringType(), True),
        StructField("summary", StringType(), True),
        StructField("category", StringType(), True),
        StructField("url", StringType(), True),
        StructField("image", StringType(), True),
        StructField("related", StringType(), True),
    ]
)


def main():
    spark = (
        SparkSession.builder
        .appName("news-events-parquet-writer")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .load()
    )

    parsed = (
        raw.selectExpr(
            "CAST(key AS STRING) AS kafka_key",
            "CAST(value AS STRING) AS raw_json",
            "timestamp AS kafka_timestamp",
            "partition",
            "offset",
        )
        .select(
            "kafka_key",
            "raw_json",
            "kafka_timestamp",
            "partition",
            "offset",
            from_json(col("raw_json"), news_schema).alias("event"),
        )
        .select(
            "kafka_key",
            "raw_json",
            "kafka_timestamp",
            "partition",
            "offset",
            "event.*",
        )
    )

    # Simple placeholder sentiment.
    # Later we can replace this with VADER/FinBERT/etc.
    text_col = lower(expr("concat(coalesce(headline, ''), ' ', coalesce(summary, ''))"))

    normalized = (
        parsed
        .withColumn("event_time", to_timestamp(col("event_ts")))
        .withColumn("ingest_time", to_timestamp(col("ingest_ts")))
        .withColumn("spark_process_time", current_timestamp())
        .withColumn("event_date", to_date(col("event_time")))
        .withColumn("text_length", length(expr("concat(coalesce(headline, ''), ' ', coalesce(summary, ''))")))
        .withColumn(
            "simple_sentiment_label",
            when(text_col.rlike("beat|beats|growth|bullish|buy|surge|gain|record|strong|upgrade"), "positive")
            .when(text_col.rlike("miss|falls|bearish|sell|drop|loss|weak|downgrade|risk|lawsuit"), "negative")
            .otherwise("neutral")
        )
        .withColumn(
            "simple_sentiment_score",
            when(col("simple_sentiment_label") == "positive", 1.0)
            .when(col("simple_sentiment_label") == "negative", -1.0)
            .otherwise(0.0)
        )
        .withColumn(
            "ingest_latency_seconds",
            expr("unix_timestamp(spark_process_time) - unix_timestamp(ingest_time)")
        )
        .select(
            "event_id",
            "payload_version",
            "event_type",
            "symbol",
            "source",
            "dataset",
            "schema",
            "event_time",
            "event_date",
            "ingest_time",
            "spark_process_time",
            "headline",
            "summary",
            "category",
            "url",
            "image",
            "related",
            "text_length",
            "simple_sentiment_label",
            "simple_sentiment_score",
            "partition",
            "offset",
            "ingest_latency_seconds",
            "raw_json",
        )
        .where(col("symbol").isNotNull())
        .where(col("headline").isNotNull())
    )

    query = (
        normalized.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .partitionBy("symbol", "event_date")
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("Spark news Parquet writer started.")
    print(f"Reading Kafka topic: {TOPIC}")
    print(f"Bootstrap servers: {BOOTSTRAP_SERVERS}")
    print(f"Starting offsets: {STARTING_OFFSETS}")
    print(f"Output path: {OUTPUT_PATH}")
    print(f"Checkpoint: {CHECKPOINT_LOCATION}")

    query.awaitTermination()


if __name__ == "__main__":
    main()
