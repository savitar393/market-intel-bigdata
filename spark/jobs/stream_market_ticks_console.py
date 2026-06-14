import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, current_timestamp, expr, from_json, to_timestamp
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_MARKET_TOPIC", "raw_market_ticks")
STARTING_OFFSETS = os.getenv("SPARK_STARTING_OFFSETS", "latest")
CHECKPOINT_LOCATION = os.getenv(
    "SPARK_MARKET_CHECKPOINT",
    "data/checkpoints/spark_market_ticks_console",
)

market_schema = StructType(
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
        StructField("price", DoubleType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True),
        StructField("change", DoubleType(), True),
        StructField("change_percent", DoubleType(), True),
        StructField("exchange", StringType(), True),
        StructField("market_hours", StringType(), True),
    ]
)


def main():
    spark = (
        SparkSession.builder
        .appName("market-ticks-kafka-stream")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
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
            from_json(col("raw_json"), market_schema).alias("event"),
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

    enriched = (
        parsed
        .withColumn("market_price", coalesce(col("price"), col("close")))
        .withColumn("event_time", to_timestamp(col("event_ts")))
        .withColumn("ingest_time", to_timestamp(col("ingest_ts")))
        .withColumn("spark_process_time", current_timestamp())
        .withColumn(
            "ingest_latency_seconds",
            expr("unix_timestamp(spark_process_time) - unix_timestamp(ingest_time)")
        )
        .select(
            "event_time",
            "symbol",
            "event_type",
            "market_price",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "source",
            "partition",
            "offset",
            "ingest_latency_seconds",
        )
    )

    query = (
        enriched.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", "30")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="5 seconds")
        .start()
    )

    print("Spark Structured Streaming job started.")
    print(f"Reading Kafka topic: {TOPIC}")
    print(f"Bootstrap servers: {BOOTSTRAP_SERVERS}")
    print(f"Starting offsets: {STARTING_OFFSETS}")
    print(f"Checkpoint: {CHECKPOINT_LOCATION}")

    query.awaitTermination()


if __name__ == "__main__":
    main()
