import os

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    coalesce,
    current_timestamp,
    expr,
    from_json,
    to_timestamp,
)
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
    "SPARK_MARKET_CASSANDRA_CHECKPOINT",
    "data/checkpoints/spark_market_ticks_cassandra",
)

CASSANDRA_HOSTS = [
    h.strip()
    for h in os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
    if h.strip()
]
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "market_intel")

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


def write_batch_to_cassandra(batch_df, batch_id: int):
    rows = list(batch_df.toLocalIterator())

    if not rows:
        print(f"Batch {batch_id}: no rows to write.")
        return

    cluster = Cluster(CASSANDRA_HOSTS, port=CASSANDRA_PORT)
    session = cluster.connect(CASSANDRA_KEYSPACE)

    insert_query = """
        INSERT INTO market_ticks_by_symbol (
            symbol,
            event_time,
            event_id,
            event_type,
            source,
            dataset,
            schema_name,
            market_price,
            price,
            open,
            high,
            low,
            close,
            volume,
            ingest_time,
            spark_process_time,
            ingest_latency_seconds,
            raw_json
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    written = 0

    for row in rows:
        session.execute(
            insert_query,
            (
                row.symbol,
                row.event_time,
                row.event_id,
                row.event_type,
                row.source,
                row.dataset,
                row.schema_name,
                row.market_price,
                row.price,
                row.open,
                row.high,
                row.low,
                row.close,
                row.volume,
                row.ingest_time,
                row.spark_process_time,
                float(row.ingest_latency_seconds)
                if row.ingest_latency_seconds is not None
                else None,
                row.raw_json,
            ),
        )
        written += 1

    cluster.shutdown()

    print(f"Batch {batch_id}: wrote {written} rows to Cassandra.")


def main():
    spark = (
        SparkSession.builder
        .appName("market-ticks-cassandra-writer")
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

    normalized = (
        parsed
        .withColumn("market_price", coalesce(col("price"), col("close")))
        .withColumn("event_time", to_timestamp(col("event_ts")))
        .withColumn("ingest_time", to_timestamp(col("ingest_ts")))
        .withColumn("spark_process_time", current_timestamp())
        .withColumn("schema_name", col("schema"))
        .withColumn(
            "ingest_latency_seconds",
            expr("unix_timestamp(spark_process_time) - unix_timestamp(ingest_time)")
        )
        .select(
            "event_id",
            "event_type",
            "symbol",
            "source",
            "dataset",
            "schema_name",
            "event_time",
            "ingest_time",
            "spark_process_time",
            "market_price",
            "price",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "ingest_latency_seconds",
            "raw_json",
        )
        .where(col("symbol").isNotNull())
        .where(col("event_time").isNotNull())
        .where(col("event_id").isNotNull())
        .where(col("market_price").isNotNull())
    )

    query = (
        normalized.writeStream
        .foreachBatch(write_batch_to_cassandra)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("Spark Cassandra market writer started.")
    print(f"Kafka topic: {TOPIC}")
    print(f"Kafka bootstrap servers: {BOOTSTRAP_SERVERS}")
    print(f"Cassandra hosts: {CASSANDRA_HOSTS}:{CASSANDRA_PORT}")
    print(f"Cassandra keyspace: {CASSANDRA_KEYSPACE}")
    print(f"Checkpoint: {CHECKPOINT_LOCATION}")

    query.awaitTermination()


if __name__ == "__main__":
    main()
