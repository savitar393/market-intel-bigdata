import os

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    expr,
    from_json,
    length,
    lower,
    to_timestamp,
    when,
)
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_NEWS_TOPIC", "raw_news_events")
STARTING_OFFSETS = os.getenv("SPARK_STARTING_OFFSETS", "latest")

CHECKPOINT_LOCATION = os.getenv(
    "SPARK_NEWS_CASSANDRA_CHECKPOINT",
    "data/checkpoints/spark_news_events_cassandra",
)

CASSANDRA_HOSTS = [
    h.strip()
    for h in os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
    if h.strip()
]
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "market_intel")

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
        StructField("simple_sentiment_label", StringType(), True),
        StructField("simple_sentiment_score", DoubleType(), True),
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
        INSERT INTO news_events_by_symbol (
            symbol,
            event_time,
            event_id,
            source,
            dataset,
            schema_name,
            headline,
            summary,
            category,
            url,
            related,
            simple_sentiment_label,
            simple_sentiment_score,
            ingest_time,
            spark_process_time,
            ingest_latency_seconds,
            raw_json
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    written = 0

    for row in rows:
        session.execute(
            insert_query,
            (
                row.symbol,
                row.event_time,
                row.event_id,
                row.source,
                row.dataset,
                row.schema_name,
                row.headline,
                row.summary,
                row.category,
                row.url,
                row.related,
                row.simple_sentiment_label,
                float(row.simple_sentiment_score)
                if row.simple_sentiment_score is not None
                else None,
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

    print(f"Batch {batch_id}: wrote {written} news rows to Cassandra.")


def main():
    spark = (
        SparkSession.builder
        .appName("news-events-cassandra-writer")
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

    # If the producer already sends simple sentiment fields, use them.
    # If not, compute a fallback rule-based sentiment here.
    text_col = lower(expr("concat(coalesce(headline, ''), ' ', coalesce(summary, ''))"))

    normalized = (
        parsed
        .withColumn("event_time", to_timestamp(col("event_ts")))
        .withColumn("ingest_time", to_timestamp(col("ingest_ts")))
        .withColumn("spark_process_time", current_timestamp())
        .withColumn("schema_name", col("schema"))
        .withColumn("text_length", length(expr("concat(coalesce(headline, ''), ' ', coalesce(summary, ''))")))
        .withColumn(
            "fallback_sentiment_label",
            when(text_col.rlike("beat|beats|growth|bullish|buy|surge|gain|record|strong|upgrade"), "positive")
            .when(text_col.rlike("miss|falls|bearish|sell|drop|loss|weak|downgrade|risk|lawsuit"), "negative")
            .otherwise("neutral")
        )
        .withColumn(
            "simple_sentiment_label",
            when(col("simple_sentiment_label").isNotNull(), col("simple_sentiment_label"))
            .otherwise(col("fallback_sentiment_label"))
        )
        .withColumn(
            "simple_sentiment_score",
            when(col("simple_sentiment_score").isNotNull(), col("simple_sentiment_score"))
            .when(col("simple_sentiment_label") == "positive", 1.0)
            .when(col("simple_sentiment_label") == "negative", -1.0)
            .otherwise(0.0)
        )
        .withColumn(
            "ingest_latency_seconds",
            expr("unix_timestamp(spark_process_time) - unix_timestamp(ingest_time)")
        )
        .select(
            "event_id",
            "symbol",
            "source",
            "dataset",
            "schema_name",
            "event_time",
            "ingest_time",
            "spark_process_time",
            "headline",
            "summary",
            "category",
            "url",
            "related",
            "simple_sentiment_label",
            "simple_sentiment_score",
            "ingest_latency_seconds",
            "raw_json",
        )
        .where(col("symbol").isNotNull())
        .where(col("event_time").isNotNull())
        .where(col("event_id").isNotNull())
        .where(col("headline").isNotNull())
    )

    query = (
        normalized.writeStream
        .foreachBatch(write_batch_to_cassandra)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("Spark Cassandra news writer started.")
    print(f"Kafka topic: {TOPIC}")
    print(f"Kafka bootstrap servers: {BOOTSTRAP_SERVERS}")
    print(f"Cassandra hosts: {CASSANDRA_HOSTS}:{CASSANDRA_PORT}")
    print(f"Cassandra keyspace: {CASSANDRA_KEYSPACE}")
    print(f"Checkpoint: {CHECKPOINT_LOCATION}")

    query.awaitTermination()


if __name__ == "__main__":
    main()
