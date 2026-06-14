import os
from datetime import datetime, timezone
from uuid import uuid4

from cassandra.cluster import Cluster
from dotenv import load_dotenv


def main():
    load_dotenv()

    hosts = [
        h.strip()
        for h in os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
        if h.strip()
    ]
    port = int(os.getenv("CASSANDRA_PORT", "9042"))
    keyspace = os.getenv("CASSANDRA_KEYSPACE", "market_intel")

    print(f"Connecting to Cassandra hosts={hosts} port={port} keyspace={keyspace}")

    cluster = Cluster(hosts, port=port)
    session = cluster.connect(keyspace)

    event_id = str(uuid4())
    now = datetime.now(timezone.utc)

    session.execute(
        """
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
        """,
        (
            "TEST",
            now,
            event_id,
            "market_tick",
            "cassandra_smoke_test",
            "local",
            "test_schema",
            123.45,
            123.45,
            None,
            None,
            None,
            None,
            100,
            now,
            now,
            0.0,
            '{"test": true}',
        ),
    )

    rows = session.execute(
        """
        SELECT symbol, event_time, event_id, market_price, source
        FROM market_ticks_by_symbol
        WHERE symbol = %s
        LIMIT 5
        """,
        ("TEST",),
    )

    print("Rows:")
    found = False
    for row in rows:
        found = True
        print(row)

    if not found:
        raise RuntimeError("Insert succeeded but no rows were returned.")

    cluster.shutdown()
    print("Cassandra smoke test passed.")


if __name__ == "__main__":
    main()
