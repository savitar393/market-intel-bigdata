import argparse
import os
from datetime import datetime, timezone
from uuid import uuid4

from cassandra.cluster import Cluster
from dotenv import load_dotenv


def direction_label(value: int | None) -> str:
    if value == 1:
        return "UP"
    if value == 0:
        return "DOWN"
    return "UNKNOWN"


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Generate simple prediction alerts from Cassandra model predictions."
    )
    parser.add_argument(
        "--symbols",
        default="AAPL,MSFT,NVDA",
        help="Comma-separated symbols.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Prediction rows per symbol to inspect.",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.55,
        help="Minimum confidence to create an alert.",
    )

    args = parser.parse_args()

    hosts = [
        h.strip()
        for h in os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
        if h.strip()
    ]
    port = int(os.getenv("CASSANDRA_PORT", "9042"))
    keyspace = os.getenv("CASSANDRA_KEYSPACE", "market_intel")

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    cluster = Cluster(hosts, port=port)
    session = cluster.connect(keyspace)

    select_query = f"""
        SELECT
            symbol,
            event_time,
            prediction_time,
            event_id,
            model_name,
            market_price,
            predicted_direction,
            probability_down,
            probability_up,
            target_direction,
            source
        FROM model_predictions_by_symbol
        WHERE symbol = %s
        LIMIT {max(1, min(args.limit, 100))}
    """

    insert_query = """
        INSERT INTO alerts_by_symbol (
            symbol,
            alert_time,
            alert_id,
            event_time,
            prediction_time,
            model_name,
            alert_type,
            severity,
            predicted_direction,
            probability_down,
            probability_up,
            confidence,
            message,
            source
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    total_alerts = 0

    for symbol in symbols:
        rows = session.execute(select_query, (symbol,))

        for row in rows:
            probability_up = float(row.probability_up or 0.0)
            probability_down = float(row.probability_down or 0.0)

            predicted_direction = int(row.predicted_direction)
            confidence = probability_up if predicted_direction == 1 else probability_down

            if confidence < args.threshold:
                continue

            label = direction_label(predicted_direction)
            severity = "high" if confidence >= 0.60 else "medium"

            message = (
                f"{symbol} predicted {label} with {confidence * 100:.1f}% confidence "
                f"using {row.model_name} at price {row.market_price}."
            )

            session.execute(
                insert_query,
                (
                    symbol,
                    datetime.now(timezone.utc),
                    str(uuid4()),
                    row.event_time,
                    row.prediction_time,
                    row.model_name,
                    "prediction_threshold",
                    severity,
                    predicted_direction,
                    probability_down,
                    probability_up,
                    confidence,
                    message,
                    "prediction_alert_generator",
                ),
            )

            total_alerts += 1
            print(f"ALERT {severity.upper()} {message}")

    cluster.shutdown()

    print(f"Generated {total_alerts} alerts.")


if __name__ == "__main__":
    main()
