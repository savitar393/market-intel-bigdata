import argparse
import os
from datetime import datetime, timezone

import requests
from cassandra.cluster import Cluster
from dotenv import load_dotenv


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Send prediction alerts from Cassandra to Telegram."
    )
    parser.add_argument(
        "--symbols",
        default="AAPL,MSFT,NVDA",
        help="Comma-separated symbols.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Alerts per symbol to inspect.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print notifications without sending to Telegram.",
    )

    args = parser.parse_args()

    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not args.dry_run and (not bot_token or not chat_id):
        raise RuntimeError(
            "Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID. Set them in .env."
        )

    cassandra_hosts = [
        h.strip()
        for h in os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
        if h.strip()
    ]
    cassandra_port = int(os.getenv("CASSANDRA_PORT", "9042"))
    keyspace = os.getenv("CASSANDRA_KEYSPACE", "market_intel")

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    safe_limit = max(1, min(args.limit, 100))

    cluster = Cluster(cassandra_hosts, port=cassandra_port)
    session = cluster.connect(keyspace)

    select_alerts_query = f"""
        SELECT
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
        FROM alerts_by_symbol
        WHERE symbol = %s
        LIMIT {safe_limit}
    """

    check_sent_query = """
        SELECT alert_id
        FROM telegram_notifications_by_alert
        WHERE alert_id = %s
        LIMIT 1
    """

    insert_log_query = """
        INSERT INTO telegram_notifications_by_alert (
            alert_id,
            symbol,
            alert_time,
            sent_at,
            chat_id,
            status,
            response_text
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    sent_count = 0
    skipped_count = 0

    for symbol in symbols:
        rows = session.execute(select_alerts_query, (symbol,))

        for row in rows:
            already_sent = session.execute(
                check_sent_query,
                (row.alert_id,),
            ).one()

            if already_sent:
                skipped_count += 1
                continue

            text = (
                "🚨 Market Intel Alert\n\n"
                f"Symbol: {row.symbol}\n"
                f"Severity: {str(row.severity).upper()}\n"
                f"Message: {row.message}\n\n"
                f"Model: {row.model_name}\n"
                f"Confidence: {float(row.confidence) * 100:.1f}%\n"
                f"Event time: {row.event_time}\n"
                f"Alert time: {row.alert_time}\n\n"
                "Note: academic demo alert, not financial advice."
            )

            print("\n--- Notification ---")
            print(text)

            status = "dry_run"
            response_text = "not sent"

            if not args.dry_run:
                url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
                response = requests.post(
                    url,
                    json={
                        "chat_id": chat_id,
                        "text": text,
                    },
                    timeout=15,
                )

                status = "sent" if response.ok else "failed"
                response_text = response.text[:1000]

                if not response.ok:
                    print(f"Telegram send failed: {response.status_code} {response.text}")

            session.execute(
                insert_log_query,
                (
                    row.alert_id,
                    row.symbol,
                    row.alert_time,
                    datetime.now(timezone.utc),
                    str(chat_id) if chat_id else "dry_run",
                    status,
                    response_text,
                ),
            )

            sent_count += 1

    cluster.shutdown()

    print(
        f"\nDone. sent_or_logged={sent_count}, skipped_already_sent={skipped_count}, dry_run={args.dry_run}"
    )


if __name__ == "__main__":
    main()
