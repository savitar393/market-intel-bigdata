import json
import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import databento as db
from dotenv import load_dotenv


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean_value(value):
    if value is None:
        return None

    if hasattr(value, "isoformat"):
        return value.isoformat()

    try:
        if hasattr(value, "item"):
            return value.item()
    except Exception:
        pass

    return value


def row_to_event(row: dict, config: dict) -> dict:
    event_ts = (
        row.get("ts_event")
        or row.get("ts_recv")
        or row.get("index")
        or row.get("timestamp")
    )

    return {
        "event_id": str(uuid4()),
        "payload_version": "1.0",
        "event_type": "market_bar",
        "symbol": str(row.get("symbol", config["symbols"][0])),
        "source": "databento_historical",
        "dataset": config["dataset"],
        "schema": config["schema"],
        "event_ts": clean_value(event_ts),
        "ingest_ts": now_utc(),
        "open": clean_value(row.get("open")),
        "high": clean_value(row.get("high")),
        "low": clean_value(row.get("low")),
        "close": clean_value(row.get("close")),
        "volume": clean_value(row.get("volume")),
    }


def main():
    load_dotenv()

    api_key = os.getenv("DATABENTO_API_KEY")
    if not api_key:
        raise RuntimeError("Missing DATABENTO_API_KEY. Set it in your .env file.")

    config = {
        "dataset": os.getenv("DATABENTO_DATASET", "XNAS.ITCH"),
        "schema": os.getenv("DATABENTO_SCHEMA", "ohlcv-1m"),
        "symbols": [
            s.strip()
            for s in os.getenv("DATABENTO_SYMBOLS", "AAPL,MSFT,NVDA").split(",")
            if s.strip()
        ],
        "start": os.getenv("DATABENTO_START", "2024-05-21T15:00"),
        "end": os.getenv("DATABENTO_END", "2024-05-21T15:05"),
    }

    print("Databento validation config:")
    print(json.dumps(config, indent=2))

    client = db.Historical(api_key)

    print("\nChecking available datasets...")
    datasets = client.metadata.list_datasets()
    print(f"Available datasets count: {len(datasets)}")
    print("First datasets:", datasets[:20])

    if config["dataset"] not in datasets:
        print(f"\nWARNING: Dataset {config['dataset']} is not in your available datasets.")
        print("Pick one from the printed dataset list and update .env.")
        return

    print(f"\nChecking schemas for dataset={config['dataset']}...")
    schemas = client.metadata.list_schemas(dataset=config["dataset"])
    print("Schemas:", schemas)

    if config["schema"] not in schemas:
        print(f"\nWARNING: Schema {config['schema']} is not available for {config['dataset']}.")
        print("Pick one from the printed schema list and update .env.")
        return

    print(f"\nChecking dataset range for {config['dataset']}...")
    dataset_range = client.metadata.get_dataset_range(dataset=config["dataset"])
    print(json.dumps(dataset_range, indent=2, default=str)[:3000])

    print("\nRequesting small historical sample...")
    data = client.timeseries.get_range(
        dataset=config["dataset"],
        schema=config["schema"],
        symbols=config["symbols"],
        start=config["start"],
        end=config["end"],
    )

    df = data.to_df()
    print("\nDataFrame shape:", df.shape)
    print("Columns:", list(df.columns))
    print("\nHead:")
    print(df.head())

    output_dir = Path("data/samples")
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = output_dir / "databento_sample.csv"
    jsonl_path = output_dir / "databento_events.jsonl"

    df.to_csv(csv_path)
    print(f"\nSaved CSV sample to {csv_path}")

    df_reset = df.reset_index()
    events = []

    for _, row in df_reset.head(20).iterrows():
        row_dict = row.to_dict()
        event = row_to_event(row_dict, config)
        events.append(event)

    with jsonl_path.open("w", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event, default=str) + "\n")

    print(f"Saved normalized JSONL sample to {jsonl_path}")

    print("\nFirst normalized event:")
    if events:
        print(json.dumps(events[0], indent=2, default=str))
    else:
        print("No events returned. Try a different time window or dataset.")


if __name__ == "__main__":
    main()
