import argparse
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf
from dotenv import load_dotenv


DEFAULT_OUTPUT_PATH = "data/processed/daily_stock_prices/daily_stock_prices.csv"


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.reset_index()

    rename_map = {
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
        "Dividends": "dividends",
        "Stock Splits": "stock_splits",
    }

    df = df.rename(columns=rename_map)

    for col in [
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
        "dividends",
        "stock_splits",
    ]:
        if col not in df.columns:
            df[col] = 0.0

    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)

    return df[
        [
            "date",
            "open",
            "high",
            "low",
            "close",
            "adj_close",
            "volume",
            "dividends",
            "stock_splits",
        ]
    ]


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Backfill real daily stock data from Yahoo Finance."
    )
    parser.add_argument(
        "--symbols",
        default=os.getenv("DAILY_BACKFILL_SYMBOLS", "AAPL,MSFT,NVDA,AMZN,TSLA"),
    )
    parser.add_argument(
        "--start",
        default=os.getenv("DAILY_BACKFILL_START", "2010-01-01"),
    )
    parser.add_argument(
        "--end",
        default=os.getenv("DAILY_BACKFILL_END", ""),
        help="YYYY-MM-DD. Empty means today/current available data.",
    )
    parser.add_argument(
        "--output-path",
        default=os.getenv("DAILY_BACKFILL_OUTPUT_PATH", DEFAULT_OUTPUT_PATH),
    )

    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    output_path = Path(args.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    all_frames = []
    ingest_ts = datetime.now(timezone.utc).isoformat()

    print("Daily Yahoo Finance backfill config:")
    print(f"  symbols     = {symbols}")
    print(f"  start       = {args.start}")
    print(f"  end         = {args.end or '(latest)'}")
    print(f"  output_path = {output_path}")

    for symbol in symbols:
        print(f"\n=== Downloading {symbol} daily history ===")

        ticker = yf.Ticker(symbol)

        history_kwargs = {
            "start": args.start,
            "auto_adjust": False,
            "actions": True,
        }

        if args.end:
            history_kwargs["end"] = args.end

        df = ticker.history(**history_kwargs)

        if df.empty:
            print(f"No daily history returned for {symbol}")
            continue

        df = normalize_columns(df)
        df["symbol"] = symbol
        df["source"] = "yfinance_daily_backfill"
        df["ingest_ts"] = ingest_ts

        print(f"{symbol}: {len(df)} rows, {df['date'].min()} → {df['date'].max()}")

        all_frames.append(df)

    if not all_frames:
        raise RuntimeError("No daily data downloaded.")

    result = pd.concat(all_frames, ignore_index=True)
    result = result[
        [
            "symbol",
            "date",
            "open",
            "high",
            "low",
            "close",
            "adj_close",
            "volume",
            "dividends",
            "stock_splits",
            "source",
            "ingest_ts",
        ]
    ]

    result.to_csv(output_path, index=False)

    print("")
    print(f"Wrote {len(result)} daily rows to {output_path}")
    print("Daily backfill completed.")


if __name__ == "__main__":
    main()
