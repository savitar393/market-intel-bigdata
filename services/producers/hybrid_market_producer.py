import argparse
import os
import signal
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def run_command(command: list[str], env: dict, name: str) -> int:
    print("")
    print("=" * 80)
    print(f"Starting: {name}")
    print("Command:", " ".join(command))
    print("=" * 80)

    process = subprocess.Popen(
        command,
        cwd=PROJECT_ROOT,
        env=env,
    )

    try:
        return process.wait()
    except KeyboardInterrupt:
        print(f"Keyboard interrupt received. Stopping {name}...")
        process.send_signal(signal.SIGINT)

        try:
            return process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            return process.wait()


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(
        description=(
            "Run Databento historical replay first, then automatically switch "
            "to live WebSocket market data."
        )
    )

    parser.add_argument(
        "--symbols",
        default=os.getenv("HYBRID_SYMBOLS", os.getenv("DATABENTO_SYMBOLS", "AAPL,MSFT,NVDA,AMZN,TSLA")),
        help="Comma-separated stock symbols for Databento/yfinance.",
    )
    parser.add_argument(
        "--live-symbols",
        default=os.getenv("LIVE_SYMBOLS", "AAPL,MSFT,NVDA,AMZN,TSLA,BTC-USD"),
        help="Comma-separated live symbols for yfinance.",
    )
    parser.add_argument(
        "--finnhub-symbols",
        default=os.getenv("FINNHUB_WS_SYMBOLS", "AAPL,MSFT,NVDA,AMZN,TSLA,BINANCE:BTCUSDT"),
        help="Comma-separated Finnhub WebSocket symbols.",
    )
    parser.add_argument(
        "--historical-speed",
        type=float,
        default=float(os.getenv("HYBRID_HISTORICAL_SPEED", "7200")),
        help="Replay speed for Databento historical data.",
    )
    parser.add_argument(
        "--live-provider",
        choices=["yfinance", "finnhub", "both"],
        default=os.getenv("HYBRID_LIVE_PROVIDER", "both"),
        help="Live provider after historical replay. 'both' means yfinance first, Finnhub fallback.",
    )
    parser.add_argument(
        "--live-max-messages",
        type=int,
        default=int(os.getenv("HYBRID_LIVE_MAX_MESSAGES", "0")),
        help="Max live messages. 0 means run forever.",
    )
    parser.add_argument(
        "--skip-historical",
        action="store_true",
        help="Skip Databento replay and start live WebSocket immediately.",
    )
    parser.add_argument(
        "--skip-live",
        action="store_true",
        help="Only run Databento replay, then stop.",
    )

    args = parser.parse_args()

    env = os.environ.copy()

    # Keep all producers using the same symbol/topic configuration.
    env["DATABENTO_SYMBOLS"] = args.symbols
    env["LIVE_SYMBOLS"] = args.live_symbols
    env["FINNHUB_WS_SYMBOLS"] = args.finnhub_symbols

    print("Hybrid market producer config:")
    print(f"  DATABENTO_SYMBOLS      = {env.get('DATABENTO_SYMBOLS')}")
    print(f"  DATABENTO_START        = {env.get('DATABENTO_START')}")
    print(f"  DATABENTO_END          = {env.get('DATABENTO_END')}")
    print(f"  LIVE_SYMBOLS           = {env.get('LIVE_SYMBOLS')}")
    print(f"  FINNHUB_WS_SYMBOLS     = {env.get('FINNHUB_WS_SYMBOLS')}")
    print(f"  KAFKA_BOOTSTRAP_SERVERS= {env.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')}")
    print(f"  KAFKA_MARKET_TOPIC     = {env.get('KAFKA_MARKET_TOPIC', 'raw_market_ticks')}")
    print(f"  historical_speed       = {args.historical_speed}")
    print(f"  live_provider          = {args.live_provider}")
    print(f"  live_max_messages      = {args.live_max_messages}")

    if not args.skip_historical:
        historical_cmd = [
            sys.executable,
            "services/producers/databento_replay_producer.py",
            "--speed",
            str(args.historical_speed),
        ]

        historical_code = run_command(
            historical_cmd,
            env=env,
            name="Databento historical replay",
        )

        if historical_code != 0:
            raise SystemExit(
                f"Databento historical replay failed with exit code {historical_code}."
            )

        print("")
        print("Historical replay completed. Switching to live market stream...")

    if args.skip_live:
        print("skip-live enabled. Hybrid producer finished after historical replay.")
        return

    live_commands = []

    if args.live_provider in {"yfinance", "both"}:
        live_commands.append(
            (
                "yfinance live WebSocket producer",
                [
                    sys.executable,
                    "services/producers/yfinance_live_producer.py",
                    "--symbols",
                    args.live_symbols,
                    "--max-messages",
                    str(args.live_max_messages),
                ],
            )
        )

    if args.live_provider in {"finnhub", "both"}:
        live_commands.append(
            (
                "Finnhub WebSocket fallback producer",
                [
                    sys.executable,
                    "services/producers/finnhub_ws_producer.py",
                    "--symbols",
                    args.finnhub_symbols,
                    "--max-messages",
                    str(args.live_max_messages),
                ],
            )
        )

    for idx, (name, command) in enumerate(live_commands):
        code = run_command(command, env=env, name=name)

        if code == 0:
            print(f"{name} exited successfully.")
            return

        print(f"{name} failed with exit code {code}.")

        if idx < len(live_commands) - 1:
            print("Trying next live provider...")
        else:
            raise SystemExit("All live providers failed.")

    print("Hybrid market producer finished.")


if __name__ == "__main__":
    main()
