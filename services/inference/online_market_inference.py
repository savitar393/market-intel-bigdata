import argparse
import json
import math
import os
import statistics
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from cassandra.cluster import Cluster
from confluent_kafka import Consumer
from dotenv import load_dotenv


DEFAULT_MODEL_PATH = "data/model_artifacts/online_serving/logistic_regression_online.json"


def parse_ts(value):
    if value is None:
        return datetime.now(timezone.utc)

    if isinstance(value, (int, float)):
        # yfinance sometimes uses epoch milliseconds.
        if value > 10_000_000_000:
            return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
        return datetime.fromtimestamp(value, tz=timezone.utc)

    text = str(value)

    if text.isdigit():
        number = int(text)
        if number > 10_000_000_000:
            return datetime.fromtimestamp(number / 1000.0, tz=timezone.utc)
        return datetime.fromtimestamp(number, tz=timezone.utc)

    text = text.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def floor_minute(ts):
    return ts.replace(second=0, microsecond=0)


@dataclass
class Bar:
    symbol: str
    minute: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float

    def update(self, price, volume):
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.volume += float(volume or 0.0)


class OnlineLogisticModel:
    def __init__(self, path):
        payload = json.loads(Path(path).read_text(encoding="utf-8"))

        self.model_name = payload["model_name"]
        self.threshold = float(payload.get("threshold", 0.47))
        self.feature_columns = payload["feature_columns"]
        self.coefficients = [float(x) for x in payload["coefficients"]]
        self.intercept = float(payload["intercept"])

        scaler = payload.get("scaler") or {}
        self.mean = scaler.get("mean")
        self.std = scaler.get("std")

    def _scale(self, values):
        if self.mean is None or self.std is None:
            return values

        scaled = []

        for x, mean, std in zip(values, self.mean, self.std):
            mean = float(mean or 0.0)
            std = float(std or 0.0)

            if std == 0:
                scaled.append(0.0)
            else:
                scaled.append((x - mean) / std)

        return scaled

    def predict(self, features):
        values = [float(features.get(col, 0.0) or 0.0) for col in self.feature_columns]
        scaled = self._scale(values)

        margin = self.intercept + sum(
            coef * value for coef, value in zip(self.coefficients, scaled)
        )

        # Numeric stability.
        if margin >= 0:
            z = math.exp(-margin)
            probability_up = 1.0 / (1.0 + z)
        else:
            z = math.exp(margin)
            probability_up = z / (1.0 + z)

        probability_down = 1.0 - probability_up
        predicted_direction = 1 if probability_up >= self.threshold else 0

        return predicted_direction, probability_down, probability_up


class FeatureState:
    def __init__(self, max_bars=120):
        self.bars = defaultdict(lambda: deque(maxlen=max_bars))
        self.news_state = defaultdict(
            lambda: {
                "news_count": 0.0,
                "avg_sentiment_score": 0.0,
                "positive_news_count": 0.0,
                "negative_news_count": 0.0,
            }
        )

    def add_bar(self, bar):
        self.bars[bar.symbol].append(bar)

    def compute_features(self, bar):
        history = list(self.bars[bar.symbol])

        closes = [b.close for b in history] + [bar.close]
        volumes = [b.volume for b in history] + [bar.volume]

        def lag_return(lag):
            if len(closes) <= lag:
                return 0.0
            prev = closes[-lag - 1]
            if prev == 0:
                return 0.0
            return (closes[-1] - prev) / prev

        def rolling_mean(window):
            values = closes[-window:]
            return statistics.mean(values) if values else bar.close

        def rolling_std(values):
            if len(values) < 2:
                return 0.0
            return statistics.pstdev(values)

        def rolling_price_std(window):
            return rolling_std(closes[-window:])

        def rolling_volatility(window):
            returns = []
            for idx in range(max(1, len(closes) - window + 1), len(closes)):
                prev = closes[idx - 1]
                cur = closes[idx]
                if prev != 0:
                    returns.append((cur - prev) / prev)
            return rolling_std(returns)

        def rolling_volume_mean(window):
            values = volumes[-window:]
            return statistics.mean(values) if values else bar.volume

        def volume_surprise(window):
            avg_volume = rolling_volume_mean(window)
            if avg_volume == 0:
                return 0.0
            return bar.volume / avg_volume

        news = self.news_state[bar.symbol]

        features = {
            "market_price": bar.close,
            "volume": bar.volume,
            "return_1": lag_return(1),
            "return_2": lag_return(2),
            "return_3": lag_return(3),
            "return_5": lag_return(5),
            "return_10": lag_return(10),
            "rolling_mean_3": rolling_mean(3),
            "rolling_mean_5": rolling_mean(5),
            "rolling_mean_10": rolling_mean(10),
            "rolling_mean_30": rolling_mean(30),
            "rolling_price_std_5": rolling_price_std(5),
            "rolling_price_std_10": rolling_price_std(10),
            "rolling_price_std_30": rolling_price_std(30),
            "rolling_volatility_3": rolling_volatility(3),
            "rolling_volatility_5": rolling_volatility(5),
            "rolling_volatility_10": rolling_volatility(10),
            "rolling_volatility_30": rolling_volatility(30),
            "rolling_volume_mean_5": rolling_volume_mean(5),
            "rolling_volume_mean_10": rolling_volume_mean(10),
            "volume_surprise_5": volume_surprise(5),
            "volume_surprise_10": volume_surprise(10),
            "log_volume": math.log1p(max(bar.volume, 0.0)),
            "bar_range": bar.high - bar.low,
            "candle_body": abs(bar.close - bar.open),
            "upper_shadow": bar.high - max(bar.open, bar.close),
            "lower_shadow": min(bar.open, bar.close) - bar.low,
            "minute_of_day": float(bar.minute.hour * 60 + bar.minute.minute),
            "news_count": news["news_count"],
            "avg_sentiment_score": news["avg_sentiment_score"],
            "positive_news_count": news["positive_news_count"],
            "negative_news_count": news["negative_news_count"],
        }

        return features


class CassandraWriter:
    def __init__(self, hosts, port, keyspace):
        cluster = Cluster(hosts, port=port)
        self.session = cluster.connect(keyspace)

        self.insert_prediction = self.session.prepare(
            """
            INSERT INTO model_predictions_by_symbol (
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
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

    def write_prediction(
        self,
        symbol,
        event_time,
        model_name,
        market_price,
        predicted_direction,
        probability_down,
        probability_up,
    ):
        self.session.execute(
            self.insert_prediction,
            (
                symbol,
                event_time,
                datetime.now(timezone.utc),
                str(uuid4()),
                model_name,
                float(market_price),
                int(predicted_direction),
                float(probability_down),
                float(probability_up),
                None,
                "online_kafka_tick_inference",
            ),
        )


def build_consumer(bootstrap_servers, topic, group_id):
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([topic])
    return consumer


def event_price(event):
    price = event.get("price")

    if price is None:
        price = event.get("close")

    return float(price) if price is not None else None


def event_volume(event):
    volume = event.get("volume")

    if volume is None:
        return 0.0

    try:
        return float(volume)
    except Exception:
        return 0.0


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Online market inference from Kafka live ticks."
    )
    parser.add_argument(
        "--model-path",
        default=os.getenv("ONLINE_MODEL_PATH", DEFAULT_MODEL_PATH),
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument(
        "--topic",
        default=os.getenv("KAFKA_MARKET_TOPIC", "raw_market_ticks"),
    )
    parser.add_argument(
        "--group-id",
        default=os.getenv("ONLINE_INFERENCE_GROUP_ID", "online-market-inference"),
    )
    parser.add_argument(
        "--symbols",
        default=os.getenv("ONLINE_INFERENCE_SYMBOLS", "AAPL,MSFT,NVDA,AMZN,TSLA"),
    )
    parser.add_argument(
        "--max-predictions",
        type=int,
        default=int(os.getenv("ONLINE_INFERENCE_MAX_PREDICTIONS", "0")),
        help="0 means run forever.",
    )

    args = parser.parse_args()

    symbols = {s.strip().upper() for s in args.symbols.split(",") if s.strip()}

    model = OnlineLogisticModel(args.model_path)
    state = FeatureState()

    cassandra_hosts = [
        h.strip()
        for h in os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
        if h.strip()
    ]
    cassandra_port = int(os.getenv("CASSANDRA_PORT", "9042"))
    cassandra_keyspace = os.getenv("CASSANDRA_KEYSPACE", "market_intel")

    writer = CassandraWriter(
        hosts=cassandra_hosts,
        port=cassandra_port,
        keyspace=cassandra_keyspace,
    )

    consumer = build_consumer(args.bootstrap_servers, args.topic, args.group_id)

    current_bars = {}
    prediction_count = 0

    print("Online market inference started")
    print(f"Kafka topic: {args.topic}")
    print(f"Symbols: {sorted(symbols)}")
    print(f"Model: {model.model_name}")
    print(f"Threshold: {model.threshold}")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue

            try:
                event = json.loads(msg.value().decode("utf-8"))
            except Exception as exc:
                print(f"Invalid JSON event: {exc}")
                continue

            event_type = event.get("event_type")
            symbol = str(event.get("symbol", "")).upper()

            if symbol not in symbols:
                continue

            if event_type not in {"market_tick", "market_bar"}:
                continue

            price = event_price(event)

            if price is None:
                continue

            volume = event_volume(event)
            ts = floor_minute(parse_ts(event.get("event_ts")))

            active = current_bars.get(symbol)

            if active is None:
                current_bars[symbol] = Bar(
                    symbol=symbol,
                    minute=ts,
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    volume=volume,
                )
                continue

            if ts == active.minute:
                active.update(price, volume)
                continue

            # Minute changed: finalize previous bar and score it.
            features = state.compute_features(active)
            predicted_direction, probability_down, probability_up = model.predict(features)

            writer.write_prediction(
                symbol=active.symbol,
                event_time=active.minute,
                model_name=model.model_name,
                market_price=active.close,
                predicted_direction=predicted_direction,
                probability_down=probability_down,
                probability_up=probability_up,
            )

            state.add_bar(active)
            prediction_count += 1

            direction_label = "UP" if predicted_direction == 1 else "DOWN"

            print(
                f"[{prediction_count}] {active.minute.isoformat()} "
                f"{active.symbol} close={active.close} "
                f"prediction={direction_label} "
                f"prob_up={probability_up:.4f} "
                f"source=online_kafka_tick_inference"
            )

            current_bars[symbol] = Bar(
                symbol=symbol,
                minute=ts,
                open=price,
                high=price,
                low=price,
                close=price,
                volume=volume,
            )

            if args.max_predictions and prediction_count >= args.max_predictions:
                print("Reached max predictions.")
                break

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
