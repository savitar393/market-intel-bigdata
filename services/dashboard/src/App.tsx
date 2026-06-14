import { useEffect, useMemo, useRef, useState } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  ResponsiveContainer,
} from "recharts";
import "./App.css";

type MarketItem = {
  symbol: string;
  event_time: string;
  event_type: string;
  source: string;
  market_price: number;
  volume?: number | null;
  ingest_latency_seconds?: number | null;
};

type NewsItem = {
  symbol: string;
  event_time: string;
  headline: string;
  simple_sentiment_label: string;
  simple_sentiment_score: number;
  source: string;
};

type LiveSnapshot = {
  type: string;
  symbol: string;
  market: MarketItem[];
  news: NewsItem[];
  predictions: PredictionItem[];
  generated_at: string;
};

type PredictionItem = {
  symbol: string;
  event_time: string;
  prediction_time: string;
  model_name: string;
  market_price: number;
  predicted_direction: number;
  probability_down: number;
  probability_up: number;
  target_direction?: number | null;
  source: string;
};

const API_BASE = import.meta.env.VITE_API_BASE ?? "http://localhost:8000";
const WS_BASE = import.meta.env.VITE_WS_BASE ?? "ws://localhost:8000";

const SYMBOLS = ["AAPL", "MSFT", "NVDA", "AMZN", "BTC-USD"];

function App() {
  const [symbol, setSymbol] = useState("AAPL");
  const [snapshot, setSnapshot] = useState<LiveSnapshot | null>(null);
  const [connectionStatus, setConnectionStatus] = useState("disconnected");
  const [lastError, setLastError] = useState<string | null>(null);
  const requestSeqRef = useRef(0);

  useEffect(() => {
    const requestSeq = requestSeqRef.current + 1;
    requestSeqRef.current = requestSeq;

    let socket: WebSocket | null = null;
    let reconnectTimer: number | null = null;
    let shouldReconnect = true;

    const selectedSymbol = symbol;
    const encodedSymbol = encodeURIComponent(selectedSymbol);

    setConnectionStatus("connecting");
    setLastError(null);
    setSnapshot(null);

    function isCurrentRequest() {
      return requestSeqRef.current === requestSeq;
    }

    async function loadInitialSnapshot() {
      try {
        const response = await fetch(
          `${API_BASE}/api/v1/dashboard/snapshot/${encodedSymbol}`
        );

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }

        const data = await response.json();

        if (!isCurrentRequest()) {
          return;
        }

        setSnapshot({
          type: "initial_snapshot",
          symbol: data.symbol,
          market: data.market ?? [],
          news: data.news ?? [],
          predictions: data.predictions ?? [],
          generated_at: data.generated_at,
        });
      } catch (error) {
        if (!isCurrentRequest()) {
          return;
        }

        setLastError(`REST snapshot failed: ${String(error)}`);
      }
    }

    function connectWebSocket() {
      if (!shouldReconnect || !isCurrentRequest()) {
        return;
      }

      setConnectionStatus("connecting");

      socket = new WebSocket(
        `${WS_BASE}/ws/live/${encodedSymbol}?interval_seconds=2`
      );

      socket.onopen = () => {
        if (!isCurrentRequest()) {
          return;
        }

        setConnectionStatus("connected");
        setLastError(null);
      };

      socket.onmessage = (event) => {
        if (!isCurrentRequest()) {
          return;
        }

        try {
          const data = JSON.parse(event.data);

          if (data.symbol !== selectedSymbol) {
            return;
          }

          setSnapshot(data);
        } catch (error) {
          setLastError(`WebSocket message parse failed: ${String(error)}`);
        }
      };

      socket.onerror = () => {
        if (!isCurrentRequest()) {
          return;
        }

        setConnectionStatus("error");
        setLastError("WebSocket error. Check FastAPI server.");
      };

      socket.onclose = () => {
        if (!shouldReconnect || !isCurrentRequest()) {
          return;
        }

        setConnectionStatus("disconnected");
        reconnectTimer = window.setTimeout(connectWebSocket, 3000);
      };
    }

    loadInitialSnapshot();
    connectWebSocket();

    return () => {
      shouldReconnect = false;

      if (reconnectTimer !== null) {
        window.clearTimeout(reconnectTimer);
      }

      if (socket !== null) {
        socket.onopen = null;
        socket.onmessage = null;
        socket.onerror = null;
        socket.onclose = null;

        if (
          socket.readyState === WebSocket.CONNECTING ||
          socket.readyState === WebSocket.OPEN
        ) {
          socket.close(1000, "symbol changed");
        }
      }
    };
  }, [symbol]);

  const marketChartData = useMemo(() => {
    return [...(snapshot?.market ?? [])]
      .reverse()
      .map((item) => ({
        time: new Date(item.event_time).toLocaleTimeString(),
        price: item.market_price,
        source: item.source,
      }));
  }, [snapshot]);

  const latestMarket = snapshot?.market?.[0];
  const newsItems = snapshot?.news ?? [];

  const predictionItems = snapshot?.predictions ?? [];
  const latestPrediction = predictionItems[0];

  const predictionLabel =
    latestPrediction?.predicted_direction === 1
      ? "UP"
      : latestPrediction?.predicted_direction === 0
        ? "DOWN"
        : "-";

  const predictionConfidence =
    latestPrediction?.predicted_direction === 1
      ? latestPrediction?.probability_up
      : latestPrediction?.predicted_direction === 0
        ? latestPrediction?.probability_down
        : null;

  return (
    <main className="page">
      <section className="hero">
        <div>
          <p className="eyebrow">Market Intel Big Data</p>
          <h1>Real-Time Multimodal Stock Intelligence</h1>
          <p className="subtitle">
            Live dashboard powered by Kafka, Spark, Cassandra, FastAPI, and WebSocket.
          </p>
        </div>

        <div className={`status status-${connectionStatus}`}>
          WebSocket: {connectionStatus}
        </div>
      </section>

      <section className="controls">
        <label htmlFor="symbol">Symbol</label>
        <select
          id="symbol"
          value={symbol}
          onChange={(event) => setSymbol(event.target.value)}
        >
          {SYMBOLS.map((item) => (
            <option key={item} value={item}>
              {item}
            </option>
          ))}
        </select>

        <span className="generated">
          Last update: {snapshot?.generated_at ?? "-"}
        </span>
      </section>

      {lastError && <div className="error">{lastError}</div>}

      <section className="cards">
        <article className="card">
          <h2>Latest Price</h2>
          <p className="metric">
            {latestMarket?.market_price?.toLocaleString() ?? "-"}
          </p>
          <p className="muted">
            {latestMarket?.event_type ?? "-"} · {latestMarket?.source ?? "-"}
          </p>
        </article>

        <article className="card">
          <h2>Latest Event Time</h2>
          <p className="metric small">
            {latestMarket?.event_time
              ? new Date(latestMarket.event_time).toLocaleString()
              : "-"}
          </p>
          <p className="muted">Newest record from Cassandra</p>
        </article>

        <article className="card">
          <h2>Ingest Latency</h2>
          <p className="metric">
            {latestMarket?.ingest_latency_seconds ?? "-"}s
          </p>
          <p className="muted">Spark process time minus ingest time</p>
        </article>

        <article className="card">
          <h2>Model Prediction</h2>
          <p className={`metric prediction-${predictionLabel.toLowerCase()}`}>
            {predictionLabel}
          </p>
          <p className="muted">
            confidence{" "}
            {predictionConfidence !== null && predictionConfidence !== undefined
              ? `${(predictionConfidence * 100).toFixed(1)}%`
              : "-"}
          </p>
        </article>
      </section>

      <section className="panel">
        <h2>Market Price Stream</h2>
        <div className="chart">
          <ResponsiveContainer width="100%" height={320}>
            <LineChart data={marketChartData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="time" minTickGap={24} />
              <YAxis domain={["auto", "auto"]} />
              <Tooltip />
              <Line
                type="monotone"
                dataKey="price"
                strokeWidth={2}
                dot={false}
                isAnimationActive={false}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </section>

      <section className="panel">
        <h2>Latest Model Predictions</h2>

        {predictionItems.length === 0 && (
          <p className="muted">No prediction records found for {symbol}.</p>
        )}

        {predictionItems.length > 0 && (
          <div className="prediction-table">
            <div className="prediction-row prediction-header">
              <span>Time</span>
              <span>Model</span>
              <span>Prediction</span>
              <span>Prob. Up</span>
              <span>Prob. Down</span>
              <span>Actual</span>
            </div>

            {predictionItems.slice(0, 10).map((item) => (
              <div key={item.event_time + item.model_name} className="prediction-row">
                <span>{new Date(item.event_time).toLocaleString()}</span>
                <span>{item.model_name}</span>
                <span
                  className={
                    item.predicted_direction === 1
                      ? "prediction-up"
                      : "prediction-down"
                  }
                >
                  {item.predicted_direction === 1 ? "UP" : "DOWN"}
                </span>
                <span>{(item.probability_up * 100).toFixed(1)}%</span>
                <span>{(item.probability_down * 100).toFixed(1)}%</span>
                <span>
                  {item.target_direction === 1
                    ? "UP"
                    : item.target_direction === 0
                      ? "DOWN"
                      : "-"}
                </span>
              </div>
            ))}
          </div>
        )}
      </section>

      <section className="panel">
        <h2>Latest Company News</h2>
        <div className="news-list">
          {newsItems.length === 0 && (
            <p className="muted">No news records found for {symbol}.</p>
          )}

          {newsItems.map((item) => (
            <article key={item.event_time + item.headline} className="news-item">
              <div className={`sentiment sentiment-${item.simple_sentiment_label}`}>
                {item.simple_sentiment_label}
              </div>
              <div>
                <h3>{item.headline}</h3>
                <p className="muted">
                  {new Date(item.event_time).toLocaleString()} · score{" "}
                  {item.simple_sentiment_score}
                </p>
              </div>
            </article>
          ))}
        </div>
      </section>
    </main>
  );
}

export default App;
