import { useEffect, useMemo, useState } from "react";
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
  generated_at: string;
};

const API_BASE = "http://localhost:8000";
const WS_BASE = "ws://localhost:8000";

const SYMBOLS = ["AAPL", "MSFT", "NVDA", "AMZN", "BTC-USD"];

function App() {
  const [symbol, setSymbol] = useState("AAPL");
  const [snapshot, setSnapshot] = useState<LiveSnapshot | null>(null);
  const [connectionStatus, setConnectionStatus] = useState("disconnected");
  const [lastError, setLastError] = useState<string | null>(null);

  useEffect(() => {
    let socket: WebSocket | null = null;
    let reconnectTimer: number | null = null;

    async function loadInitialSnapshot() {
      try {
        const response = await fetch(`${API_BASE}/api/v1/dashboard/snapshot/${symbol}`);
        const data = await response.json();
        setSnapshot({
          type: "initial_snapshot",
          symbol: data.symbol,
          market: data.market ?? [],
          news: data.news ?? [],
          generated_at: data.generated_at,
        });
      } catch (error) {
        setLastError(`REST snapshot failed: ${String(error)}`);
      }
    }

    function connectWebSocket() {
      setConnectionStatus("connecting");

      socket = new WebSocket(`${WS_BASE}/ws/live/${symbol}?interval_seconds=2`);

      socket.onopen = () => {
        setConnectionStatus("connected");
        setLastError(null);
      };

      socket.onmessage = (event) => {
        const data = JSON.parse(event.data);
        setSnapshot(data);
      };

      socket.onerror = () => {
        setConnectionStatus("error");
        setLastError("WebSocket error. Check FastAPI server.");
      };

      socket.onclose = () => {
        setConnectionStatus("disconnected");
        reconnectTimer = window.setTimeout(connectWebSocket, 3000);
      };
    }

    loadInitialSnapshot();
    connectWebSocket();

    return () => {
      if (reconnectTimer !== null) {
        window.clearTimeout(reconnectTimer);
      }

      if (socket !== null) {
        socket.close();
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
