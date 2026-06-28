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
  alerts: AlertItem[];
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

type AlertItem = {
  symbol: string;
  alert_time: string;
  event_time: string;
  prediction_time: string;
  model_name: string;
  alert_type: string;
  severity: string;
  predicted_direction: number;
  probability_down: number;
  probability_up: number;
  confidence: number;
  message: string;
  source: string;
};

type SystemSummary = {
  symbol: string;
  generated_at: string;
  counts: {
    market_rows: number;
    news_rows: number;
    prediction_rows: number;
    alert_rows: number;
  };
  market: {
    latest_source?: string | null;
    latest_market_price?: number | null;
    avg_ingest_latency_seconds?: number | null;
    event_time_lag_seconds?: number | null;
    ingest_time_freshness_seconds?: number | null;
    spark_process_freshness_seconds?: number | null;
  };
  news: {
    avg_ingest_latency_seconds?: number | null;
    event_time_lag_seconds?: number | null;
    ingest_time_freshness_seconds?: number | null;
  };
  prediction: {
    model_name?: string | null;
    predicted_direction?: number | null;
    confidence?: number | null;
    prediction_freshness_seconds?: number | null;
  };
  alert: {
    latest_severity?: string | null;
    latest_confidence?: number | null;
    alert_freshness_seconds?: number | null;
  };
  note?: string;
};

const API_BASE = import.meta.env.VITE_API_BASE ?? "http://localhost:8000";
const WS_BASE = import.meta.env.VITE_WS_BASE ?? "ws://localhost:8000";

const SYMBOLS = ["AAPL", "MSFT", "NVDA", "AMZN", "TSLA", "BTC-USD"];

const FINAL_MODEL_NOTE =
  "Academic demo only. Final classifier: Logistic Regression, 1-minute horizon, tuned threshold probability_up ≥ 0.47.";

const MARKET_ONLY_SYMBOLS = new Set(["BTC-USD"]);

function App() {
  const [symbol, setSymbol] = useState("AAPL");
  const [snapshot, setSnapshot] = useState<LiveSnapshot | null>(null);
  const [connectionStatus, setConnectionStatus] = useState("disconnected");
  const [lastError, setLastError] = useState<string | null>(null);
  const [systemSummary, setSystemSummary] = useState<SystemSummary | null>(null);
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
          alerts: data.alerts ?? [],
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

  function formatSeconds(value?: number | null) {
      if (value === null || value === undefined || Number.isNaN(value)) {
        return "-";
      }

      if (value < 60) {
        return `${value.toFixed(1)}s`;
      }

      if (value < 3600) {
        return `${(value / 60).toFixed(1)}m`;
      }

      if (value < 86400) {
        return `${(value / 3600).toFixed(1)}h`;
      }

      return `${(value / 86400).toFixed(1)}d`;
  }

  function formatPercent(value?: number | null) {
    if (value === null || value === undefined || Number.isNaN(value)) {
      return "-";
    }

    return `${(value * 100).toFixed(1)}%`;
  }

  useEffect(() => {
    let cancelled = false;
    let timer: number | null = null;

    const encodedSymbol = encodeURIComponent(symbol);

    async function loadSystemSummary() {
      try {
        const response = await fetch(
          `${API_BASE}/api/v1/system/summary/${encodedSymbol}?limit=20`
        );

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }

        const data = await response.json();

        if (!cancelled) {
          setSystemSummary(data);
        }
      } catch (error) {
        if (!cancelled) {
          console.error("System summary failed:", error);
        }
      }

      if (!cancelled) {
        timer = window.setTimeout(loadSystemSummary, 5000);
      }
    }

    setSystemSummary(null);
    loadSystemSummary();

    return () => {
      cancelled = true;

      if (timer !== null) {
        window.clearTimeout(timer);
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

  const alertItems = snapshot?.alerts ?? [];
  const latestAlert = alertItems[0];

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

        <div className="demo-note">
          <strong>Model note:</strong> {FINAL_MODEL_NOTE}
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

            {MARKET_ONLY_SYMBOLS.has(symbol) && (
              <div className="demo-note warning-note">
                BTC-USD is used for live WebSocket demonstration only. The trained stock prediction model is served for AAPL, MSFT, NVDA, AMZN, and TSLA.
              </div>
            )}
          </p>
        </article>

        <article className="card">
          <h2>Latest Alert</h2>
          <p className={`metric alert-${latestAlert?.severity ?? "none"}`}>
            {latestAlert?.severity?.toUpperCase() ?? "-"}
          </p>
          <p className="muted">
            {latestAlert?.confidence !== undefined
              ? `${(latestAlert.confidence * 100).toFixed(1)}% confidence`
              : "No active alert"}
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
        <h2>Prediction Alerts</h2>

        {alertItems.length === 0 && (
          <p className="muted">No alert records found for {symbol}.</p>
        )}

        {alertItems.length > 0 && (
          <div className="alert-list">
            {alertItems.slice(0, 10).map((item) => (
              <article key={item.alert_time + item.message} className="alert-item">
                <div className={`alert-badge alert-${item.severity}`}>
                  {item.severity}
                </div>

                <div>
                  <h3>{item.message}</h3>
                  <p className="muted">
                    {new Date(item.alert_time).toLocaleString()} ·{" "}
                    {item.alert_type} · {item.model_name}
                  </p>
                </div>
              </article>
            ))}
          </div>
        )}
      </section>

      <section className="panel">
        <h2>System Performance Summary</h2>

        {!systemSummary && (
          <p className="muted">Loading system metrics for {symbol}...</p>
        )}

        {systemSummary && (
          <>
            <div className="system-grid">
              <article className="system-metric">
                <span className="system-label">Market Latency</span>
                <strong>
                  {formatSeconds(systemSummary.market.avg_ingest_latency_seconds)}
                </strong>
                <span className="muted">avg ingest latency</span>
              </article>

              <article className="system-metric">
                <span className="system-label">Market Freshness</span>
                <strong>
                  {formatSeconds(systemSummary.market.ingest_time_freshness_seconds)}
                </strong>
                <span className="muted">since latest ingest</span>
              </article>

              <article className="system-metric">
                <span className="system-label">Prediction Freshness</span>
                <strong>
                  {formatSeconds(systemSummary.prediction.prediction_freshness_seconds)}
                </strong>
                <span className="muted">
                  {systemSummary.prediction.model_name ?? "no model"}
                </span>
              </article>

              <article className="system-metric">
                <span className="system-label">Alert Freshness</span>
                <strong>
                  {formatSeconds(systemSummary.alert.alert_freshness_seconds)}
                </strong>
                <span className="muted">
                  {systemSummary.alert.latest_severity ?? "no alert"}
                </span>
              </article>

              <article className="system-metric">
                <span className="system-label">Prediction Confidence</span>
                <strong>{formatPercent(systemSummary.prediction.confidence)}</strong>
                <span className="muted">
                  direction{" "}
                  {systemSummary.prediction.predicted_direction === 1
                    ? "UP"
                    : systemSummary.prediction.predicted_direction === 0
                      ? "DOWN"
                      : "-"}
                </span>
              </article>

              <article className="system-metric">
                <span className="system-label">Window Rows</span>
                <strong>
                  {systemSummary.counts.market_rows}/
                  {systemSummary.counts.news_rows}/
                  {systemSummary.counts.prediction_rows}/
                  {systemSummary.counts.alert_rows}
                </strong>
                <span className="muted">market/news/pred/alert</span>
              </article>
            </div>

            <p className="muted system-note">
              {systemSummary.note}
            </p>
          </>
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
