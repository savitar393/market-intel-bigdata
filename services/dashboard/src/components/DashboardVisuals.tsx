import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

export type ModelMetricItem = {
  model_name: string;
  model_label: string;
  accuracy?: number | null;
  f1?: number | null;
  roc_auc?: number | null;
};

export type PredictionTimelineItem = {
  event_time: string;
  prediction_time?: string;
  market_price?: number | null;
  predicted_direction?: number | null;
  prediction_label?: string;
  probability_up?: number | null;
  probability_down?: number | null;
  confidence?: number | null;
  model_name?: string | null;
  source?: string | null;
};

export type DailyTrendItem = {
  event_date: string;
  close?: number | null;
  ma_100?: number | null;
  ma_200?: number | null;
  ma_100_ratio?: number | null;
  ma_200_ratio?: number | null;
};

function percent(value?: number | null) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }

  return `${(value * 100).toFixed(1)}%`;
}

function number(value?: number | null) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }

  return value.toLocaleString(undefined, {
    maximumFractionDigits: 2,
  });
}

function dateLabel(value: string) {
  if (!value) {
    return "-";
  }

  const parsed = new Date(value);

  if (Number.isNaN(parsed.getTime())) {
    return value;
  }

  return parsed.toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
  });
}

function timeLabel(value: string) {
  if (!value) {
    return "-";
  }

  const parsed = new Date(value);

  if (Number.isNaN(parsed.getTime())) {
    return value;
  }

  return parsed.toLocaleTimeString(undefined, {
    hour: "2-digit",
    minute: "2-digit",
  });
}

export function ModelPerformanceChart({
  items,
}: {
  items: ModelMetricItem[];
}) {
  const data = items.map((item) => ({
    model: item.model_label || item.model_name,
    accuracy: item.accuracy ?? 0,
    f1: item.f1 ?? 0,
    roc_auc: item.roc_auc ?? 0,
  }));

  if (data.length === 0) {
    return <p className="muted">No model evaluation metrics found.</p>;
  }

  return (
    <div className="chart">
      <ResponsiveContainer width="100%" height={320}>
        <BarChart data={data}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="model" />
          <YAxis tickFormatter={(value) => `${(Number(value) * 100).toFixed(0)}%`} />
          <Tooltip formatter={(value) => percent(Number(value))} />
          <Legend />
          <Bar dataKey="accuracy" name="Accuracy" />
          <Bar dataKey="f1" name="F1-score" />
          <Bar dataKey="roc_auc" name="ROC-AUC" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

export function PredictionTimelineChart({
  items,
}: {
  items: PredictionTimelineItem[];
}) {
  const data = items
    .filter((item) => item.event_time && item.market_price !== null)
    .map((item) => ({
      ...item,
      time: timeLabel(item.event_time),
      upPrice: item.predicted_direction === 1 ? item.market_price : null,
      downPrice: item.predicted_direction === 0 ? item.market_price : null,
      confidencePercent:
        item.confidence !== null && item.confidence !== undefined
          ? item.confidence * 100
          : null,
    }));

  if (data.length === 0) {
    return <p className="muted">No prediction timeline records found.</p>;
  }

  return (
    <div className="chart">
      <ResponsiveContainer width="100%" height={360}>
        <ScatterChart data={data}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="time" minTickGap={24} />
          <YAxis
            yAxisId="price"
            dataKey="market_price"
            domain={["auto", "auto"]}
            tickFormatter={(value) => number(Number(value))}
          />
          <Tooltip
            formatter={(value, name) => {
              if (name === "Confidence") {
                return [`${Number(value).toFixed(1)}%`, name];
              }

              return [number(Number(value)), name];
            }}
            labelFormatter={(_, payload) => {
              const item = payload?.[0]?.payload as PredictionTimelineItem | undefined;
              return item?.event_time
                ? new Date(item.event_time).toLocaleString()
                : "-";
            }}
          />
          <Line
            yAxisId="price"
            type="monotone"
            dataKey="market_price"
            name="Market price"
            strokeWidth={2}
            dot={false}
            isAnimationActive={false}
          />
          <Scatter
            yAxisId="price"
            dataKey="upPrice"
            name="Predicted UP"
            isAnimationActive={false}
          />
          <Scatter
            yAxisId="price"
            dataKey="downPrice"
            name="Predicted DOWN"
            isAnimationActive={false}
          />
        </ScatterChart>
      </ResponsiveContainer>
    </div>
  );
}

export function DailyTrendChart({
  items,
}: {
  items: DailyTrendItem[];
}) {
  const data = items
    .filter((item) => item.event_date)
    .map((item) => ({
      ...item,
      date: dateLabel(item.event_date),
    }));

  if (data.length === 0) {
    return <p className="muted">No daily context records found.</p>;
  }

  return (
    <div className="chart">
      <ResponsiveContainer width="100%" height={360}>
        <LineChart data={data}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="date" minTickGap={24} />
          <YAxis domain={["auto", "auto"]} tickFormatter={(value) => number(Number(value))} />
          <Tooltip
            formatter={(value, name) => [number(Number(value)), name]}
            labelFormatter={(_, payload) => {
              const item = payload?.[0]?.payload as DailyTrendItem | undefined;
              return item?.event_date ?? "-";
            }}
          />
          <Legend />
          <Line type="monotone" dataKey="close" name="Daily close" dot={false} strokeWidth={2} />
          <Line type="monotone" dataKey="ma_100" name="MA100" dot={false} strokeWidth={2} />
          <Line type="monotone" dataKey="ma_200" name="MA200" dot={false} strokeWidth={2} />
          <ReferenceLine y={0} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
