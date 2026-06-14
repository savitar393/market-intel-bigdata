CREATE KEYSPACE IF NOT EXISTS market_intel
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

USE market_intel;

CREATE TABLE IF NOT EXISTS market_ticks_by_symbol (
  symbol text,
  event_time timestamp,
  event_id text,
  event_type text,
  source text,
  dataset text,
  schema_name text,
  market_price double,
  price double,
  open double,
  high double,
  low double,
  close double,
  volume bigint,
  ingest_time timestamp,
  spark_process_time timestamp,
  ingest_latency_seconds double,
  raw_json text,
  PRIMARY KEY ((symbol), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time DESC);

CREATE TABLE IF NOT EXISTS news_events_by_symbol (
  symbol text,
  event_time timestamp,
  event_id text,
  source text,
  dataset text,
  schema_name text,
  headline text,
  summary text,
  category text,
  url text,
  related text,
  simple_sentiment_label text,
  simple_sentiment_score double,
  ingest_time timestamp,
  spark_process_time timestamp,
  ingest_latency_seconds double,
  raw_json text,
  PRIMARY KEY ((symbol), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time DESC);

CREATE TABLE IF NOT EXISTS model_predictions_by_symbol (
  symbol text,
  event_time timestamp,
  prediction_time timestamp,
  event_id text,
  model_name text,
  market_price double,
  predicted_direction int,
  probability_down double,
  probability_up double,
  target_direction int,
  source text,
  PRIMARY KEY ((symbol), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time DESC);

CREATE TABLE IF NOT EXISTS model_registry (
  model_name text,
  version text,
  selected_at timestamp,
  selection_metric text,
  accuracy double,
  f1 double,
  weighted_precision double,
  weighted_recall double,
  roc_auc double,
  notes text,
  PRIMARY KEY ((model_name), version)
);
