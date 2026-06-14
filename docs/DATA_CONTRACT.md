# Data Contract

## Kafka topic: raw_market_ticks

This topic stores normalized market tick/bar events from multiple sources.

## Canonical event schema

```json
{
  "event_id": "uuid",
  "payload_version": "1.0",
  "event_type": "market_bar",
  "symbol": "AAPL",
  "source": "databento_historical",
  "dataset": "XNAS.ITCH",
  "schema": "ohlcv-1m",
  "event_ts": "2024-05-21T15:00:00Z",
  "ingest_ts": "2026-06-08T12:00:00Z",
  "open": 190.0,
  "high": 191.0,
  "low": 189.5,
  "close": 190.7,
  "volume": 10000
}
