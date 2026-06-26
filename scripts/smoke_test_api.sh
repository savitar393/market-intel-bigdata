#!/usr/bin/env bash
set -euo pipefail

API_BASE="${API_BASE:-http://localhost:8000}"
SYMBOL="${SYMBOL:-AAPL}"

echo "Testing API_BASE=$API_BASE SYMBOL=$SYMBOL"

echo ""
echo "1. Health"
curl -fsS "$API_BASE/api/v1/health" | python -m json.tool >/dev/null
echo "OK: health"

echo ""
echo "2. Market latest"
curl -fsS "$API_BASE/api/v1/market/latest/$SYMBOL?limit=3" | python -m json.tool >/dev/null
echo "OK: market latest"

echo ""
echo "3. News latest"
curl -fsS "$API_BASE/api/v1/news/latest/$SYMBOL?limit=3" | python -m json.tool >/dev/null
echo "OK: news latest"

echo ""
echo "4. Predictions latest"
curl -fsS "$API_BASE/api/v1/predictions/latest/$SYMBOL?limit=3" | python -m json.tool >/dev/null
echo "OK: predictions latest"

echo ""
echo "5. Alerts latest"
curl -fsS "$API_BASE/api/v1/alerts/latest/$SYMBOL?limit=3" | python -m json.tool >/dev/null
echo "OK: alerts latest"

echo ""
echo "6. Dashboard snapshot"
curl -fsS "$API_BASE/api/v1/dashboard/snapshot/$SYMBOL" | python -m json.tool >/dev/null
echo "OK: dashboard snapshot"

echo ""
echo "7. System summary"
curl -fsS "$API_BASE/api/v1/system/summary/$SYMBOL?limit=20" | python -m json.tool >/dev/null
echo "OK: system summary"

echo ""
echo "8. Prometheus metrics"
curl -fsS "$API_BASE/metrics" | grep -q "market_intel"
echo "OK: metrics"

echo ""
echo "Smoke test passed."
