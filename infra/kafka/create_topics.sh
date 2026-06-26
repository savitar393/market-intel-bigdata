#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-infra/docker/compose.local.yml}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-market_kafka}"
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"

TOPICS=(
  "raw_market_ticks:3"
  "raw_news_events:3"
)

echo "Starting Kafka container if needed..."
docker compose -f "$COMPOSE_FILE" up -d kafka

echo "Waiting for Kafka to become available..."
for i in {1..30}; do
  if docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --list >/dev/null 2>&1; then
    echo "Kafka is ready."
    break
  fi

  if [[ "$i" -eq 30 ]]; then
    echo "Kafka did not become ready in time."
    exit 1
  fi

  sleep 2
done

for topic_spec in "${TOPICS[@]}"; do
  topic="${topic_spec%%:*}"
  partitions="${topic_spec##*:}"

  echo "Creating topic=$topic partitions=$partitions if missing..."

  docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 1
done

echo ""
echo "Kafka topics:"
docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --list

echo ""
echo "Topic details:"
for topic_spec in "${TOPICS[@]}"; do
  topic="${topic_spec%%:*}"

  docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --describe \
    --topic "$topic"
done

echo "Kafka topic bootstrap complete."
