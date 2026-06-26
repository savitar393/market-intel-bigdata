#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-infra/docker/compose.local.yml}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-market_kafka}"
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"

TOPICS=(
  "raw_market_ticks:3"
  "raw_news_events:3"
)

echo "WARNING: this deletes Kafka topics and all messages in them."
read -r -p "Continue? Type 'yes' to proceed: " confirm

if [[ "$confirm" != "yes" ]]; then
  echo "Aborted."
  exit 0
fi

docker compose -f "$COMPOSE_FILE" up -d kafka

echo "Waiting for Kafka..."
for i in {1..30}; do
  if docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --list >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

for topic_spec in "${TOPICS[@]}"; do
  topic="${topic_spec%%:*}"

  echo "Deleting topic=$topic if it exists..."
  docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --delete \
    --topic "$topic" || true
done

echo "Waiting for deletion propagation..."
sleep 5

for topic_spec in "${TOPICS[@]}"; do
  topic="${topic_spec%%:*}"
  partitions="${topic_spec##*:}"

  echo "Recreating topic=$topic..."
  docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 1
done

echo "Kafka topics reset complete."
