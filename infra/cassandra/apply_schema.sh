#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-infra/docker/compose.local.yml}"
CASSANDRA_CONTAINER="${CASSANDRA_CONTAINER:-market_cassandra}"
SCHEMA_FILE="${SCHEMA_FILE:-infra/cassandra/schema.cql}"

echo "Starting Cassandra container if needed..."
docker compose -f "$COMPOSE_FILE" up -d cassandra

echo "Waiting for Cassandra cqlsh..."
for i in {1..60}; do
  if docker exec "$CASSANDRA_CONTAINER" cqlsh -e "DESCRIBE KEYSPACES;" >/dev/null 2>&1; then
    echo "Cassandra is ready."
    break
  fi

  if [[ "$i" -eq 60 ]]; then
    echo "Cassandra did not become ready in time."
    exit 1
  fi

  sleep 2
done

echo "Applying schema: $SCHEMA_FILE"
docker cp "$SCHEMA_FILE" "$CASSANDRA_CONTAINER:/schema.cql"
docker exec "$CASSANDRA_CONTAINER" cqlsh -f /schema.cql

echo "Cassandra schema applied."
docker exec "$CASSANDRA_CONTAINER" cqlsh -e "DESCRIBE KEYSPACE market_intel;"
