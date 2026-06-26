#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-infra/docker/compose.local.yml}"
NAMENODE_CONTAINER="${NAMENODE_CONTAINER:-market_hadoop_namenode}"

docker compose -f "$COMPOSE_FILE" up -d hadoop-namenode hadoop-datanode

echo "Waiting for HDFS..."
for i in {1..60}; do
  if docker exec "$NAMENODE_CONTAINER" hdfs dfs -ls / >/dev/null 2>&1; then
    echo "HDFS is ready."
    break
  fi

  if [[ "$i" -eq 60 ]]; then
    echo "HDFS did not become ready in time."
    exit 1
  fi

  sleep 2
done

docker exec "$NAMENODE_CONTAINER" hdfs dfs -mkdir -p /market-intel/raw/market
docker exec "$NAMENODE_CONTAINER" hdfs dfs -mkdir -p /market-intel/raw/news
docker exec "$NAMENODE_CONTAINER" hdfs dfs -mkdir -p /market-intel/features
docker exec "$NAMENODE_CONTAINER" hdfs dfs -mkdir -p /market-intel/model-artifacts

echo "HDFS directories:"
docker exec "$NAMENODE_CONTAINER" hdfs dfs -ls -R /market-intel
