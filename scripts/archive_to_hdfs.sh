#!/usr/bin/env bash
set -euo pipefail

NAMENODE_CONTAINER="${NAMENODE_CONTAINER:-market_hadoop_namenode}"

LOCAL_MARKET_PATH="${LOCAL_MARKET_PATH:-data/processed/market_ticks}"
LOCAL_NEWS_PATH="${LOCAL_NEWS_PATH:-data/processed/news_events}"
LOCAL_FEATURE_PATH="${LOCAL_FEATURE_PATH:-data/features/model_training_dataset}"

HDFS_BASE="${HDFS_BASE:-/market-intel}"

copy_dir_to_hdfs() {
  local local_path="$1"
  local hdfs_path="$2"

  if [[ ! -d "$local_path" ]]; then
    echo "SKIP: local path does not exist: $local_path"
    return
  fi

  echo "Archiving $local_path → hdfs://$hdfs_path"

  tmp_name="$(basename "$local_path")"
  docker exec "$NAMENODE_CONTAINER" rm -rf "/tmp/$tmp_name" || true
  docker cp "$local_path" "$NAMENODE_CONTAINER:/tmp/$tmp_name"

  docker exec "$NAMENODE_CONTAINER" hdfs dfs -rm -r -f "$hdfs_path" || true
  docker exec "$NAMENODE_CONTAINER" hdfs dfs -mkdir -p "$(dirname "$hdfs_path")"
  docker exec "$NAMENODE_CONTAINER" hdfs dfs -put "/tmp/$tmp_name" "$hdfs_path"

  docker exec "$NAMENODE_CONTAINER" rm -rf "/tmp/$tmp_name" || true
}

copy_dir_to_hdfs "$LOCAL_MARKET_PATH" "$HDFS_BASE/raw/market/market_ticks"
copy_dir_to_hdfs "$LOCAL_NEWS_PATH" "$HDFS_BASE/raw/news/news_events"
copy_dir_to_hdfs "$LOCAL_FEATURE_PATH" "$HDFS_BASE/features/model_training_dataset"

echo ""
echo "Archive complete. HDFS contents:"
docker exec "$NAMENODE_CONTAINER" hdfs dfs -ls -R "$HDFS_BASE"
