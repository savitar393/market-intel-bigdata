#!/usr/bin/env bash
set -euo pipefail

OUTPUT_DIR="docs/model_evaluation/horizon_experiments"

mkdir -p "$OUTPUT_DIR/classification"
mkdir -p "$OUTPUT_DIR/regression"

echo "Running horizon experiments..."
echo "Output directory: $OUTPUT_DIR"

run_classification_horizon() {
  local horizon="$1"
  local label_col="target_direction_${horizon}"

  echo ""
  echo "=== Classification horizon ${horizon} minute(s): ${label_col} ==="

  rm -rf data/model_artifacts/baseline_models

  CLASSIFICATION_LABEL_COLUMN="$label_col" \
    spark-submit ml/training/train_spark_baseline_models.py

  cp data/model_artifacts/baseline_models/model_metrics.json \
    "$OUTPUT_DIR/classification/model_metrics_h${horizon}.json"

  echo "Saved classification metrics: $OUTPUT_DIR/classification/model_metrics_h${horizon}.json"
}

run_regression_horizon() {
  local horizon="$1"
  local label_col="target_return_${horizon}"

  echo ""
  echo "=== Regression horizon ${horizon} minute(s): ${label_col} ==="

  rm -rf data/model_artifacts/regression_models

  REGRESSION_LABEL_COLUMN="$label_col" \
    spark-submit ml/training/train_spark_regression_models.py

  cp data/model_artifacts/regression_models/regression_model_metrics.json \
    "$OUTPUT_DIR/regression/regression_model_metrics_h${horizon}.json"

  echo "Saved regression metrics: $OUTPUT_DIR/regression/regression_model_metrics_h${horizon}.json"
}

for horizon in 1 5 10; do
  run_classification_horizon "$horizon"
done

for horizon in 1 5 10; do
  run_regression_horizon "$horizon"
done

echo ""
echo "All horizon experiments completed."
echo ""
find "$OUTPUT_DIR" -type f | sort
