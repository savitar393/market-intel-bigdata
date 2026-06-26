SHELL := /usr/bin/env bash
.RECIPEPREFIX := >

COMPOSE_FILE ?= infra/docker/compose.local.yml
SYMBOL ?= AAPL

.PHONY: help
help:
> @echo "Market Intel Big Data commands"
> @echo ""
> @echo "Infrastructure:"
> @echo "  make infra-up             Start all Docker services"
> @echo "  make infra-down           Stop Docker services"
> @echo "  make ps                   Show containers"
> @echo "  make logs-api             Tail API logs"
> @echo "  make logs-cassandra       Tail Cassandra logs"
> @echo "  make logs-kafka           Tail Kafka logs"
> @echo ""
> @echo "Bootstrap:"
> @echo "  make topics               Create Kafka topics"
> @echo "  make reset-topics         Delete/recreate Kafka topics"
> @echo "  make schema               Apply Cassandra schema"
> @echo "  make bootstrap            Start infra + topics + schema"
> @echo ""
> @echo "Local development:"
> @echo "  make api                  Run FastAPI locally"
> @echo "  make dashboard            Run React dashboard locally"
> @echo "  make smoke                Run API smoke test"
> @echo ""
> @echo "Data and ML:"
> @echo "  make validate-databento   Validate Databento data"
> @echo "  make validate-yfinance    Validate yfinance WebSocket"
> @echo "  make validate-finnhub     Validate Finnhub REST data"
> @echo "  make build-features       Build feature dataset"
> @echo "  make train-cls            Train classification models"
> @echo "  make train-reg            Train regression models"
> @echo "  make select-champion      Select champion classifier"
> @echo "  make load-predictions     Load predictions to Cassandra"
> @echo "  make generate-alerts      Generate prediction alerts"

.PHONY: infra-up
infra-up:
> docker compose -f $(COMPOSE_FILE) up -d --build

.PHONY: infra-down
infra-down:
> docker compose -f $(COMPOSE_FILE) down

.PHONY: ps
ps:
> docker ps

.PHONY: logs-api
logs-api:
> docker logs market_api --tail 100 -f

.PHONY: logs-cassandra
logs-cassandra:
> docker logs market_cassandra --tail 100 -f

.PHONY: logs-kafka
logs-kafka:
> docker logs market_kafka --tail 100 -f

.PHONY: topics
topics:
> bash infra/kafka/create_topics.sh

.PHONY: reset-topics
reset-topics:
> bash infra/kafka/reset_topics.sh

.PHONY: schema
schema:
> bash infra/cassandra/apply_schema.sh

.PHONY: bootstrap
bootstrap: infra-up topics schema
> @echo "Bootstrap complete."

.PHONY: api
api:
> uvicorn services.api.main:app --reload --host 0.0.0.0 --port 8000

.PHONY: dashboard
dashboard:
> cd services/dashboard && npm run dev -- --host 0.0.0.0

.PHONY: smoke
smoke:
> SYMBOL=$(SYMBOL) bash scripts/smoke_test_api.sh

.PHONY: validate-databento
validate-databento:
> python services/producers/validate_databento.py

.PHONY: validate-yfinance
validate-yfinance:
> python services/producers/validate_yfinance_live.py

.PHONY: validate-finnhub
validate-finnhub:
> python services/producers/validate_finnhub.py

.PHONY: replay-databento
replay-databento:
> python services/producers/databento_replay_producer.py --speed 3600

.PHONY: produce-yfinance
produce-yfinance:
> python services/producers/yfinance_live_producer.py --symbols AAPL,MSFT,NVDA,AMZN,BTC-USD --max-messages 20

.PHONY: produce-news
produce-news:
> python services/producers/finnhub_news_producer.py --symbols AAPL,MSFT,NVDA --max-events-per-symbol 5 --once

.PHONY: build-features
build-features:
> rm -rf data/features/model_training_dataset
> spark-submit spark/jobs/build_market_news_features.py

.PHONY: train-cls
train-cls:
> rm -rf data/model_artifacts/baseline_models
> spark-submit ml/training/train_spark_baseline_models.py

.PHONY: train-reg
train-reg:
> rm -rf data/model_artifacts/regression_models
> spark-submit ml/training/train_spark_regression_models.py

.PHONY: select-champion
select-champion:
> python ml/evaluation/select_champion_model.py

.PHONY: load-predictions
load-predictions:
> python ml/serving/load_predictions_to_cassandra.py

.PHONY: generate-alerts
generate-alerts:
> python services/alerts/generate_prediction_alerts.py --symbols AAPL,MSFT,NVDA --threshold 0.55

.PHONY: telegram-dry-run
telegram-dry-run:
> python services/notifications/send_telegram_alerts.py --symbols AAPL,MSFT,NVDA --limit 3 --dry-run

.PHONY: mlflow-up
mlflow-up:
> docker compose -f $(COMPOSE_FILE) up -d --build mlflow

.PHONY: log-mlflow
log-mlflow:
> python ml/tracking/log_mlflow_runs.py --all

.PHONY: log-mlflow-artifacts
log-mlflow-artifacts:
> python ml/tracking/log_mlflow_runs.py --all --log-model-artifacts
