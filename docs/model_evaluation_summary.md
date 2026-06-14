# Model Evaluation Summary

## Experiment context

This experiment trains three baseline classification models to predict the next-interval price direction from market and news-derived features.

The target variable is `target_direction`:

- `1` = next interval return is positive
- `0` = next interval return is not positive

## Model comparison

| Model | Train rows | Test rows | Accuracy | F1 | Precision | Recall | ROC-AUC |
|---|---:|---:|---:|---:|---:|---:|---:|
| logistic_regression | 1096 | 456 | 0.4825 | 0.4647 | 0.4807 | 0.4825 | 0.4669 |
| random_forest | 1096 | 456 | 0.4496 | 0.4429 | 0.4475 | 0.4496 | 0.4426 |
| gradient_boosted_trees | 1096 | 456 | 0.4978 | 0.4975 | 0.4979 | 0.4978 | 0.4753 |

## Champion model

Champion model: **gradient_boosted_trees**

The champion model is selected using F1 score as the primary metric. Accuracy and ROC-AUC are used as secondary tie-breakers.

## Interpretation

The current baseline models perform close to random-level prediction. This result is acceptable at this milestone because the purpose is to validate the full big-data ML pipeline: data ingestion, Spark processing, feature generation, model training, and metric reporting.

The current dataset uses a limited historical window and a simple target. Future improvements should include a longer historical window, richer rolling features, better time alignment between market and news events, and additional sequence-based models.

## Feature columns

- `market_price`
- `volume`
- `return_1`
- `rolling_mean_3`
- `rolling_volatility_3`
- `news_count`
- `avg_sentiment_score`
- `positive_news_count`
- `negative_news_count`