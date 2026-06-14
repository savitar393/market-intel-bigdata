# Model Evaluation Summary

## Experiment context

This experiment trains three baseline classification models to predict the next-interval price direction from market and news-derived features.

The target variable is `target_direction`:

- `1` = next interval return is positive
- `0` = next interval return is not positive

## Model comparison

| Model | Train rows | Test rows | Accuracy | F1 | Precision | Recall | ROC-AUC |
|---|---:|---:|---:|---:|---:|---:|---:|
| logistic_regression | 1070 | 446 | 0.4933 | 0.4930 | 0.5034 | 0.4933 | 0.5047 |
| random_forest | 1070 | 446 | 0.4529 | 0.4527 | 0.4622 | 0.4529 | 0.4443 |
| gradient_boosted_trees | 1070 | 446 | 0.4596 | 0.4605 | 0.4670 | 0.4596 | 0.4410 |

## Champion model

Champion model: **logistic_regression**

The champion model is selected using F1 score as the primary metric. Accuracy and ROC-AUC are used as secondary tie-breakers.

## Interpretation

The current baseline models perform close to random-level prediction. This result is acceptable at this milestone because the purpose is to validate the full big-data ML pipeline: data ingestion, Spark processing, feature generation, model training, and metric reporting.

The current dataset uses a limited historical window and a simple target. Future improvements should include a longer historical window, richer rolling features, better time alignment between market and news events, and additional sequence-based models.

## Feature columns

- `market_price`
- `volume`
- `return_1`
- `return_2`
- `return_3`
- `return_5`
- `return_10`
- `rolling_mean_3`
- `rolling_mean_5`
- `rolling_mean_10`
- `rolling_mean_30`
- `rolling_price_std_5`
- `rolling_price_std_10`
- `rolling_price_std_30`
- `rolling_volatility_3`
- `rolling_volatility_5`
- `rolling_volatility_10`
- `rolling_volatility_30`
- `rolling_volume_mean_5`
- `rolling_volume_mean_10`
- `volume_surprise_5`
- `volume_surprise_10`
- `log_volume`
- `bar_range`
- `candle_body`
- `upper_shadow`
- `lower_shadow`
- `minute_of_day`
- `news_count`
- `avg_sentiment_score`
- `positive_news_count`
- `negative_news_count`