# Final Model Summary

## 1. Modeling Tasks

The project evaluates two prediction tasks:

1. **Classification task**  
   Predict whether the next short-horizon return direction is UP or DOWN.

2. **Regression task**  
   Predict the next short-horizon return value.

The model dataset is built from market time-series features and news sentiment features. The split strategy uses a time-based 70:30 split per symbol to reduce future leakage.

## 2. Horizon Comparison

Three prediction horizons were evaluated.

| Horizon | Classification Target | Regression Target |
|---:|---|---|
| 1 minute | `target_direction_1` | `target_return_1` |
| 5 minutes | `target_direction_5` | `target_return_5` |
| 10 minutes | `target_direction_10` | `target_return_10` |

## 3. Best Classification Result

The best classification setup is the 1-minute horizon.

| Model | Horizon | Accuracy | F1 | ROC-AUC |
|---|---:|---:|---:|---:|
| Logistic Regression | 1 min | 0.5414 | 0.5240 | 0.5541 |
| Gradient-Boosted Trees | 1 min | 0.5338 | 0.5266 | 0.5441 |
| Random Forest | 1 min | 0.5286 | 0.5130 | 0.5459 |

Interpretation:

- Gradient-Boosted Trees has the best default-threshold F1.
- Logistic Regression has the best accuracy and ROC-AUC.
- Because Logistic Regression has the strongest probability ranking, it is selected for threshold tuning.

## 4. Threshold Tuning Result

Threshold tuning tested probability thresholds from 0.35 to 0.65.

| Model | Horizon | Threshold | Accuracy | Weighted F1 | Precision UP | Recall UP |
|---|---:|---:|---:|---:|---:|---:|
| Logistic Regression | 1 min | 0.47 | 0.5374 | 0.5367 | 0.5038 | 0.5959 |

Final served classifier:

```text
Model: Logistic Regression
Target: target_direction_1
Decision rule: probability_up >= 0.47
Purpose: short-horizon UP/DOWN direction prediction
```

## 5. Best Regression Result

Regression was evaluated for 1-minute, 5-minute, and 10-minute returns.

| Model | Horizon | RMSE | MAE | R² | Directional Accuracy |
|---|---:|---:|---:|---:|---:|
| Linear Regression | 10 min | 0.002576 | 0.001324 | 0.0017 | 0.5083 |

Interpretation:

- The 10-minute Linear Regression model gives the best directional accuracy among regression models.
- The R² is only slightly positive, so regression is treated as an experimental comparison rather than the main served prediction.
- MAPE is not emphasized because return targets are often close to zero, which makes percentage error unstable.

## 6. Final Modeling Decision

The final system uses:

```text
Primary served model:
1-minute Logistic Regression classifier with tuned threshold 0.47

Supporting experiment:
10-minute Linear Regression return model
```

The classification model is used in the dashboard and alert system because it gives clearer UP/DOWN output for users. The regression result is reported as an additional experiment to compare return-size prediction against directional classification.

## 7. Limitations

The model results should be interpreted as an academic prototype, not as investment advice.

Important limitations:

1. Short-horizon stock movement is noisy and difficult to predict.
2. The current model uses a limited number of symbols and historical window.
3. News sentiment features depend on historical date alignment and API availability.
4. The system prioritizes demonstrating an end-to-end big data architecture, not building a production trading signal.
5. Accuracy and F1 are only one part of the evaluation; system latency, freshness, monitoring, and deployment architecture are also evaluated.
