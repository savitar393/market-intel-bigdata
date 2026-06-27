# Model Evaluation Diagnostics

This report summarizes classification model prediction behavior using saved Spark prediction outputs.

**Champion model:** `gradient_boosted_trees`

## 1. Metrics from training script

| Model | Accuracy | F1 | Weighted Precision | Weighted Recall | ROC AUC | Split |
|---|---:|---:|---:|---:|---:|---|
| logistic_regression | 0.5515 | 0.4972 | 0.5504 | 0.5515 | 0.5610 | time_based_70_30_per_symbol |
| random_forest | 0.5483 | 0.4674 | 0.5528 | 0.5483 | 0.5518 | time_based_70_30_per_symbol |
| gradient_boosted_trees | 0.5347 | 0.5079 | 0.5253 | 0.5347 | 0.5321 | time_based_70_30_per_symbol |

## 2. Confusion and directional diagnostics

| Model | Rows | Accuracy | TP | TN | FP | FN | Actual UP Rate | Predicted UP Rate | F1 UP | F1 DOWN |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| gradient_boosted_trees | 4408 | 0.5347 | 587 | 1770 | 587 | 1464 | 0.4653 | 0.2663 | 0.3640 | 0.6332 |
| logistic_regression | 4408 | 0.5515 | 420 | 2011 | 346 | 1631 | 0.4653 | 0.1738 | 0.2982 | 0.6704 |
| random_forest | 4408 | 0.5483 | 282 | 2135 | 222 | 1769 | 0.4653 | 0.1143 | 0.2207 | 0.6820 |

## 3. Per-symbol diagnostics

| Model | Symbol | Rows | Accuracy | TP | TN | FP | FN | Actual UP Rate | Predicted UP Rate |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| gradient_boosted_trees | AAPL | 1096 | 0.5465 | 129 | 470 | 134 | 363 | 0.4489 | 0.2400 |
| gradient_boosted_trees | AMZN | 999 | 0.5596 | 133 | 426 | 156 | 284 | 0.4174 | 0.2893 |
| gradient_boosted_trees | MSFT | 1064 | 0.5489 | 179 | 405 | 164 | 316 | 0.4652 | 0.3224 |
| gradient_boosted_trees | NVDA | 1249 | 0.4924 | 146 | 469 | 133 | 501 | 0.5180 | 0.2234 |
| logistic_regression | AAPL | 1096 | 0.5493 | 16 | 586 | 18 | 476 | 0.4489 | 0.0310 |
| logistic_regression | AMZN | 999 | 0.5906 | 20 | 570 | 12 | 397 | 0.4174 | 0.0320 |
| logistic_regression | MSFT | 1064 | 0.5442 | 27 | 552 | 17 | 468 | 0.4652 | 0.0414 |
| logistic_regression | NVDA | 1249 | 0.5284 | 357 | 303 | 299 | 290 | 0.5180 | 0.5252 |
| random_forest | AAPL | 1096 | 0.5602 | 54 | 560 | 44 | 438 | 0.4489 | 0.0894 |
| random_forest | AMZN | 999 | 0.5946 | 77 | 517 | 65 | 340 | 0.4174 | 0.1421 |
| random_forest | MSFT | 1064 | 0.5555 | 77 | 514 | 55 | 418 | 0.4652 | 0.1241 |
| random_forest | NVDA | 1249 | 0.4948 | 74 | 544 | 58 | 573 | 0.5180 | 0.1057 |

## 4. Interpretation notes

- `TP` means the model predicted UP and the next return was actually UP.
- `TN` means the model predicted DOWN and the next return was actually DOWN.
- `FP` means the model predicted UP but the next return was DOWN.
- `FN` means the model predicted DOWN but the next return was UP.
- For short-horizon market prediction, near-random performance is common. The purpose of this report is to make model limitations visible instead of hiding them.
- Model quality should be interpreted together with data volume, prediction horizon, class balance, and time-based validation.
