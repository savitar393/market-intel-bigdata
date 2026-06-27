# Classification Threshold Tuning

This report tunes the probability threshold for predicting UP direction.

Default Spark classification uses a 0.50 threshold. This search tests thresholds from 0.35 to 0.65.

## Top thresholds by weighted F1

| Rank | Model | Threshold | Accuracy | Weighted F1 | Precision UP | Recall UP | Predicted UP Rate |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | logistic_regression | 0.47 | 0.5374 | 0.5367 | 0.5038 | 0.5959 | 0.5521 |
| 2 | logistic_regression | 0.48 | 0.5335 | 0.5336 | 0.5003 | 0.5027 | 0.4690 |
| 3 | logistic_regression | 0.46 | 0.5390 | 0.5332 | 0.5047 | 0.6668 | 0.6167 |
| 4 | logistic_regression | 0.49 | 0.5356 | 0.5305 | 0.5031 | 0.4210 | 0.3906 |
| 5 | gradient_boosted_trees | 0.49 | 0.5304 | 0.5296 | 0.4968 | 0.4750 | 0.4462 |
| 6 | gradient_boosted_trees | 0.50 | 0.5338 | 0.5266 | 0.5008 | 0.4002 | 0.3730 |
| 7 | gradient_boosted_trees | 0.48 | 0.5259 | 0.5264 | 0.4928 | 0.5383 | 0.5099 |
| 8 | gradient_boosted_trees | 0.47 | 0.5272 | 0.5260 | 0.4946 | 0.5932 | 0.5598 |
| 9 | random_forest | 0.49 | 0.5272 | 0.5251 | 0.4929 | 0.4508 | 0.4269 |
| 10 | logistic_regression | 0.50 | 0.5414 | 0.5240 | 0.5133 | 0.3381 | 0.3074 |