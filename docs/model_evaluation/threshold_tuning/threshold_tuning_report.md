# Classification Threshold Tuning

This report tunes the probability threshold for predicting UP direction.

Default Spark classification uses a 0.50 threshold. This search tests thresholds from 0.35 to 0.65.

## Top thresholds by weighted F1

| Rank | Model | Threshold | Accuracy | Weighted F1 | Precision UP | Recall UP | Predicted UP Rate |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | gradient_boosted_trees | 0.48 | 0.5338 | 0.5329 | 0.4899 | 0.4712 | 0.4399 |
| 2 | logistic_regression | 0.47 | 0.5317 | 0.5323 | 0.4885 | 0.5082 | 0.4757 |
| 3 | logistic_regression | 0.48 | 0.5390 | 0.5322 | 0.4950 | 0.4037 | 0.3730 |
| 4 | random_forest | 0.49 | 0.5399 | 0.5313 | 0.4961 | 0.3890 | 0.3586 |
| 5 | random_forest | 0.48 | 0.5304 | 0.5309 | 0.4870 | 0.5045 | 0.4737 |
| 6 | logistic_regression | 0.46 | 0.5298 | 0.5292 | 0.4884 | 0.5942 | 0.5564 |
| 7 | gradient_boosted_trees | 0.49 | 0.5366 | 0.5289 | 0.4916 | 0.3935 | 0.3660 |
| 8 | gradient_boosted_trees | 0.47 | 0.5274 | 0.5282 | 0.4851 | 0.5465 | 0.5151 |
| 9 | random_forest | 0.47 | 0.5275 | 0.5257 | 0.4868 | 0.6125 | 0.5754 |
| 10 | logistic_regression | 0.49 | 0.5456 | 0.5228 | 0.5052 | 0.3089 | 0.2796 |