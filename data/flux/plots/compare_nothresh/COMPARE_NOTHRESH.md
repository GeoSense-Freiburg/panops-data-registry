# Mortality block with vs without the thresholded metrics

Without = new_deadwood_gain_pp_thresh and relative_tree_loss_pct_thresh removed (12 m: 193 instead of 213 predictors, 24 m: 338 instead of 368). Everything else identical. XGBoost and LightGBM, Optuna re-tuned for each variant.

## Skill: mean R² of C+T+M (4 EFPs × 2 windows)

|                 |   with |   without |   diff |
|:----------------|-------:|----------:|-------:|
| ('V4.1', 'LGB') |  0.442 |     0.442 |  0     |
| ('V4.1', 'XGB') |  0.438 |     0.441 |  0.003 |
| ('V5', 'LGB')   |  0.408 |     0.394 | -0.014 |
| ('V5', 'XGB')   |  0.407 |     0.395 | -0.012 |

Per EFP (mean of learners and windows):

|                    |   with |   without |   diff |
|:-------------------|-------:|----------:|-------:|
| ('V4.1', 'ETmax')  |  0.445 |     0.446 |  0.001 |
| ('V4.1', 'GPPsat') |  0.415 |     0.416 |  0.001 |
| ('V4.1', 'NEPmax') |  0.529 |     0.538 |  0.008 |
| ('V4.1', 'WUE')    |  0.371 |     0.366 | -0.005 |
| ('V5', 'ETmax')    |  0.448 |     0.445 | -0.002 |
| ('V5', 'GPPsat')   |  0.364 |     0.354 | -0.01  |
| ('V5', 'NEPmax')   |  0.524 |     0.505 | -0.02  |
| ('V5', 'WUE')      |  0.295 |     0.275 | -0.02  |

## Effect of adding mortality: change in overall RMSE (%), mean of learners and windows

|                    |   with |   without |
|:-------------------|-------:|----------:|
| ('V4.1', 'ETmax')  |  -1.92 |     -1.79 |
| ('V4.1', 'GPPsat') |  -6.98 |     -7.07 |
| ('V4.1', 'NEPmax') |  -9.9  |    -10.04 |
| ('V4.1', 'WUE')    |  -6.14 |     -6.06 |
| ('V5', 'ETmax')    |  -1.06 |     -0.54 |
| ('V5', 'GPPsat')   |  -3.15 |     -3.09 |
| ('V5', 'NEPmax')   | -10.25 |     -8.22 |
| ('V5', 'WUE')      |  -4.49 |     -2.22 |

## Table 1 statistics: median per-site RMSE change (%) by total-mortality class
(mean of XGB/LGB × 12/24 m; 'sig' = number of the 4 learner-window tests with q < 0.05)

|                               |   with |   without |   sig with (of 4) |   sig without (of 4) |
|:------------------------------|-------:|----------:|------------------:|---------------------:|
| ('V4.1', 'all', 'GPPsat')     |  -6.22 |     -6.25 |                 4 |                    4 |
| ('V4.1', 'all', 'NEPmax')     | -11.47 |    -12.49 |                 4 |                    4 |
| ('V4.1', 'all', 'ETmax')      |  -1.98 |     -1.86 |                 1 |                    2 |
| ('V4.1', 'all', 'WUE')        |  -4.3  |     -2.65 |                 1 |                    0 |
| ('V4.1', 'midhigh', 'GPPsat') |  -7.48 |     -7.26 |                 4 |                    4 |
| ('V4.1', 'midhigh', 'NEPmax') | -11.43 |    -12.97 |                 4 |                    4 |
| ('V4.1', 'midhigh', 'ETmax')  |  -3.65 |     -3.02 |                 2 |                    2 |
| ('V4.1', 'midhigh', 'WUE')    |  -3.73 |     -2.24 |                 1 |                    0 |
| ('V4.1', 'high', 'GPPsat')    | -13.13 |    -14.81 |                 4 |                    4 |
| ('V4.1', 'high', 'NEPmax')    | -18.05 |    -17.11 |                 4 |                    4 |
| ('V4.1', 'high', 'ETmax')     |  -3.95 |     -0.8  |                 2 |                    0 |
| ('V4.1', 'high', 'WUE')       |  -5.12 |     -4.92 |                 1 |                    1 |
| ('V5', 'all', 'GPPsat')       |  -3.95 |     -2.21 |                 4 |                    4 |
| ('V5', 'all', 'NEPmax')       | -11.53 |     -9.47 |                 4 |                    4 |
| ('V5', 'all', 'ETmax')        |  -2.26 |     -1.75 |                 2 |                    2 |
| ('V5', 'all', 'WUE')          |  -2.02 |     -0.45 |                 1 |                    0 |
| ('V5', 'midhigh', 'GPPsat')   |  -3.89 |     -3.72 |                 3 |                    3 |
| ('V5', 'midhigh', 'NEPmax')   | -12.1  |     -8.82 |                 4 |                    4 |
| ('V5', 'midhigh', 'ETmax')    |  -2.63 |     -2.16 |                 2 |                    3 |
| ('V5', 'midhigh', 'WUE')      |  -4.19 |     -1.45 |                 3 |                    0 |
| ('V5', 'high', 'GPPsat')      |  -4.65 |     -3.53 |                 0 |                    0 |
| ('V5', 'high', 'NEPmax')      | -19.48 |    -13.5  |                 4 |                    4 |
| ('V5', 'high', 'ETmax')       |  -1.47 |     -1.69 |                 0 |                    0 |
| ('V5', 'high', 'WUE')         |  -9.67 |     -7.56 |                 4 |                    4 |

## Fig 4 (direction, High vs Low+Mid): significant tests

|                     | relative deadwood significant   | total mortality significant   |
|:--------------------|:--------------------------------|:------------------------------|
| ('V4.1', 'with')    | 13 of 16                        | 16 of 16                      |
| ('V4.1', 'without') | 13 of 16                        | 16 of 16                      |
| ('V5', 'with')      | 12 of 16                        | 15 of 16                      |
| ('V5', 'without')   | 13 of 16                        | 16 of 16                      |
