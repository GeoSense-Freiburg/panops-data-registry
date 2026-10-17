# TabPFN v2 screening test - efp_v5_nothresh

101 sites / 361 site-years; AutoGluon default zoo + TabPFN v2; grouped 5-fold site CV; one GPU.

- TabPFN gets ensemble weight > 0 in 4 of 16 tasks (mean weight 0.02)
- median rank among single models: 9 of 10; best single model in 0 tasks
- total time 38 min

| response   | model_id   |   n_predictors | best_single     |   best_single_rmse_sd |   tabpfn_rmse_sd |   tabpfn_rank |   n_models |   tabpfn_weight |   ensemble_rmse_sd |   fit_s |
|:-----------|:-----------|---------------:|:----------------|----------------------:|-----------------:|--------------:|-----------:|----------------:|-------------------:|--------:|
| GPPsat     | M3_12m     |            143 | NeuralNetTorch  |                 0.771 |            0.955 |            10 |         10 |           0     |              0.768 | 107.161 |
| GPPsat     | M4_12m     |            193 | NeuralNetTorch  |                 0.772 |            0.903 |             9 |         10 |           0     |              0.754 | 136.4   |
| NEPmax     | M3_12m     |            143 | NeuralNetTorch  |                 0.715 |            0.904 |             9 |         10 |           0     |              0.71  |  99.242 |
| NEPmax     | M4_12m     |            193 | NeuralNetTorch  |                 0.662 |            0.767 |             7 |         10 |           0.043 |              0.652 | 138.665 |
| ETmax      | M3_12m     |            143 | NeuralNetTorch  |                 0.76  |            0.819 |            10 |         10 |           0     |              0.737 | 107.265 |
| ETmax      | M4_12m     |            193 | NeuralNetTorch  |                 0.762 |            0.822 |             9 |         10 |           0     |              0.739 | 115.935 |
| WUE        | M3_12m     |            143 | NeuralNetFastAI |                 0.819 |            0.888 |             7 |         10 |           0.125 |              0.812 | 139.538 |
| WUE        | M4_12m     |            193 | LightGBMXT      |                 0.821 |            0.852 |             6 |         10 |           0.1   |              0.789 | 163.244 |
| GPPsat     | M3_24m     |            263 | NeuralNetTorch  |                 0.786 |            0.931 |            10 |         10 |           0     |              0.778 | 156.598 |
| GPPsat     | M4_24m     |            338 | NeuralNetFastAI |                 0.787 |            0.926 |            10 |         10 |           0     |              0.755 | 204.716 |
| NEPmax     | M3_24m     |            263 | NeuralNetTorch  |                 0.724 |            0.884 |             9 |         10 |           0     |              0.719 | 139.109 |
| NEPmax     | M4_24m     |            338 | NeuralNetTorch  |                 0.689 |            0.787 |             8 |         10 |           0     |              0.664 | 171.1   |
| ETmax      | M3_24m     |            263 | NeuralNetTorch  |                 0.766 |            0.822 |             9 |         10 |           0     |              0.748 | 129.075 |
| ETmax      | M4_24m     |            338 | NeuralNetTorch  |                 0.764 |            0.813 |             8 |         10 |           0     |              0.74  | 147.163 |
| WUE        | M3_24m     |            263 | NeuralNetTorch  |                 0.819 |            0.892 |             8 |         10 |           0.071 |              0.794 | 142.424 |
| WUE        | M4_24m     |            338 | NeuralNetFastAI |                 0.76  |            0.859 |             6 |         10 |           0     |              0.733 | 193.748 |
