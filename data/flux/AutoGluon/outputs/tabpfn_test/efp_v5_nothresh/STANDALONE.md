# TabPFN v2 standalone vs inside AutoGluon - efp_v5_nothresh

Out-of-fold RMSE / sd(y) on the same site-disjoint 5 folds. Lower is better. best_single = best AutoGluon base model.

- standalone default: median 0.871; n_estimators 32: median 0.865; inside AutoGluon: median 0.872; best single model: median 0.765
- standalone TabPFN beats the best AutoGluon base model in 0 of 16 tasks
- total time 9 min

| response   | model_id   |   standalone_default |   standalone_32 |   tabpfn_in_autogluon | best_single     |   best_single_rmse_sd |   ensemble_rmse_sd |   gap_to_best_single |
|:-----------|:-----------|---------------------:|----------------:|----------------------:|:----------------|----------------------:|-------------------:|---------------------:|
| ETmax      | M3_12m     |                0.826 |           0.823 |                 0.819 | NeuralNetTorch  |                 0.76  |              0.737 |                0.063 |
| ETmax      | M3_24m     |                0.823 |           0.822 |                 0.822 | NeuralNetTorch  |                 0.766 |              0.748 |                0.056 |
| ETmax      | M4_12m     |                0.819 |           0.826 |                 0.822 | NeuralNetTorch  |                 0.762 |              0.739 |                0.057 |
| ETmax      | M4_24m     |                0.822 |           0.816 |                 0.813 | NeuralNetTorch  |                 0.764 |              0.74  |                0.051 |
| GPPsat     | M3_12m     |                0.954 |           0.952 |                 0.955 | NeuralNetTorch  |                 0.771 |              0.768 |                0.181 |
| GPPsat     | M3_24m     |                0.935 |           0.939 |                 0.931 | NeuralNetTorch  |                 0.786 |              0.778 |                0.149 |
| GPPsat     | M4_12m     |                0.924 |           0.917 |                 0.903 | NeuralNetTorch  |                 0.772 |              0.754 |                0.144 |
| GPPsat     | M4_24m     |                0.923 |           0.913 |                 0.926 | NeuralNetFastAI |                 0.787 |              0.755 |                0.126 |
| NEPmax     | M3_12m     |                0.895 |           0.894 |                 0.904 | NeuralNetTorch  |                 0.715 |              0.71  |                0.179 |
| NEPmax     | M3_24m     |                0.874 |           0.871 |                 0.884 | NeuralNetTorch  |                 0.724 |              0.719 |                0.146 |
| NEPmax     | M4_12m     |                0.784 |           0.767 |                 0.767 | NeuralNetTorch  |                 0.662 |              0.652 |                0.105 |
| NEPmax     | M4_24m     |                0.762 |           0.767 |                 0.787 | NeuralNetTorch  |                 0.689 |              0.664 |                0.073 |
| WUE        | M3_12m     |                0.89  |           0.888 |                 0.888 | NeuralNetFastAI |                 0.819 |              0.812 |                0.069 |
| WUE        | M3_24m     |                0.892 |           0.89  |                 0.892 | NeuralNetTorch  |                 0.819 |              0.794 |                0.07  |
| WUE        | M4_12m     |                0.868 |           0.859 |                 0.852 | LightGBMXT      |                 0.821 |              0.789 |                0.038 |
| WUE        | M4_24m     |                0.855 |           0.857 |                 0.859 | NeuralNetFastAI |                 0.76  |              0.733 |                0.095 |
