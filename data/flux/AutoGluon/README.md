# AutoGluon test on the paper_v41 datasets

Question: instead of choosing between XGBoost, LightGBM and RF by hand, let an AutoML system
(AutoGluon-Tabular 1.6.3) choose the model for each task. Everything except the learner is as in
`scripts/paper_v41/`: same datasets, predictors, windows, models and cross-validation.

## What is identical to the paper runs

| | |
|---|---|
| Data | v4.1 EFP set (106 sites / 388 site-years: GPPsat, NEPmax, ETmax, WUE) and v4.1 flux set (102 / 362: GPP_gs, NEP_gs, ET_gs, WUE_gs) - the same CSVs `paper_common.R` reads |
| Predictors | `paper_cols()` ported line by line (`ag_common.py`): 143 / 213 / 263 / 368 predictors |
| Models | M3 = C+T, M4 = C+T+D only |
| Windows | 12m and 24m |
| Evaluation | repeated leave-one-site-out: 3 reps x 80 % of the training sites per held-out site, predictions averaged (`11_train_repcv.R`) |
| Model selection | 5-fold CV with site-disjoint folds, seed 42 (as the Optuna objective) |
| SHAP | M4, model fit on all site-years, SHAP per site-year, averaged per site (`12_shap_M4.R`) |

## What necessarily differs

- **Learner.** Per fit AutoGluon trains LightGBM (3 variants), CatBoost, XGBoost, random forest,
  extra trees and two neural networks (PyTorch MLP, FastAI), each as a 5-fold bag over site-disjoint
  folds, then builds a weighted ensemble on the out-of-site predictions. Default (`medium_quality`)
  model zoo, no stacking, no time limit, CPU only. The larger `best_quality` portfolio was not used:
  about 100 configurations per fit is not affordable inside 10 000 leave-one-site-out fits.
- **Selection is nested.** XGB / LGB hyperparameters were tuned once on all sites and then evaluated
  by LOSO. AutoGluon is rerun from scratch inside every LOSO fit, so the held-out site never
  influences which model or weights are chosen. This is the stricter test.
- **SHAP method.** The winning ensembles mix neural networks and trees, so TreeSHAP is not available.
  SHAP comes from `shap.PermutationExplainer` (10 antithetic permutations per site-year, 100 background
  site-years, seed 42). Values are exactly additive to the ensemble prediction, but the baseline
  differs from TreeSHAP, so compare group shares and signs with XGB / LGB rather than raw magnitudes.
- **Random numbers.** numpy, not R, so the 80 % site subsamples are drawn by the same rule but are
  not the identical draws.

## Scripts

| Script | Does | Runtime |
|---|---|---|
| `ag_common.py` | datasets, predictor blocks, site folds, the AutoGluon fit call | - |
| `01_fit_full.py <set>` | one fit per task on all site-years; grouped-CV leaderboard; keeps the predictors | ~4 min per set |
| `02_loso_repcv.py <set>` | repeated LOSO, ensemble and every base learner | several hours per set |
| `03_shap.py <set>` | permutation SHAP for M4 | several hours per set |
| `04_compare.py <set>` | tables against XGB / LGB -> `RESULTS_<set>.md` | seconds |
| `run_loso.sh`, `run_shap.sh` | both sets in sequence, for tmux; status in `logs/STATUS_*.txt` | |

`<set>` = `efp` or `flux`. Python: `/home/nk1125/miniconda3/envs/autogluon_env/bin/python`
(separate env; the R env is untouched). Steps 2 and 3 cache every site in `cache/` and resume.

## Outputs (`outputs/<set>/`)

Same column layout as the XGB / LGB files in `derived_tables/paper_v41*/models/`, prefix `AG`:

- `AG_predictions_repCV.csv`, `AG_metrics_repCV.csv` - the AutoGluon ensemble
- `AG_predictions_repCV_by_model.csv`, `AG_metrics_repCV_by_model.csv` - every base learner, same folds
- `AG_site_shap_M4.csv`, `AG_site_signed_shap_M4.csv` - per-site SHAP (variable level, group level)
- `shap_values/M4_<window>_<response>.csv.gz` - SHAP per site-year
- `AG_leaderboard_groupCV.csv`, `AG_best_groupCV.csv` - step 1 leaderboard and ensemble weights
- `comparison_*.csv`, `ensemble_weights_loso.csv` - step 4

`models/` (fitted predictors, ~2 GB per set) and `cache/` are not tracked by git.
