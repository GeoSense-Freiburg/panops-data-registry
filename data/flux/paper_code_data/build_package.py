#!/usr/bin/env python
"""
Assemble the code-and-data package for the paper (Nature Ecology & Evolution submission) from the project.

Copies the scripts that produce the final results (V5 maps, mortality block without thresholded metrics, AutoGluon as
main learner) into code/, the analysis-ready data and model outputs into data/, writes data/site_metadata.csv with the
FLUXNET per-site citations, and builds one Excel source-data file per main display item (make_source_data.R).
Re-run after any change of results:
    /home/nk1125/miniconda3/envs/autogluon_env/bin/python paper_code_data/build_package.py
The package is meant for a Zenodo deposit (DOI for the Code/Data availability statements); see README.md.
"""
import os, re, shutil, subprocess, sys
import pandas as pd

ROOT = "/mnt/gsdata/projects/panops/panops-data-registry/data/flux"
PKG = os.path.join(ROOT, "paper_code_data")
RSCRIPT = "/home/nk1125/miniconda3/envs/clean_r_env/bin/Rscript"

# (source relative to ROOT, destination relative to PKG)
CODE = [
    # 1 flux data and ecosystem functional properties
    ("notebooks/12_FLUXNET_shuttle.ipynb", "code/01_flux_data/12_FLUXNET_shuttle.ipynb"),
    ("notebooks/14_calc_EFPs.ipynb", "code/01_flux_data/14_calc_EFPs.ipynb"),
    ("scripts/paper_v41/02_nepmax90_gs_monthly_meteo.R", "code/01_flux_data/02_nepmax90_gs_monthly_meteo.R"),
    # 2 tree mortality metrics
    ("scripts/paper_v41/01_mortality_metrics_v41.py", "code/02_mortality/01_mortality_metrics.py"),
    # 3 dataset assembly (climate, mortality, traits, site selection)
    ("scripts/paper_v41/paper_common.R", "code/03_dataset/paper_common.R"),
    ("scripts/paper_v41/03_build_paper_dataset.R", "code/03_dataset/03_build_paper_dataset.R"),
    ("scripts/paper_v41/04_traits_lusk_1km.R", "code/03_dataset/04_traits_lusk_1km.R"),
    # 4 AutoGluon (main learner)
    ("AutoGluon/ag_common.py", "code/04_models_autogluon/ag_common.py"),
    ("AutoGluon/01_fit_full.py", "code/04_models_autogluon/01_fit_full.py"),
    ("AutoGluon/02_loso_repcv.py", "code/04_models_autogluon/02_loso_repcv.py"),
    ("AutoGluon/03_shap.py", "code/04_models_autogluon/03_shap.py"),
    ("AutoGluon/run_efp.sh", "code/04_models_autogluon/run_efp.sh"),
    ("AutoGluon/run_efpclim.sh", "code/04_models_autogluon/run_efpclim.sh"),
    # 5 XGBoost / LightGBM (checks)
    ("scripts/optuna_tune.py", "code/05_models_xgb_lgb/optuna_tune.py"),
    ("scripts/paper_v41/10_optuna_objective.R", "code/05_models_xgb_lgb/10_optuna_objective.R"),
    ("scripts/paper_v41/13_collect_optuna.py", "code/05_models_xgb_lgb/13_collect_optuna.py"),
    ("scripts/paper_v41/11_train_repcv.R", "code/05_models_xgb_lgb/11_train_repcv.R"),
    ("scripts/paper_v41/12_shap_M4.R", "code/05_models_xgb_lgb/12_shap_M4.R"),
    ("scripts/paper_v41/run_variant.sh", "code/05_models_xgb_lgb/run_variant.sh"),
    # 6 statistics
    ("scripts/paper_v41/39_paired_bootstrap.R", "code/06_statistics/39_paired_bootstrap.R"),
    ("scripts/paper_v41/40_table_bootstrap.R", "code/06_statistics/40_table_bootstrap.R"),
    ("scripts/paper_v41/31_table_fig2_summary.R", "code/06_statistics/31_table_site_level_tests.R"),
    ("scripts/paper_v41/41_mortality_dose_response.R", "code/06_statistics/41_mortality_dose_response.R"),
    ("scripts/paper_v41/42_dose_response_robustness.R", "code/06_statistics/42_dose_response_robustness.R"),
    ("scripts/paper_v41/37_mortality_dropcol.R", "code/06_statistics/37_mortality_dropcol.R"),
    ("scripts/paper_v41/38_mortality_block_summary.R", "code/06_statistics/38_mortality_block_summary.R"),
    # 7 figures
    ("scripts/paper_v41/20_fig1_site_map.R", "code/07_figures/20_fig1_site_map.R"),
    ("scripts/paper_v41/32_fig3_landscape.R", "code/07_figures/32_signed_shap_per_site.R"),
    ("scripts/paper_v41/23_fig4_signed_shap.R", "code/07_figures/23_high_vs_lowmid.R"),
    ("scripts/paper_v41/24_fig5_igbp_board.R", "code/07_figures/24_igbp_board_and_mortality_vs_direction.R"),
    ("scripts/paper_v41/43_residual_by_mortality_bin.R", "code/07_figures/43_residual_by_mortality_class.R"),
    ("scripts/paper_v41/21_fig2_M3M4.R", "code/07_figures/21_per_site_error.R"),
    ("scripts/paper_v41/29_site_delta_rmse.R", "code/07_figures/29_site_delta_rmse.R"),
    ("scripts/paper_v41/25_fig3b_site_signed_shap.R", "code/07_figures/25_signed_shap_per_site_portrait.R"),
    ("scripts/paper_v41/26_D_block_composition.R", "code/07_figures/26_mortality_block_by_metric.R"),
    ("AutoGluon/05_figures.sh", "code/07_figures/05_figures_autogluon.sh"),
    ("AutoGluon/05b_figures_ag_only.sh", "code/07_figures/05b_figures_autogluon_only.sh"),
]

DATA = [
    ("derived_tables/paper_v5/paper_dataset_full_lags_monthly_tc30_minrun2_buf100-200-300-400-500.csv",
     "data/analysis_dataset_site_years.csv"),
    ("AutoGluon/outputs/efp_v5_nothresh/AG_predictions_repCV.csv", "data/model_outputs/autogluon_predictions.csv"),
    ("AutoGluon/outputs/efp_v5_nothresh/AG_metrics_repCV.csv", "data/model_outputs/autogluon_metrics.csv"),
    ("AutoGluon/outputs/efp_v5_nothresh/AG_site_signed_shap_M4.csv", "data/model_outputs/autogluon_signed_shap_groups_per_site.csv"),
    ("AutoGluon/outputs/efp_v5_nothresh/AG_site_shap_M4.csv", "data/model_outputs/autogluon_shap_per_variable_per_site.csv"),
    ("AutoGluon/outputs/efp_v5_nothresh_efpclim/AG_predictions_repCV.csv", "data/model_outputs/autogluon_predictions_efp_specific_climate.csv"),
    ("derived_tables/paper_v5_nothresh/models/XGB_predictions_repCV.csv", "data/model_outputs/xgboost_predictions.csv"),
    ("derived_tables/paper_v5_nothresh/models/LGB_predictions_repCV.csv", "data/model_outputs/lightgbm_predictions.csv"),
    ("derived_tables/paper_v5_nothresh/models/XGB_metrics_repCV.csv", "data/model_outputs/xgboost_metrics.csv"),
    ("derived_tables/paper_v5_nothresh/models/LGB_metrics_repCV.csv", "data/model_outputs/lightgbm_metrics.csv"),
    ("derived_tables/paper_v5_nothresh/models/XGB_site_signed_shap_M4.csv", "data/model_outputs/xgboost_signed_shap_groups_per_site.csv"),
    ("derived_tables/paper_v5_nothresh/models/LGB_site_signed_shap_M4.csv", "data/model_outputs/lightgbm_signed_shap_groups_per_site.csv"),
    ("plots/paper_v5_nothresh/paired_bootstrap_statistics.csv", "data/statistics/paired_bootstrap_statistics.csv"),
    ("plots/paper_v5_nothresh/paired_bootstrap_statistics_efp_v5_nothresh_efpclim.csv", "data/statistics/paired_bootstrap_statistics_efp_specific_climate.csv"),
    ("plots/paper_v5_nothresh/table_fig2_dMAE_total_mort_permlog_statistics.csv", "data/statistics/site_level_logratio_permutation_MAE.csv"),
    ("plots/paper_v5_nothresh/dose_response/residual_by_mortality_AG_24m.csv", "data/statistics/residual_by_mortality_class_autogluon_24m.csv"),
    ("plots/paper_v5_nothresh/dose_response/residual_correction_by_mortality_AG_24m.csv", "data/statistics/residual_correction_by_mortality_class_autogluon_24m.csv"),
    ("plots/paper_v5_nothresh/dose_response/residual_correction_trend_AG_24m.csv", "data/statistics/residual_correction_trend_autogluon_24m.csv"),
    ("plots/paper_v5_nothresh/dose_response/dose_response_robustness_AG.csv", "data/statistics/dose_response_robustness_autogluon.csv"),
    ("plots/paper_v5_nothresh/mortality_split/mortality_block_dropcol.csv", "data/statistics/mortality_block_dropcol.csv"),
]


def copy(pairs):
    missing = []
    for src, dst in pairs:
        s, d = os.path.join(ROOT, src), os.path.join(PKG, dst)
        if not os.path.exists(s):
            missing.append(src); continue
        os.makedirs(os.path.dirname(d), exist_ok=True)
        shutil.copyfile(s, d)
    return missing


def site_metadata():
    ds = pd.read_csv(os.path.join(PKG, "data/analysis_dataset_site_years.csv"), usecols=["SITE_ID", "YEAR"])
    n = ds.groupby("SITE_ID").agg(n_site_years=("YEAR", "size"), first_year=("YEAR", "min"), last_year=("YEAR", "max")).reset_index()
    snap = pd.read_csv(os.path.join(ROOT, "fluxnet_2017_2025_V02/fluxnet_shuttle_snapshot_20260512T115423.csv"))
    snap = snap.drop_duplicates("site_id")[["site_id", "site_name", "location_lat", "location_long", "igbp",
                                            "product_source_network", "oneflux_code_version", "product_citation"]]
    m = n.merge(snap, left_on="SITE_ID", right_on="site_id", how="left").drop(columns="site_id")
    m.to_csv(os.path.join(PKG, "data/site_metadata.csv"), index=False)
    return len(m), int(m.product_citation.isna().sum())


def main():
    for sub in ("code", "data", "source_data", "environment"):
        shutil.rmtree(os.path.join(PKG, sub), ignore_errors=True)
    miss = copy(CODE) + copy(DATA)
    n, nocite = site_metadata()
    env_dir = os.path.join(PKG, "environment"); os.makedirs(env_dir, exist_ok=True)
    with open(os.path.join(env_dir, "python_autogluon_env.txt"), "w") as fh:   # exact Python package versions
        subprocess.run(["/home/nk1125/miniconda3/envs/autogluon_env/bin/python", "-m", "pip", "freeze"], stdout=fh, check=True)
    subprocess.run([RSCRIPT, "-e", "pk <- c('data.table','xgboost','lightgbm','nlme','ggplot2','ggh4x','patchwork','terra','sf','bigleaf','writexl','scales');"
                    "v <- sapply(pk, function(p) tryCatch(as.character(packageVersion(p)), error = function(e) NA));"
                    "writeLines(c(R.version.string, paste(pk, v)), file.path('" + env_dir + "', 'R_packages.txt'))"], check=True)
    r = subprocess.run([RSCRIPT, os.path.join(PKG, "make_source_data.R")], capture_output=True, text=True)
    print(r.stdout[-2000:], r.stderr[-2000:] if r.returncode else "")
    print(f"code files: {len(CODE)}, data files: {len(DATA)}, sites: {n} (without citation: {nocite})")
    if miss:
        print("MISSING:", *miss, sep="\n  "); sys.exit(1)


if __name__ == "__main__":
    main()
