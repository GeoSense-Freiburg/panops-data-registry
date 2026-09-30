#!/usr/bin/env Rscript
# ============================================================================
# Inputs for drawing the paper_v41 figures from the existing V10 EFP models (PAPER_RESP_SET=v10efp):
# V10 tree cover >= 30 %, 93 sites / 395 site-years, deadtrees v2-2, XGBoost and LightGBM Optuna,
# lag2-corrected 24m models (true_24m). No refitting - SHAP was computed by the V10 runs.
#   derived_tables/paper_v10_efp/paper_dataset_v10_efp.csv   site-years with IGBP, EFPs, mortality metrics
#   derived_tables/paper_v10_efp/models/<L>_site_signed_shap_M4.csv   copied from <L>_v10_true24m_optuna/
# Then e.g.  PAPER_RESP_SET=v10efp Rscript scripts/paper_v41/25_fig3b_site_signed_shap.R XGB
# ============================================================================
suppressMessages(library(data.table))
setwd("/mnt/gsdata/projects/panops/panops-data-registry/data/flux")
B <- "derived_tables/outputs_afterEGU_results"
OUT <- "derived_tables/paper_v10_efp"; dir.create(file.path(OUT, "models"), showWarnings = FALSE, recursive = TRUE)

d <- fread(file.path(B, "v10_lag2/v10_lag2_B2_GPPsat_harmonized.csv"),
           select = c("SITE_ID", "YEAR", "GPPsat", "NEPmax", "ETmax", "WUE", "uWUE",
                      "absolute_mortality_500m", "relative_mortality_500m", "relative_disturbance_500m"))
meta <- fread(file.path(B, "EFP_mortality_trait_hydro_combined_with_meteo_dist_lags.csv"),
              select = c("SITE_ID", "IGBP", "LOCATION_LAT", "LOCATION_LONG", "forest_mean_pct_500m"))
meta <- meta[, .(IGBP = IGBP[1], LOCATION_LAT = LOCATION_LAT[1], LOCATION_LONG = LOCATION_LONG[1],
                 tc_site_mean_500m = mean(forest_mean_pct_500m, na.rm = TRUE)), by = SITE_ID]
d <- merge(d, meta, by = "SITE_ID", all.x = TRUE)
stopifnot(!anyNA(d$IGBP), uniqueN(d$SITE_ID) == 93, nrow(d) == 395)
fwrite(d, file.path(OUT, "paper_dataset_v10_efp.csv"))

for (L in c("XGB", "LGB")) {
  s <- fread(file.path(B, sprintf("%s_v10_true24m_optuna/%s_site_signed_shap_M4.csv", L, L)))
  stopifnot(uniqueN(s$test_site) == 93, all(c("M4_12m", "M4_24m") %in% s$model))
  fwrite(s, file.path(OUT, "models", sprintf("%s_site_signed_shap_M4.csv", L)))
}
cat(sprintf("written %s: %d site-years, %d sites; signed SHAP for XGB and LGB\n", OUT, nrow(d), uniqueN(d$SITE_ID)))
