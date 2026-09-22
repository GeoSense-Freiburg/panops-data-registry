#!/usr/bin/env Rscript
# ============================================================================
# M3 (climate+traits) vs M4 (climate+traits+disturbance) for the growing-
# season accumulated flux totals built by extract_yearly_flux_totals.R -
# a circularity-free alternative to the GPPsat/NEPmax/ETmax/WUE EFPs used
# elsewhere in this project (see daily_reports/2026-09-17-efp-calculation-
# and-circularity.md for why those EFPs bake in some of the same climate
# variables used as predictors; a plain accumulated sum doesn't).
#
# Predictors (per the user's scope): climate lag1 AND lag2 (the 24m-window
# convention), plant traits, and mortality/disturbance metrics - sourced
# from the EXISTING, already-lag2-corrected 24m harmonized file
# (derived_tables/outputs_afterEGU_results/v10_lag2/v10_lag2_B2_GPPsat_
# harmonized.csv - any one response's B2 file works, since the predictor
# columns/values are identical across them; GPPsat's is used only to pull
# SITE_ID/YEAR + predictors, its own response column is discarded). Same
# tc>=30, 93-site scope throughout - NOT re-derived, just read from there.
#
# Response: GPP_sum_gC, RECO_sum_gC, NEP_sum_gC, ET_sum_mm, WUE_annual from
# predict_fluxes/derived_tables/yearly_flux_totals_raw.csv (status=="ok"
# rows only). Merging the 395-row predictor table with the 557 "ok" flux
# rows on SITE_ID+YEAR gives 378 site-years across all 93 sites (95.7% of
# the main predictor rows retained).
#
# Hyperparameters: GPPsat's XGB-Optuna tuned config, reused unchanged as a
# first pass for every new response (their scale/distribution differs a lot
# from GPPsat's own - a fresh light retune would likely help if this
# direction is pursued further; flagged, not yet done).
#
# Independent of the main project: everything reads/writes only within
# predict_fluxes/ except for the one predictor-source file above (read-only).
#
#   Rscript model_M3_M4_flux_totals.R [n_perm]   (default 0, no permutation)
# ============================================================================
suppressMessages({library(data.table); library(xgboost); library(parallel)})
setwd("/mnt/gsdata/projects/panops/panops-data-registry/data/flux")
OUT <- "predict_fluxes/plots"; dir.create(OUT, showWarnings = FALSE, recursive = TRUE)
TABLE_OUT <- "predict_fluxes/derived_tables"

args <- commandArgs(trailingOnly = TRUE)
N_PERM <- if (length(args) >= 1) as.integer(args[1]) else 0
SEED <- 42
N_CORES <- as.integer(Sys.getenv("V10_CORES", "60"))
RESPONSE_VARS <- c("GPP_sum_gC", "RECO_sum_gC", "NEP_sum_gC", "ET_sum_mm", "WUE_annual")

OPTUNA_GPPSAT <- fread("plots/V10/Optuna/optuna_best_configs.csv")[learner == "XGB" & response == "GPPsat"]
cat("Reusing GPPsat's XGB-Optuna config for every response (first pass):\n"); print(OPTUNA_GPPSAT)

pred <- fread("derived_tables/outputs_afterEGU_results/v10_lag2/v10_lag2_B2_GPPsat_harmonized.csv")
flux <- fread(file.path(TABLE_OUT, "yearly_flux_totals_raw.csv"))[status == "ok"]
dt_all <- merge(pred[, -c("GPPsat","NEPmax","ETmax","WUE","uWUE",
                          grep("_anom", names(pred), value = TRUE)), with = FALSE],
                flux[, c("SITE_ID","YEAR", RESPONSE_VARS), with = FALSE], by = c("SITE_ID","YEAR"))
cat(sprintf("\nMerged predictor+response table: %d site-years, %d sites\n", nrow(dt_all), uniqueN(dt_all$SITE_ID)))

get_cols <- function(df, spec) {
  ac <- colnames(df)
  climate <- grep('^(lag1_|lag2_)', ac, value = TRUE)  # 24m-window convention, per user's request
  traits  <- grep('^(P12_|P50_|P88_|gsmax_|rdmax_|SSD|SLA|Leaf|Stem)', ac, value = TRUE)
  dist    <- grep('^(absolute_|relative_|new_|mortality_|disturbance_)', ac, value = TRUE)
  p <- c()
  if (grepl('C', spec)) p <- c(p, climate)
  if (grepl('T', spec)) p <- c(p, traits)
  if (grepl('D', spec)) p <- c(p, dist)
  unique(p[!is.na(p)])
}

fit_predict <- function(tr, te, xv, resp, o) {
  pars <- list(objective = "reg:squarederror", nthread = 1, seed = SEED,
               learning_rate = o$learning_rate, max_depth = as.integer(o$max_depth),
               min_child_weight = o$min_child_weight, subsample = o$subsample,
               colsample_bytree = o$colsample_bytree, reg_lambda = o$reg_lambda)
  bst <- tryCatch(xgb.train(params = pars, data = xgb.DMatrix(as.matrix(tr[, ..xv]), label = tr[[resp]]),
                            nrounds = as.integer(o$nrounds), verbose = 0), error = function(e) NULL)
  if (is.null(bst)) return(NULL)
  as.numeric(predict(bst, xgb.DMatrix(as.matrix(te[, ..xv]))))
}

fit_loso <- function(dt, xv, resp, o) {
  sites <- sort(unique(dt$SITE_ID))
  fold <- rbindlist(mclapply(sites, function(ts) {
    tr <- dt[SITE_ID != ts]; te <- dt[SITE_ID == ts]
    pr <- fit_predict(tr, te, xv, resp, o)
    if (is.null(pr)) return(NULL)
    data.table(observed = te[[resp]], predicted = pr)
  }, mc.cores = N_CORES), fill = TRUE)
  c(rmse = sqrt(mean((fold$predicted - fold$observed)^2)),
    r2 = 1 - sum((fold$observed - fold$predicted)^2) / sum((fold$observed - mean(fold$observed))^2))
}

results <- list()
for (resp in RESPONSE_VARS) {
  o <- OPTUNA_GPPSAT
  xv3 <- get_cols(dt_all, "C+T")
  xv4 <- get_cols(dt_all, "C+T+D")
  dist_cols <- setdiff(xv4, xv3)

  dt <- dt_all[, unique(c("SITE_ID","YEAR", resp, xv4)), with = FALSE]
  dt <- dt[complete.cases(dt)]
  cat(sprintf("\n=== %s : n=%d site-years, %d sites | M3 p=%d | M4 p=%d ===\n",
              resp, nrow(dt), uniqueN(dt$SITE_ID), length(xv3), length(xv4)))

  m3 <- fit_loso(dt, xv3, resp, o)
  m4 <- fit_loso(dt, xv4, resp, o)
  delta <- 100 * (m4[["rmse"]] - m3[["rmse"]]) / m3[["rmse"]]
  cat(sprintf("  M3 (C+T)   RMSE=%.4f R2=%.3f\n", m3[["rmse"]], m3[["r2"]]))
  cat(sprintf("  M4 (C+T+D) RMSE=%.4f R2=%.3f  (dRMSE=%+.2f%%)\n", m4[["rmse"]], m4[["r2"]], delta))

  p_val <- NA_real_; perm_mean <- NA_real_
  if (N_PERM > 0) {
    rp <- numeric(N_PERM)
    for (r in seq_len(N_PERM)) {
      set.seed(SEED + r * 7919L)
      dp <- copy(dt); pi <- sample(nrow(dp)); dc <- dist_cols
      dp[, (dc) := dp[pi, ..dc]]
      rp[r] <- fit_loso(dp, xv4, resp, o)[["rmse"]]
    }
    p_val <- (sum(rp <= m4[["rmse"]]) + 1) / (N_PERM + 1); perm_mean <- mean(rp)
    cat(sprintf("  permutation (n=%d): null mean RMSE=%.4f, p=%.4f\n", N_PERM, perm_mean, p_val))
  }
  results[[resp]] <- data.table(response = resp, n = nrow(dt), n_sites = uniqueN(dt$SITE_ID),
    n_predictors_M3 = length(xv3), n_predictors_M4 = length(xv4),
    rmse_M3 = m3[["rmse"]], r2_M3 = m3[["r2"]], rmse_M4 = m4[["rmse"]], r2_M4 = m4[["r2"]],
    delta_rmse_pct = delta, n_perm = N_PERM, rmse_perm_mean = perm_mean, p_perm = p_val)
}
out <- rbindlist(results)
fwrite(out, file.path(TABLE_OUT, sprintf("model_M3_M4_flux_totals%s.csv", if (N_PERM>0) sprintf("_perm%d",N_PERM) else "")))
cat("\n=== SUMMARY ===\n")
print(out[, .(response, n, n_sites, rmse_M3=round(rmse_M3,2), r2_M3=round(r2_M3,3),
              rmse_M4=round(rmse_M4,2), r2_M4=round(r2_M4,3), delta_rmse_pct=round(delta_rmse_pct,2), p_perm)])
cat("\nMODEL_M3_M4_FLUX_TOTALS_DONE\n")
