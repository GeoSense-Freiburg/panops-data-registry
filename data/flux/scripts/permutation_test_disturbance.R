#!/usr/bin/env Rscript
# ============================================================================
# Permutation test for the mortality/disturbance predictor block, addressing
# Rene's question at the 2026-09-14 FORTRACK meeting: does adding D improve
# RMSE because it carries real information, or simply because M4 has ~100
# more columns than M3 (more flexibility to fit noise)?
#
# Design: block-permute the D block's ROWS across site-years (shuffle which
# site-year's 100-column disturbance vector is attached to which site-year's
# y/climate/traits), keeping the D block's own internal correlation structure
# and marginal distribution intact - the standard permutation-importance
# recipe, scaled from one variable to the whole block. Retrain M4 on this
# permuted-D data under the IDENTICAL LOSO folds and the SAME Optuna-tuned
# hyperparameters as the real model (same code path as
# run_v10_XGB_optuna.R), repeated N_PERM times. If real M4's RMSE is not
# unusually good compared to this null distribution, the M3->M4 improvement
# is not distinguishable from adding N random columns.
#
#   Rscript permutation_test_disturbance.R [n_perm]   (default 10)
#
# Scope (first pass, per the "let's start with" framing): XGBoost-Optuna,
# tc>=30, 12m window, all 4 EFPs (GPPsat/NEPmax/WUE as the significant ones,
# ETmax as a built-in negative control since real D didn't help there either).
# ============================================================================
suppressMessages({library(data.table); library(xgboost); library(parallel)})
setwd("/mnt/gsdata/projects/panops/panops-data-registry/data/flux")

args <- commandArgs(trailingOnly = TRUE)
N_PERM <- if (length(args) >= 1) as.integer(args[1]) else 10
SEED <- 42
N_CORES <- as.integer(Sys.getenv("V10_CORES", "60"))

B <- "derived_tables/outputs_afterEGU_results"
OUT <- "plots/V10/disturbance_split"; dir.create(OUT, showWarnings = FALSE, recursive = TRUE)
RESPONSE_VARS <- c("GPPsat", "NEPmax", "ETmax", "WUE")
OUT_SUFFIX <- sprintf("_n%d", N_PERM)  # keeps every rep-count's results side by side, never overwrites

OPTUNA <- local({
  cfg <- fread("plots/V10/Optuna/optuna_best_configs.csv")[learner == "XGB"]
  setNames(split(cfg, cfg$response), cfg$response)
})

get_cols <- function(df, spec) {
  ac <- colnames(df)
  climate <- grep('^lag1_', ac, value = TRUE)  # 12m window only
  traits  <- grep('^(P12_|P50_|P88_|gsmax_|rdmax_|SSD|SLA|Leaf|Stem)', ac, value = TRUE)
  dist    <- grep('^(absolute_|relative_|new_|mortality_|disturbance_)', ac, value = TRUE)
  p <- c()
  if (grepl('C', spec)) p <- c(p, climate)
  if (grepl('T', spec)) p <- c(p, traits)
  if (grepl('D', spec)) p <- c(p, dist)
  unique(p[!is.na(p)])
}

fit_loso_rmse <- function(dt, xv, resp, o) {
  sites <- sort(unique(dt$SITE_ID))
  pars <- list(objective = "reg:squarederror", nthread = 1, seed = SEED,
               learning_rate = o$learning_rate, max_depth = as.integer(o$max_depth),
               min_child_weight = o$min_child_weight, subsample = o$subsample,
               colsample_bytree = o$colsample_bytree, reg_lambda = o$reg_lambda)
  fold <- rbindlist(mclapply(sites, function(ts) {
    tr <- dt[SITE_ID != ts]; te <- dt[SITE_ID == ts]
    Xtr <- as.matrix(tr[, ..xv]); Xte <- as.matrix(te[, ..xv])
    bst <- tryCatch(xgb.train(params = pars, data = xgb.DMatrix(Xtr, label = tr[[resp]]),
                              nrounds = as.integer(o$nrounds), verbose = 0), error = function(e) NULL)
    if (is.null(bst)) return(NULL)
    data.table(observed = te[[resp]], predicted = as.numeric(predict(bst, xgb.DMatrix(Xte))))
  }, mc.cores = N_CORES), fill = TRUE)
  sqrt(mean((fold$predicted - fold$observed)^2, na.rm = TRUE))
}

results <- list()
for (resp in RESPONSE_VARS) {
  cat(sprintf("\n=== %s ===\n", resp))
  b1 <- as.data.frame(fread(sprintf("%s/v10/v10_B1_%s_harmonized.csv", B, resp)))
  o <- OPTUNA[[resp]]

  xv3 <- get_cols(b1, "C+T")
  xv4 <- get_cols(b1, "C+T+D")
  dist_cols <- setdiff(xv4, xv3)  # the D block, isolated

  dt <- as.data.table(b1[, unique(c("SITE_ID", "YEAR", resp, xv4))])
  dt <- dt[complete.cases(dt)]
  cat(sprintf("  n=%d site-years, %d sites | M3 p=%d | M4 p=%d | D block=%d\n",
              nrow(dt), uniqueN(dt$SITE_ID), length(xv3), length(xv4), length(dist_cols)))

  rmse_m3 <- fit_loso_rmse(dt, xv3, resp, o)
  rmse_m4_real <- fit_loso_rmse(dt, xv4, resp, o)
  cat(sprintf("  M3 (C+T) RMSE       = %.4f\n", rmse_m3))
  cat(sprintf("  M4 (C+T+D) RMSE     = %.4f  (real disturbance, delta=%+.2f%%)\n",
              rmse_m4_real, 100*(rmse_m4_real-rmse_m3)/rmse_m3))

  rmse_perm <- numeric(N_PERM)
  for (r in seq_len(N_PERM)) {
    set.seed(SEED + r * 7919L)  # distinct, reproducible permutation per rep
    dt_perm <- copy(dt)
    perm_idx <- sample(nrow(dt_perm))
    dperm_cols <- dist_cols
    dt_perm[, (dperm_cols) := dt_perm[perm_idx, ..dperm_cols]]  # block-permute D's rows
    rmse_perm[r] <- fit_loso_rmse(dt_perm, xv4, resp, o)
    cat(sprintf("    perm %2d/%d: RMSE = %.4f\n", r, N_PERM, rmse_perm[r]))
  }

  p_val <- (sum(rmse_perm <= rmse_m4_real) + 1) / (N_PERM + 1)  # one-sided: real as good or better than null
  results[[resp]] <- data.table(
    response = resp, n = nrow(dt), n_predictors_M3 = length(xv3), n_predictors_M4 = length(xv4),
    rmse_M3 = rmse_m3, rmse_M4_real = rmse_m4_real,
    rmse_perm_mean = mean(rmse_perm), rmse_perm_sd = sd(rmse_perm),
    rmse_perm_min = min(rmse_perm), n_perm = N_PERM, p_perm = p_val)
  cat(sprintf("  Permuted-D RMSE: mean=%.4f sd=%.4f min=%.4f (n=%d reps)\n",
              mean(rmse_perm), sd(rmse_perm), min(rmse_perm), N_PERM))
  cat(sprintf("  Permutation p-value (real M4 <= permuted null): p=%.4f\n", p_val))
}

out <- rbindlist(results)
fwrite(out, file.path(OUT, sprintf("permutation_test_disturbance_12m%s.csv", OUT_SUFFIX)))
cat("\n\n=== SUMMARY ===\n")
print(out[, .(response, n, rmse_M3 = round(rmse_M3,3), rmse_M4_real = round(rmse_M4_real,3),
              rmse_perm_mean = round(rmse_perm_mean,3), rmse_perm_min = round(rmse_perm_min,3),
              p_perm = round(p_perm,4))])
cat("\nPERMUTATION_TEST_DONE\n")
