#!/usr/bin/env Rscript
# ============================================================================
# Circularity check (FORTRACK 2026-09-14, Miguel, action item #1 + #4).
# Several EFPs are derived from the same climate quantities used as predictors
# (see daily_reports/2026-09-17-efp-calculation-and-circularity.md). This test
# drops those quantities from the climate block and asks whether the
# disturbance gain (M3 -> M4) survives.
#
# Scenarios (12m window, tc>=30, 93 sites / 395 site-years, LOSO CV, Optuna
# hyperparameters reused unchanged from the main models):
#   baseline : all climate (P, SW_IN, TA, VPD)                    [reference]
#   own_vars : drop the climate variables embedded in that EFP's own derivation
#                GPPsat -> SW_IN + TA      NEPmax -> SW_IN
#                ETmax  -> TA              WUE    -> TA + VPD
#   precip   : drop TA, VPD and SW_IN for every EFP (only P remains) - strictest
#
#   Rscript circularity_test_climate_exclusion.R [XGB|LGB] [n_perm] [scenarios]
#   n_perm > 0 additionally runs the block-permutation null (see
#   permutation_test_disturbance.R) for every scenario x EFP; default 0.
#   scenarios: comma-separated subset of baseline,own_vars,precip (default all).
# ============================================================================
suppressMessages({library(data.table); library(parallel)})
setwd("/mnt/gsdata/projects/panops/panops-data-registry/data/flux")

args <- commandArgs(trailingOnly = TRUE)
LEARNER <- if (length(args) >= 1) args[1] else "XGB"
N_PERM  <- if (length(args) >= 2) as.integer(args[2]) else 0
stopifnot(LEARNER %in% c("XGB", "LGB"))
if (LEARNER == "XGB") suppressMessages(library(xgboost)) else suppressMessages(library(lightgbm))

SEED <- 42
N_CORES <- as.integer(Sys.getenv("V10_CORES", "60"))
B <- "derived_tables/outputs_afterEGU_results"
OUT <- "plots/V10/circularity_test"; dir.create(OUT, showWarnings = FALSE, recursive = TRUE)
RESPONSE_VARS <- c("GPPsat", "NEPmax", "ETmax", "WUE")

OWN_VARS <- list(GPPsat = c("SW_IN", "TA"), NEPmax = "SW_IN", ETmax = "TA", WUE = c("TA", "VPD"))
scenario_drop <- function(scn, resp) switch(scn,
  baseline = character(0), own_vars = OWN_VARS[[resp]], precip = c("TA", "VPD", "SW_IN"))
ALL_SCENARIOS <- c("baseline", "own_vars", "precip")
SCENARIOS <- if (length(args) >= 3) strsplit(args[3], ",")[[1]] else ALL_SCENARIOS
stopifnot(all(SCENARIOS %in% ALL_SCENARIOS))

OPTUNA <- local({
  cfg <- fread("plots/V10/Optuna/optuna_best_configs.csv")[learner == LEARNER]
  setNames(split(cfg, cfg$response), cfg$response)
})

get_cols <- function(df, spec, drop_climate_vars = character(0)) {
  ac <- colnames(df)
  climate <- grep('^lag1_', ac, value = TRUE)
  for (v in drop_climate_vars) climate <- climate[!grepl(sprintf("_%s_", v), climate, fixed = TRUE)]
  traits <- grep('^(P12_|P50_|P88_|gsmax_|rdmax_|SSD|SLA|Leaf|Stem)', ac, value = TRUE)
  dist   <- grep('^(absolute_|relative_|new_|mortality_|disturbance_)', ac, value = TRUE)
  p <- c()
  if (grepl('C', spec)) p <- c(p, climate)
  if (grepl('T', spec)) p <- c(p, traits)
  if (grepl('D', spec)) p <- c(p, dist)
  unique(p[!is.na(p)])
}
safe <- function(x) make.unique(gsub("[^A-Za-z0-9_]", "_", x), sep = "_")

fit_predict <- function(tr, te, xv, resp, o) {
  if (LEARNER == "XGB") {
    pars <- list(objective = "reg:squarederror", nthread = 1, seed = SEED,
                 learning_rate = o$learning_rate, max_depth = as.integer(o$max_depth),
                 min_child_weight = o$min_child_weight, subsample = o$subsample,
                 colsample_bytree = o$colsample_bytree, reg_lambda = o$reg_lambda)
    bst <- tryCatch(xgb.train(params = pars, data = xgb.DMatrix(as.matrix(tr[, ..xv]), label = tr[[resp]]),
                              nrounds = as.integer(o$nrounds), verbose = 0), error = function(e) NULL)
    if (is.null(bst)) return(NULL)
    return(as.numeric(predict(bst, xgb.DMatrix(as.matrix(te[, ..xv])))))
  }
  pars <- list(objective = "regression", metric = "rmse", num_threads = 1, seed = SEED, verbosity = -1,
               learning_rate = o$learning_rate, num_leaves = as.integer(o$num_leaves),
               min_data_in_leaf = as.integer(o$min_data_in_leaf), feature_fraction = o$feature_fraction,
               bagging_fraction = o$bagging_fraction, bagging_freq = 1, lambda_l2 = o$lambda_l2)
  sv <- safe(xv)
  Xtr <- as.matrix(tr[, ..xv]); colnames(Xtr) <- sv
  Xte <- as.matrix(te[, ..xv]); colnames(Xte) <- sv
  bst <- tryCatch(lgb.train(params = pars, data = lgb.Dataset(Xtr, label = tr[[resp]]),
                            nrounds = as.integer(o$nrounds), verbose = -1), error = function(e) NULL)
  if (is.null(bst)) return(NULL)
  as.numeric(predict(bst, Xte))
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
  b1 <- as.data.frame(fread(sprintf("%s/v10/v10_B1_%s_harmonized.csv", B, resp)))
  o <- OPTUNA[[resp]]
  # one shared row set for every scenario so scenarios differ ONLY in predictors
  all_cols <- get_cols(b1, "C+T+D")
  dt <- as.data.table(b1[, unique(c("SITE_ID", "YEAR", resp, all_cols))])
  dt <- dt[complete.cases(dt)]
  cat(sprintf("\n=== %s (%s, LOSO) : n=%d site-years, %d sites ===\n", resp, LEARNER, nrow(dt), uniqueN(dt$SITE_ID)))

  for (scn in SCENARIOS) {
    drop_v <- scenario_drop(scn, resp)
    xv3 <- get_cols(b1, "C+T",   drop_v)
    xv4 <- get_cols(b1, "C+T+D", drop_v)
    dist_cols <- setdiff(xv4, xv3)
    m3 <- fit_loso(dt, xv3, resp, o)
    m4 <- fit_loso(dt, xv4, resp, o)
    delta <- 100 * (m4[["rmse"]] - m3[["rmse"]]) / m3[["rmse"]]
    cat(sprintf("  %-9s drop=[%-12s] M3 p=%3d RMSE=%.4f R2=%.3f | M4 p=%3d RMSE=%.4f R2=%.3f | dRMSE=%+.2f%%\n",
                scn, paste(drop_v, collapse = "+"), length(xv3), m3[["rmse"]], m3[["r2"]],
                length(xv4), m4[["rmse"]], m4[["r2"]], delta))

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
      cat(sprintf("            permutation (n=%d): null mean RMSE=%.4f, p=%.4f\n", N_PERM, perm_mean, p_val))
    }
    results[[paste(resp, scn)]] <- data.table(
      response = resp, learner = LEARNER, scenario = scn, dropped = paste(drop_v, collapse = "+"),
      n = nrow(dt), n_climate = length(setdiff(xv3, get_cols(b1, "T"))),
      rmse_M3 = m3[["rmse"]], r2_M3 = m3[["r2"]], rmse_M4 = m4[["rmse"]], r2_M4 = m4[["r2"]],
      delta_rmse_pct = delta, n_perm = N_PERM, rmse_perm_mean = perm_mean, p_perm = p_val)
  }
}
out <- rbindlist(results)
fn <- sprintf("circularity_test_%s_LOSO%s%s.csv", LEARNER, if (N_PERM > 0) sprintf("_perm%d", N_PERM) else "",
              if (!setequal(SCENARIOS, ALL_SCENARIOS)) paste0("_", paste(SCENARIOS, collapse = "-")) else "")
fwrite(out, file.path(OUT, fn))
cat("\n=== SUMMARY: disturbance gain (M3 -> M4 dRMSE%) by scenario ===\n")
print(out[, .(response, scenario, delta_rmse_pct = round(delta_rmse_pct, 2), p_perm)])
cat("\nCIRCULARITY_TEST_DONE\n")
