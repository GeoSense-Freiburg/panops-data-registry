#!/usr/bin/env Rscript
# ============================================================================
# Per-site-year out-of-fold predictions for M3 (climate+traits) and M4
# (+disturbance) on the growing-season flux totals, so the RMSE-change figure
# can be drawn the same way as V10 Fig. 2 (per-site RMSE, paired M3 vs M4).
# model_M3_M4_flux_totals.R only stored aggregate RMSE/R2; this stores the
# predictions themselves.
#
# Same data and predictor definitions as model_M3_M4_flux_totals.R:
#   12m: climate lag1 only, disturbance contemporaneous+lag1 (B1 file)
#   24m: climate lag1+lag2, disturbance +lag2 stock metrics (B2 file)
# Learners XGBoost / LightGBM with the GPPsat Optuna config reused unchanged
# for every response (first pass, as before). CV: repeated LOSO, 3 reps x 80%
# site subsample per held-out site, predictions averaged (same scheme as
# run_v10_lag2_optuna.R) - the CV scheme Fig. 2 uses.
#
#   Rscript site_predictions_flux_totals.R <XGB|LGB> [repeated|loso]
# ============================================================================
Sys.setenv(OMP_NUM_THREADS = "1", OPENBLAS_NUM_THREADS = "1")  # shared node: no hidden thread fan-out
suppressMessages({library(data.table); library(parallel)})
setDTthreads(1)
setwd("/mnt/gsdata/projects/panops/panops-data-registry/data/flux")

args <- commandArgs(trailingOnly = TRUE)
LEARNER <- if (length(args) >= 1) args[1] else "XGB"
CV_MODE <- if (length(args) >= 2) args[2] else "repeated"
stopifnot(LEARNER %in% c("XGB", "LGB"), CV_MODE %in% c("loso", "repeated"))
if (LEARNER == "XGB") suppressMessages(library(xgboost)) else suppressMessages(library(lightgbm))

SEED <- 42
N_CORES <- as.integer(Sys.getenv("V10_CORES", "40"))
N_REPS <- 3L; SUBSAMP <- 0.8
RESPONSE_VARS <- c("GPP_sum_gC", "NEP_sum_gC", "RECO_sum_gC", "ET_sum_mm", "WUE_annual")
OUT_FILE <- sprintf("predict_fluxes/derived_tables/flux_totals_predictions_%s_%s.csv", LEARNER, CV_MODE)

o <- fread("plots/V10/Optuna/optuna_best_configs.csv")[learner == LEARNER & response == "GPPsat"]
flux <- fread("predict_fluxes/derived_tables/yearly_flux_totals_raw.csv")[status == "ok"]

safe <- function(x) make.unique(gsub("[^A-Za-z0-9_]", "_", x), sep = "_")
fit_predict <- function(tr, te, xv, resp) {
  if (LEARNER == "XGB") {
    pars <- list(objective = "reg:squarederror", nthread = 1, seed = SEED,
                 learning_rate = o$learning_rate, max_depth = as.integer(o$max_depth),
                 min_child_weight = o$min_child_weight, subsample = o$subsample,
                 colsample_bytree = o$colsample_bytree, reg_lambda = o$reg_lambda)
    b <- tryCatch(xgb.train(params = pars, data = xgb.DMatrix(as.matrix(tr[, ..xv]), label = tr[[resp]]),
                            nrounds = as.integer(o$nrounds), verbose = 0), error = function(e) NULL)
    if (is.null(b)) return(NULL)
    return(as.numeric(predict(b, xgb.DMatrix(as.matrix(te[, ..xv])))))
  }
  pars <- list(objective = "regression", metric = "rmse", num_threads = 1, seed = SEED, verbosity = -1,
               learning_rate = o$learning_rate, num_leaves = as.integer(o$num_leaves),
               min_data_in_leaf = as.integer(o$min_data_in_leaf), feature_fraction = o$feature_fraction,
               bagging_fraction = o$bagging_fraction, bagging_freq = 1, lambda_l2 = o$lambda_l2)
  sv <- safe(xv); Xtr <- as.matrix(tr[, ..xv]); colnames(Xtr) <- sv
  Xte <- as.matrix(te[, ..xv]); colnames(Xte) <- sv
  b <- tryCatch(lgb.train(params = pars, data = lgb.Dataset(Xtr, label = tr[[resp]]),
                          nrounds = as.integer(o$nrounds), verbose = -1), error = function(e) NULL)
  if (is.null(b)) return(NULL)
  as.numeric(predict(b, Xte))
}

oof <- function(dt, xv, resp) {
  sites <- sort(unique(dt$SITE_ID))
  rbindlist(mclapply(sites, function(ts) {
    tr_all <- dt[SITE_ID != ts]; te <- dt[SITE_ID == ts]
    if (CV_MODE == "loso") {
      pr <- fit_predict(tr_all, te, xv, resp)
      if (is.null(pr)) return(NULL)
      return(data.table(SITE_ID = ts, YEAR = te$YEAR, observed = te[[resp]], predicted = pr))
    }
    tr_sites <- unique(tr_all$SITE_ID); k <- max(5L, floor(SUBSAMP * length(tr_sites)))
    preds <- matrix(NA_real_, nrow(te), N_REPS)
    for (r in seq_len(N_REPS)) {
      set.seed(SEED + r * 1000L + which(sites == ts))
      pr <- fit_predict(tr_all[SITE_ID %in% sample(tr_sites, k)], te, xv, resp)
      if (!is.null(pr)) preds[, r] <- pr
    }
    ok <- rowSums(!is.na(preds)) > 0
    if (!any(ok)) return(NULL)
    data.table(SITE_ID = ts, YEAR = te$YEAR, observed = te[[resp]], predicted = rowMeans(preds, na.rm = TRUE))[ok]
  }, mc.cores = N_CORES), fill = TRUE)
}

res <- list(); t0 <- Sys.time()
for (W in c("12m", "24m")) {
  pred <- fread(sprintf("derived_tables/outputs_afterEGU_results/v10_lag2/v10_lag2_%s_GPPsat_harmonized.csv",
                        if (W == "12m") "B1" else "B2"))
  dt_all <- merge(pred[, -c("GPPsat","NEPmax","ETmax","WUE","uWUE", grep("_anom", names(pred), value = TRUE)), with = FALSE],
                  flux[, c("SITE_ID","YEAR", RESPONSE_VARS), with = FALSE], by = c("SITE_ID","YEAR"))
  ac <- colnames(dt_all)
  clim <- if (W == "12m") grep("^lag1_", ac, value = TRUE) else grep("^(lag1_|lag2_)", ac, value = TRUE)
  trait <- grep("^(P12_|P50_|P88_|gsmax_|rdmax_|SSD|SLA|Leaf|Stem)", ac, value = TRUE)
  dist <- grep("^(absolute_|relative_|new_|mortality_|disturbance_)", ac, value = TRUE)
  xv3 <- unique(c(clim, trait)); xv4 <- unique(c(clim, trait, dist))
  for (resp in RESPONSE_VARS) {
    dt <- dt_all[, unique(c("SITE_ID","YEAR", resp, xv4)), with = FALSE]
    dt <- dt[complete.cases(dt)]
    for (mod in c("M3", "M4")) {
      p <- oof(dt, if (mod == "M3") xv3 else xv4, resp)
      p[, `:=`(learner = LEARNER, cv_mode = CV_MODE, window = W, response = resp, model = mod)]
      res[[length(res) + 1]] <- p
      cat(sprintf("%s %s %-11s %s: %d preds, %d sites  [%.1f min]\n", LEARNER, W, resp, mod, nrow(p),
                  uniqueN(p$SITE_ID), as.numeric(difftime(Sys.time(), t0, units = "mins"))))
    }
  }
}
out <- rbindlist(res)
fwrite(out, OUT_FILE)
cat("SAVED:", OUT_FILE, nrow(out), "rows\nSITE_PREDICTIONS_DONE\n")
