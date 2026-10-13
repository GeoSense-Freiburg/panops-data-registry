#!/usr/bin/env Rscript
# ============================================================================
# Train M3 (C+T) and M4 (C+T+D), 12m and 24m, under repeated leave-one-site-out CV
# (3 reps x 80 % of the training sites per held-out site, predictions averaged) - the scheme
# of run_v10_lag2_optuna.R / the V10 repCV outputs. Hyperparameters: Optuna per learner x
# response x window (plots/paper_v41/Optuna/<window>/best_configs.csv).
# Output (same columns as the V10 repCV predictions, so the Fig. 2 port reads it unchanged):
#   derived_tables/paper_v41/models/<L>_predictions_repCV.csv   model = M3_12m | M4_12m | M3_24m | M4_24m
#   derived_tables/paper_v41/models/<L>_metrics_repCV.csv
#   Rscript 11_train_repcv.R <XGB|LGB>
# ============================================================================
Sys.setenv(OMP_NUM_THREADS = "1", OPENBLAS_NUM_THREADS = "1")
suppressMessages({library(data.table); library(parallel)})
source(paste0(Sys.getenv("PAPER_ROOT", "."), "/scripts/paper_v41/paper_common.R"))
setDTthreads(1)
L <- commandArgs(trailingOnly = TRUE)[1]; stopifnot(L %in% c("XGB", "LGB"))
if (L == "XGB") suppressMessages(library(xgboost)) else suppressMessages(library(lightgbm))
SEED <- 42; N_REPS <- 3L; SUBSAMP <- 0.8
N_CORES <- as.integer(Sys.getenv("PAPER_CORES", "30"))
dir.create(PAPER_OUT, showWarnings = FALSE, recursive = TRUE)
df <- as.data.table(paper_load())
safe <- function(x) make.unique(gsub("[^A-Za-z0-9_]", "_", x), sep = "_")

cfg_of <- function(win, resp) {
  f <- file.path(PAPER_TUNE, win, "best_configs.csv")
  c <- fread(f)[learner == L & response == resp]
  if (nrow(c) != 1) stop(sprintf("no tuned config for %s %s %s", L, win, resp))
  c
}
fit_predict <- function(tr, te, xv, resp, o) {
  if (L == "XGB") {
    p <- list(objective = "reg:squarederror", nthread = 1, seed = SEED, learning_rate = o$learning_rate,
              max_depth = as.integer(o$max_depth), min_child_weight = o$min_child_weight, subsample = o$subsample,
              colsample_bytree = o$colsample_bytree, reg_lambda = o$reg_lambda)
    f <- tryCatch(xgb.train(params = p, data = xgb.DMatrix(as.matrix(tr[, ..xv]), label = tr[[resp]]),
                            nrounds = as.integer(o$nrounds), verbose = 0), error = function(e) NULL)
    if (is.null(f)) return(NULL)
    return(as.numeric(predict(f, xgb.DMatrix(as.matrix(te[, ..xv])))))
  }
  p <- list(objective = "regression", metric = "rmse", num_threads = 1, seed = SEED, verbosity = -1,
            learning_rate = o$learning_rate, num_leaves = as.integer(o$num_leaves),
            min_data_in_leaf = as.integer(o$min_data_in_leaf), feature_fraction = o$feature_fraction,
            bagging_fraction = o$bagging_fraction, bagging_freq = 1, lambda_l2 = o$lambda_l2)
  sv <- safe(xv); Xtr <- as.matrix(tr[, ..xv]); colnames(Xtr) <- sv; Xte <- as.matrix(te[, ..xv]); colnames(Xte) <- sv
  f <- tryCatch(lgb.train(params = p, data = lgb.Dataset(Xtr, label = tr[[resp]]), nrounds = as.integer(o$nrounds),
                          verbose = -1), error = function(e) NULL)
  if (is.null(f)) return(NULL)
  as.numeric(predict(f, Xte))
}

preds <- list(); mets <- list(); t0 <- Sys.time()
for (win in WINDOWS) for (resp in EFP_ORDER) {
  o <- cfg_of(win, resp)
  for (mod in c("M3", "M4")) {
    xv <- paper_cols(df, win, if (mod == "M3") "C+T" else "C+T+D")
    sites <- sort(unique(df$SITE_ID))
    fold <- rbindlist(mclapply(sites, function(ts) {
      tr_all <- df[SITE_ID != ts]; te <- df[SITE_ID == ts]
      tr_sites <- unique(tr_all$SITE_ID); k <- max(5L, floor(SUBSAMP * length(tr_sites)))
      pm <- matrix(NA_real_, nrow(te), N_REPS)
      for (r in seq_len(N_REPS)) {
        set.seed(SEED + r * 1000L + which(sites == ts))
        pr <- fit_predict(tr_all[SITE_ID %in% sample(tr_sites, k)], te, xv, resp, o)
        if (!is.null(pr)) pm[, r] <- pr
      }
      ok <- rowSums(!is.na(pm)) > 0
      if (!any(ok)) return(NULL)
      data.table(model = paste0(mod, "_", win), response = resp, SITE_ID = ts, YEAR = te$YEAR, observed = te[[resp]],
                 predicted = rowMeans(pm, na.rm = TRUE), pred_sd = apply(pm, 1, sd, na.rm = TRUE),
                 n_reps_used = rowSums(!is.na(pm)))[ok]
    }, mc.cores = N_CORES), fill = TRUE)
    preds[[length(preds) + 1]] <- fold
    ob <- fold$observed; pr <- fold$predicted
    mets[[length(mets) + 1]] <- data.table(learner = L, model = paste0(mod, "_", win), response = resp, n_predictors = length(xv),
      n_pairs = nrow(fold), n_sites = uniqueN(fold$SITE_ID), RMSE = sqrt(mean((pr - ob)^2)), MAE = mean(abs(pr - ob)),
      R2 = 1 - sum((ob - pr)^2) / sum((ob - mean(ob))^2))
    cat(sprintf("%s %s %-7s %s: %3d preds, %d rows, RMSE %.4f  [%.1f min]\n", L, win, resp, mod, length(xv), nrow(fold),
                tail(mets, 1)[[1]]$RMSE, as.numeric(difftime(Sys.time(), t0, units = "mins"))))
  }
}
fwrite(rbindlist(preds), file.path(PAPER_OUT, sprintf("%s_predictions_repCV.csv", L)))
m <- rbindlist(mets); fwrite(m, file.path(PAPER_OUT, sprintf("%s_metrics_repCV.csv", L)))
w <- dcast(m, response + sub("^M[34]_", "", model) ~ sub("_.*", "", model), value.var = "RMSE")
setnames(w, 2, "window"); w[, dRMSE_pct := round(100 * (M4 - M3) / M3, 2)]
print(w)
cat("TRAIN_DONE\n")
