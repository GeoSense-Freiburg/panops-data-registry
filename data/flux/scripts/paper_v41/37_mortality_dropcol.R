#!/usr/bin/env Rscript
# ============================================================================
# Drop-column (leave-one-family-out) test for the mortality block (2026-10-08, advisor's suggestion).
# SHAP divides credit among correlated predictors, so a family's SHAP share can over- or understate what it adds.
# Here every family is removed from / used alone in the model and the change in error is measured.
#   families: absolute deadwood, relative deadwood, deadwood gain, relative tree-cover loss, total mortality
#             (all five buffers and all lags of each)
#   configs:  C+T, C+T+M (all), C+T+M minus one family (5), C+T + one family only (5)
#   unique contribution = RMSE(minus family) / RMSE(C+T+M) - 1   (> 0: the family carries information the others lack)
#   value alone         = 1 - RMSE(C+T + family) / RMSE(C+T)     (> 0: the family helps on its own)
# Same data (PAPER_RESP_SET / PAPER_NO_THRESH, default V5 without thresholds), same repeated leave-one-site-out
# (3 x 80 %, same seeds as 11_train_repcv.R) and the same Optuna settings as the main run - only the predictor set changes.
# Output: derived_tables/<sub>/dropcol/<L>_dropcol_site_rmse.csv, <L>_dropcol_rmse.csv
#   PAPER_RESP_SET=efp_v5 PAPER_NO_THRESH=1 Rscript 37_mortality_dropcol.R <XGB|LGB>
# ============================================================================
Sys.setenv(OMP_NUM_THREADS = "1", OPENBLAS_NUM_THREADS = "1")
if (Sys.getenv("PAPER_RESP_SET") == "") Sys.setenv(PAPER_RESP_SET = "efp_v5", PAPER_NO_THRESH = "1")
suppressMessages({library(data.table); library(parallel)})
source("/mnt/gsdata/projects/panops/panops-data-registry/data/flux/scripts/paper_v41/paper_common.R")
setDTthreads(1)
L <- commandArgs(trailingOnly = TRUE)[1]; stopifnot(L %in% c("XGB", "LGB"))
if (L == "XGB") suppressMessages(library(xgboost)) else suppressMessages(library(lightgbm))
SEED <- 42; N_REPS <- 3L; SUBSAMP <- 0.8
N_CORES <- as.integer(Sys.getenv("PAPER_CORES", "40"))
OUTD <- file.path(dirname(PAPER_OUT), "dropcol"); dir.create(OUTD, showWarnings = FALSE, recursive = TRUE)
df <- as.data.table(paper_load())
safe <- function(x) make.unique(gsub("[^A-Za-z0-9_]", "_", x), sep = "_")
FAMILIES <- c(absolute_mortality = "Absolute deadwood", relative_mortality = "Relative deadwood",
              new_deadwood_gain_pp = "Deadwood gain", relative_tree_loss_pct = "Relative tree-cover loss",
              relative_disturbance = "Total mortality")
fam_of <- function(v) { f <- sub("_[0-9]+m(_lag[12])?$", "", v); ifelse(f %in% names(FAMILIES), f, NA_character_) }

cfg_of <- function(win, resp) {
  c <- fread(file.path(PAPER_TUNE, win, "best_configs.csv"))[learner == L & response == resp]
  if (nrow(c) != 1) stop(sprintf("no tuned config for %s %s %s", L, win, resp)); c
}
fit_predict <- function(tr, te, xv, resp, o) {   # identical to 11_train_repcv.R
  if (L == "XGB") {
    p <- list(objective = "reg:squarederror", nthread = 1, seed = SEED, learning_rate = o$learning_rate,
              max_depth = as.integer(o$max_depth), min_child_weight = o$min_child_weight, subsample = o$subsample,
              colsample_bytree = o$colsample_bytree, reg_lambda = o$reg_lambda)
    f <- xgb.train(params = p, data = xgb.DMatrix(as.matrix(tr[, ..xv]), label = tr[[resp]]), nrounds = as.integer(o$nrounds), verbose = 0)
    return(as.numeric(predict(f, xgb.DMatrix(as.matrix(te[, ..xv])))))
  }
  p <- list(objective = "regression", metric = "rmse", num_threads = 1, seed = SEED, verbosity = -1,
            learning_rate = o$learning_rate, num_leaves = as.integer(o$num_leaves),
            min_data_in_leaf = as.integer(o$min_data_in_leaf), feature_fraction = o$feature_fraction,
            bagging_fraction = o$bagging_fraction, bagging_freq = 1, lambda_l2 = o$lambda_l2)
  sv <- safe(xv); Xtr <- as.matrix(tr[, ..xv]); colnames(Xtr) <- sv; Xte <- as.matrix(te[, ..xv]); colnames(Xte) <- sv
  f <- lgb.train(params = p, data = lgb.Dataset(Xtr, label = tr[[resp]]), nrounds = as.integer(o$nrounds), verbose = -1)
  as.numeric(predict(f, Xte))
}

sites <- sort(unique(df$SITE_ID)); res <- list(); t0 <- Sys.time()
for (win in WINDOWS) for (resp in EFP_ORDER) {
  o <- cfg_of(win, resp)
  ct <- paper_cols(df, win, "C+T"); ctm <- paper_cols(df, win, "C+T+D")
  mort <- setdiff(ctm, ct); fam <- fam_of(mort)
  stopifnot(!anyNA(fam))
  configs <- c(list("C+T" = ct, "C+T+M" = ctm),
               setNames(lapply(names(FAMILIES), function(f) c(ct, mort[fam != f])), paste0("minus:", names(FAMILIES))),
               setNames(lapply(names(FAMILIES), function(f) c(ct, mort[fam == f])), paste0("only:", names(FAMILIES))))
  for (cn in names(configs)) {
    xv <- configs[[cn]]
    pr <- rbindlist(mclapply(seq_along(sites), function(i) {
      ts <- sites[i]; tr_all <- df[SITE_ID != ts]; te <- df[SITE_ID == ts]
      tr_sites <- unique(tr_all$SITE_ID); k <- max(5L, floor(SUBSAMP * length(tr_sites)))
      pm <- sapply(seq_len(N_REPS), function(r) {
        set.seed(SEED + r * 1000L + i)
        fit_predict(tr_all[SITE_ID %in% sample(tr_sites, k)], te, xv, resp, o)
      })
      data.table(SITE_ID = ts, observed = te[[resp]], predicted = rowMeans(matrix(pm, nrow = nrow(te))))
    }, mc.cores = N_CORES))
    res[[length(res) + 1]] <- pr[, .(rmse = sqrt(mean((observed - predicted)^2)), n = .N), by = SITE_ID][
      , `:=`(learner = L, window = win, response = resp, config = cn, n_predictors = length(xv))]
    res[[length(res)]][, overall_rmse := pr[, sqrt(mean((observed - predicted)^2))]]
  }
  cat(sprintf("%s %s %-7s: %d configs  [%.1f min]\n", L, win, resp, length(configs), as.numeric(difftime(Sys.time(), t0, units = "mins"))))
}
R <- rbindlist(res)
fwrite(R, file.path(OUTD, sprintf("%s_dropcol_site_rmse.csv", L)))
O <- unique(R[, .(learner, window, response, config, n_predictors, overall_rmse)])
fwrite(O, file.path(OUTD, sprintf("%s_dropcol_rmse.csv", L)))
cat("DROPCOL_DONE\n")
