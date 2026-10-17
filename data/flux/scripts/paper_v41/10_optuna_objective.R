#!/usr/bin/env Rscript
# ============================================================================
# Optuna objective for the paper_v41 models (called once per trial by scripts/optuna_tune.py,
# which only samples parameters). Same design as the V10 objectives: grouped 5-fold CV with
# disjoint SITE groups (seed 42), scale-free RMSE (RMSE / sd(y)) averaged over two models - here
# M3 (C+T) and M4 (C+T+D), so the chosen configuration serves both and favours neither.
# Window from env PAPER_WINDOW (12m | 24m).
#   Rscript 10_optuna_objective.R <XGB|LGB> <response> <params-json>   -> prints one number or NA
# ============================================================================
Sys.setenv(OMP_NUM_THREADS = "1")
suppressMessages({library(data.table); library(jsonlite)})
source("/mnt/gsdata/projects/panops/panops-data-registry/data/flux/scripts/paper_v41/paper_common.R")
args <- commandArgs(trailingOnly = TRUE)
learner <- args[1]; resp <- args[2]; pj <- fromJSON(args[3])
WIN <- Sys.getenv("PAPER_WINDOW", "12m"); SEED <- 42; NFOLD <- 5
df <- paper_load()
safe <- function(x) make.unique(gsub("[^A-Za-z0-9_]", "_", x), sep = "_")
sites <- unique(df$SITE_ID)
set.seed(SEED); fold_of <- setNames(sample(rep_len(1:NFOLD, length(sites))), sites)
rmses <- c()
for (spec in c("C+T", "C+T+D")) {
  xv <- paper_cols(df, WIN, spec)
  X <- as.matrix(df[, xv]); y <- df[[resp]]
  pred <- rep(NA_real_, nrow(df))
  for (k in 1:NFOLD) {
    te <- fold_of[as.character(df$SITE_ID)] == k
    if (learner == "XGB") {
      suppressMessages(library(xgboost))
      par <- list(objective = "reg:squarederror", learning_rate = pj$learning_rate, max_depth = as.integer(pj$max_depth),
                  min_child_weight = pj$min_child_weight, subsample = pj$subsample, colsample_bytree = pj$colsample_bytree,
                  reg_lambda = pj$reg_lambda, nthread = 1, seed = SEED)
      fit <- tryCatch(xgb.train(params = par, data = xgb.DMatrix(X[!te, , drop = FALSE], label = y[!te]),
                                nrounds = as.integer(pj$nrounds), verbose = 0), error = function(e) NULL)
      if (is.null(fit)) { cat("NA\n"); quit(status = 0) }
      pred[te] <- as.numeric(predict(fit, xgb.DMatrix(X[te, , drop = FALSE])))
    } else {
      suppressMessages(library(lightgbm))
      Xs <- X; colnames(Xs) <- safe(xv)
      par <- list(objective = "regression", metric = "rmse", learning_rate = pj$learning_rate,
                  num_leaves = as.integer(pj$num_leaves), min_data_in_leaf = as.integer(pj$min_data_in_leaf),
                  feature_fraction = pj$feature_fraction, bagging_fraction = pj$bagging_fraction, bagging_freq = 1,
                  lambda_l2 = pj$lambda_l2, num_threads = 1, seed = SEED, verbosity = -1)
      fit <- tryCatch(lgb.train(params = par, data = lgb.Dataset(Xs[!te, , drop = FALSE], label = y[!te]),
                                nrounds = as.integer(pj$nrounds), verbose = -1), error = function(e) NULL)
      if (is.null(fit)) { cat("NA\n"); quit(status = 0) }
      pred[te] <- as.numeric(predict(fit, Xs[te, , drop = FALSE]))
    }
  }
  rmses <- c(rmses, sqrt(mean((pred - y)^2)) / sd(y))
}
cat(sprintf("%.8f\n", mean(rmses)))
