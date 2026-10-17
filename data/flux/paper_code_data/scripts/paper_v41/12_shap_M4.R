#!/usr/bin/env Rscript
# ============================================================================
# TreeSHAP for M4 (C+T+D), 12m and 24m, as in the V10 coauthor-draft SHAP
# (run_v10_lag2_signed_shap_M4.R): the model is fit on all site-years with the tuned
# hyperparameters, SHAP values are computed per site-year and averaged per site.
# Writes, in the V10 formats the coauthor figures read:
#   <L>_site_shap_M4.csv         variable, mean_abs_shap, model, response, test_site, group   (Figs 3, 5)
#   <L>_site_signed_shap_M4.csv  group, mean_signed_shap, mean_abs_shap, model, response, test_site (Figs 4, 5)
# Also M3 group-level SHAP is NOT computed (the paper figures only use M4).
#   Rscript 12_shap_M4.R <XGB|LGB>
# ============================================================================
Sys.setenv(OMP_NUM_THREADS = "1", OPENBLAS_NUM_THREADS = "1")
suppressMessages({library(data.table); library(treeshap); library(parallel)})
source(paste0(Sys.getenv("PAPER_ROOT", "."), "/scripts/paper_v41/paper_common.R"))
setDTthreads(1)
L <- commandArgs(trailingOnly = TRUE)[1]; stopifnot(L %in% c("XGB", "LGB"))
if (L == "XGB") suppressMessages(library(xgboost)) else suppressMessages(library(lightgbm))
N_CORES <- as.integer(Sys.getenv("PAPER_CORES", "30"))
df <- paper_load()
safe_names <- function(x) make.unique(gsub("[^A-Za-z0-9_]", "_", x), sep = "_")
per_var <- list(); per_grp <- list(); t0 <- Sys.time()

for (win in WINDOWS) for (resp in EFP_ORDER) {
  o <- fread(file.path(PAPER_TUNE, win, "best_configs.csv"))[learner == L & response == resp]
  xv <- paper_cols(df, win, "C+T+D"); sn <- safe_names(xv); to_orig <- setNames(xv, sn)
  Xdf <- df[, xv, drop = FALSE]; colnames(Xdf) <- sn
  if (L == "XGB") {
    p <- list(objective = "reg:squarederror", nthread = 1, seed = 42, learning_rate = o$learning_rate,
              max_depth = as.integer(o$max_depth), min_child_weight = o$min_child_weight, subsample = o$subsample,
              colsample_bytree = o$colsample_bytree, reg_lambda = o$reg_lambda)
    bst <- xgb.train(params = p, data = xgb.DMatrix(as.matrix(Xdf), label = df[[resp]]), nrounds = as.integer(o$nrounds), verbose = 0)
    uni <- xgboost.unify(bst, Xdf)
  } else {
    p <- list(objective = "regression", metric = "rmse", num_threads = 1, seed = 42, verbosity = -1,
              learning_rate = o$learning_rate, num_leaves = as.integer(o$num_leaves),
              min_data_in_leaf = as.integer(o$min_data_in_leaf), feature_fraction = o$feature_fraction,
              bagging_fraction = o$bagging_fraction, bagging_freq = 1, lambda_l2 = o$lambda_l2)
    bst <- lgb.train(params = p, data = lgb.Dataset(as.matrix(Xdf), label = df[[resp]]), nrounds = as.integer(o$nrounds), verbose = -1)
    uni <- lightgbm.unify(bst, Xdf)
  }
  sites <- unique(df$SITE_ID)
  res <- mclapply(sites, function(s) {
    te <- Xdf[df$SITE_ID == s, , drop = FALSE]
    sr <- tryCatch(treeshap(uni, te, verbose = FALSE), error = function(e) NULL)
    if (is.null(sr)) return(NULL)
    sm <- as.data.table(sr$shaps); orig <- to_orig[names(sm)]
    if (anyNA(orig)) stop("SHAP name mapping lost variables")
    grp <- paper_group(orig)
    pv <- data.table(variable = orig, mean_abs_shap = colMeans(abs(as.matrix(sm))), model = paste0("M4_", win),
                     response = resp, test_site = s, group = grp)
    pg <- rbindlist(lapply(unique(grp), function(g) {
      cols <- names(sm)[grp == g]
      data.table(group = g, signed = rowSums(sm[, ..cols]), absol = rowSums(abs(sm[, ..cols])))
    }))[, .(mean_signed_shap = mean(signed), mean_abs_shap = mean(absol)), by = group]
    pg[, `:=`(model = paste0("M4_", win), response = resp, test_site = s)]
    list(pv = pv, pg = pg)
  }, mc.cores = N_CORES)
  res <- Filter(Negate(is.null), res)
  per_var[[length(per_var) + 1]] <- rbindlist(lapply(res, `[[`, "pv"))
  per_grp[[length(per_grp) + 1]] <- rbindlist(lapply(res, `[[`, "pg"))
  cat(sprintf("%s %s %-7s: %d sites  [%.1f min]\n", L, win, resp, length(res), as.numeric(difftime(Sys.time(), t0, units = "mins"))))
}
fwrite(rbindlist(per_var), file.path(PAPER_OUT, sprintf("%s_site_shap_M4.csv", L)))
fwrite(rbindlist(per_grp), file.path(PAPER_OUT, sprintf("%s_site_signed_shap_M4.csv", L)))
cat("SHAP_DONE\n")
