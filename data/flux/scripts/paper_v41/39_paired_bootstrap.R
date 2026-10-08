#!/usr/bin/env Rscript
# ============================================================================
# Paired bootstrap + paired permutation of the error change C+T -> C+T+M (2026-10-08, advisor / Codex suggestion).
# Two levels of observation:
#   site      - one MAE (RMSE) per site and model over its years; d_i = err_i(C+T+M) - err_i(C+T);
#               statistic = mean d_i over sites; % = 100 * (mean err C+T+M / mean err C+T - 1) (ratio of the mean
#               site errors, not the mean of site ratios -> no near-zero-denominator problem)
#   siteyear  - every site-year is an observation (no averaging over years): statistic = pooled MAE (RMSE) of
#               C+T+M minus pooled C+T over the site-years; % = ratio of the pooled errors
# Dependence: years of one site are not independent (same site, same held-out models), so both levels resample
# and permute WHOLE SITES: bootstrap = sites drawn with replacement (with all their years), N_BOOT draws,
# 95 % percentile interval; permutation = C+T / C+T+M labels swapped per site (all years together), N_PERM
# draws, one-sided (C+T+M better), BH-corrected across each table.
# Subsets: all sites, Mid+High, High sites (site maximum of total mortality, as Tables 1-2); at site-year level
# also site-years whose OWN total mortality is above the Mid / High break (years without mortality left out).
# Predictions = the repeated CV predictions (mean over the 3 repetitions per site-year), as in Tables 1-2.
# Output: plots/<sub>/paired_bootstrap_statistics.csv
#   PAPER_RESP_SET=efp_v5 PAPER_NO_THRESH=1 Rscript 39_paired_bootstrap.R
# ============================================================================
if (Sys.getenv("PAPER_RESP_SET") == "") Sys.setenv(PAPER_RESP_SET = "efp_v5", PAPER_NO_THRESH = "1")
suppressMessages(library(data.table))
setwd("/mnt/gsdata/projects/panops/panops-data-registry/data/flux")
source("scripts/paper_v41/paper_common.R")
N_BOOT <- 10000L; N_PERM <- 10000L; SEED <- 42
LEARNERS <- list(XGB = file.path(PAPER_OUT, "XGB_predictions_repCV.csv"),
                 LGB = file.path(PAPER_OUT, "LGB_predictions_repCV.csv"),
                 AG  = file.path(PAPER_ROOT, "AutoGluon/outputs",
                                 paste0(PAPER_RESP_SET, if (PAPER_NO_THRESH) "_nothresh" else ""), "AG_predictions_repCV.csv"))

# ---- classes: site maximum of total mortality (as Tables 1-2) and each site-year's own value -------------
cd <- paper_class_data()[, .(SITE_ID, YEAR, tm = relative_disturbance_cls)]
cl <- cd[, .(v = max(tm, na.rm = TRUE)), by = SITE_ID]
mu <- mean(cl$v); s <- sd(cl$v); b_lo <- max(mu - 0.5 * s, 0); b_hi <- mu + 0.5 * s
cl[, cls := ifelse(v < b_lo, "Low", ifelse(v > b_hi, "High", "Mid"))]
SITE_SUB <- list(all = cl$SITE_ID, midhigh = cl[cls != "Low", SITE_ID], high = cl[cls == "High", SITE_ID])

# ---- core: units = sites; A, B = summed error terms of C+T+M / C+T per site, n = observations per site ----
# g = identity (MAE) or sqrt (RMSE, error terms = squared errors); statistic = g(sum A / sum n) - g(sum B / sum n)
stat_fun <- function(A, B, n, g) { a <- g(A / n); b <- g(B / n); cbind(diff = a - b, pct = 100 * (a / b - 1)) }
test_units <- function(A, B, n, g) {
  k <- length(A); obs <- stat_fun(sum(A), sum(B), sum(n), g)
  set.seed(SEED)
  idx <- sample.int(k, N_BOOT * k, replace = TRUE) + rep((seq_len(N_BOOT) - 1L) * k, each = k)
  W <- matrix(tabulate(idx, N_BOOT * k), N_BOOT, k, byrow = TRUE)        # bootstrap counts per site
  bs <- stat_fun(as.vector(W %*% A), as.vector(W %*% B), as.vector(W %*% n), g)
  S <- matrix(rbinom(N_PERM * k, 1, 0.5), N_PERM)                        # 1 = swap the labels of that site
  dAB <- A - B
  ps <- stat_fun(sum(A) - as.vector(S %*% dAB), sum(B) + as.vector(S %*% dAB), sum(n), g)
  data.table(n_units = k, n_obs = sum(n), diff = obs[1, "diff"], pct = obs[1, "pct"],
             diff_lo = quantile(bs[, "diff"], 0.025), diff_hi = quantile(bs[, "diff"], 0.975),
             pct_lo = quantile(bs[, "pct"], 0.025), pct_hi = quantile(bs[, "pct"], 0.975),
             p_boot = (1 + sum(bs[, "diff"] >= 0)) / (N_BOOT + 1),            # one-sided: share of draws without improvement
             p_perm = (1 + sum(ps[, "diff"] <= obs[1, "diff"])) / (N_PERM + 1))
}

rows <- list()
for (L in names(LEARNERS)) {
  if (!file.exists(LEARNERS[[L]])) { message("missing ", LEARNERS[[L]]); next }
  p <- fread(LEARNERS[[L]])[response %in% EFP_ORDER & grepl("^M[34]_(12m|24m)$", model)]
  p[, c("mod", "window") := tstrsplit(model, "_")]
  w <- dcast(p, response + window + SITE_ID + YEAR + observed ~ mod, value.var = "predicted")[is.finite(M3) & is.finite(M4)]
  w <- merge(w, cd[, .(SITE_ID, YEAR, tm)], by = c("SITE_ID", "YEAR"), all.x = TRUE)
  for (MET in c("MAE", "RMSE")) {
    e <- if (MET == "MAE") function(x) abs(x) else function(x) x^2
    g <- if (MET == "MAE") identity else sqrt
    w[, `:=`(e3 = e(observed - M3), e4 = e(observed - M4))]
    for (k in names(SITE_SUB)) {
      x <- w[SITE_ID %in% SITE_SUB[[k]]]
      # site level: one error per site, each site weight 1
      s1 <- x[, .(A = g(mean(e4)), B = g(mean(e3)), n = 1), by = .(response, window, SITE_ID)]
      rows[[length(rows) + 1]] <- s1[, test_units(A, B, n, identity), by = .(response, window)][
        , `:=`(learner = L, metric = MET, level = "site", subset = k)]
      # site-year level: pooled over site-years, sites resampled / permuted as blocks
      s2 <- x[, .(A = sum(e4), B = sum(e3), n = .N), by = .(response, window, SITE_ID)]
      rows[[length(rows) + 1]] <- s2[, test_units(A, B, n, g), by = .(response, window)][
        , `:=`(learner = L, metric = MET, level = "siteyear", subset = k)]
    }
    # site-year level, years selected by their OWN total mortality (all sites)
    for (k in c("yr_midhigh", "yr_high")) {
      x <- w[!is.na(tm) & tm > (if (k == "yr_high") b_hi else b_lo)]
      s2 <- x[, .(A = sum(e4), B = sum(e3), n = .N), by = .(response, window, SITE_ID)]
      rows[[length(rows) + 1]] <- s2[, test_units(A, B, n, g), by = .(response, window)][
        , `:=`(learner = L, metric = MET, level = "siteyear", subset = k)]
    }
  }
  cat(L, "done\n")
}
st <- rbindlist(rows)
st[, q := p.adjust(p_perm, "BH"), by = .(metric, level)]
st[, sig := fifelse(q < 0.001, "***", fifelse(q < 0.01, "**", fifelse(q < 0.05, "*", "")))]
st[, ci_below_0 := diff_hi < 0]
st[, `:=`(b_lo = b_lo, b_hi = b_hi)]
setcolorder(st, c("learner", "metric", "level", "subset", "window", "response"))
fwrite(st, file.path(PAPER_FIG, "paired_bootstrap_statistics.csv"))
print(st[, .(tests = .N, ci_below_0 = sum(ci_below_0), perm_q05 = sum(q < 0.05)), by = .(metric, level)])
cat("BOOT_DONE\n")
