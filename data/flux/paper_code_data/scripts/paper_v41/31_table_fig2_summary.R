#!/usr/bin/env Rscript
# ============================================================================
# Table summarising Fig 2: effect of adding disturbance (M3 = C+T -> M4 = C+T+D) on per-site RMSE,
# for all sites, Mid + High and High relative-disturbance sites.
#   classes: site maximum of relative disturbance across years, mean of the five buffers 100-500 m
#            (paper_class_data(), CLASS_BUF in paper_common.R); breaks mean -/+ 0.5 SD over sites
#            (Low < lower break <= Mid <= upper break < High), as in Figs 2-4
#   rows per learner x window x EFP: median paired change in per-site RMSE (%) with significance stars
#            (one-sided paired Wilcoxon, M4 < M3, BH-FDR across every test in the table) and the share of
#            sites whose RMSE improved
#   learners: XGBoost and LightGBM (Optuna), AutoGluon (ensemble) - repeated CV (3 x 80 %) predictions
# Output: manuscript/tables/table_fig2_dRMSE_reldist_v41.tex (+ .csv next to the Fig 2 statistics)
#   Rscript 31_table_fig2_summary.R
# ============================================================================
suppressMessages(library(data.table))
setwd(Sys.getenv("PAPER_ROOT", "."))
source("scripts/paper_v41/paper_common.R")
PN <- paper_n()
VTAG <- c(efp = "v41", efp_v5 = "v5", v10efp = "v22", flux = "v41_flux", v10flux = "v22_flux")[[PAPER_RESP_SET]]   # file / label tag
if (PAPER_NO_THRESH) VTAG <- paste0(VTAG, "_nothresh")   # never overwrite the main tables
# error metric (env TABLE1_METRIC): RMSE (default) or MAE (2026-10-08) - file names, caption and row labels follow it
MET <- toupper(Sys.getenv("TABLE1_METRIC", "RMSE")); stopifnot(MET %in% c("RMSE", "MAE"))
# test (env TABLE1_TEST): wilcoxon (default; median shown) or perm (2026-10-08, advisor): paired sign-flip permutation test
# of the MEAN per-site relative change, one-sided (C+T+M < C+T), N_PERM random sign flips; files get the suffix _perm
# or permlog: the same sign-flip test on the per-site log ratio log(M4/M3) - robust to sites with a tiny C+T error;
# the table shows the geometric-mean change 100 * (exp(mean log ratio) - 1); suffix _permlog
TEST <- tolower(Sys.getenv("TABLE1_TEST", "wilcoxon")); stopifnot(TEST %in% c("wilcoxon", "perm", "permlog"))
N_PERM <- 100000L; TSFX <- if (TEST == "wilcoxon") "" else paste0("_", TEST)
# sensitivity to the unequal number of site-years (2026-10-08): env TABLE1_MINYEARS (keep sites with >= k site-years)
# and TABLE1_WEIGHT (none | n | sqrt: weight each site's value by its site-years); signs are still flipped per site.
# Class breaks stay those of all sites; n in the header = sites kept. Suffix e.g. _permlog_min3, _permlog_wsqrt
MINY <- as.integer(Sys.getenv("TABLE1_MINYEARS", "1")); WT <- tolower(Sys.getenv("TABLE1_WEIGHT", "none"))
stopifnot(WT %in% c("none", "n", "sqrt"), WT == "none" || TEST != "wilcoxon")
if (MINY > 1) TSFX <- paste0(TSFX, "_min", MINY)
if (WT != "none") TSFX <- paste0(TSFX, "_w", WT)
wfun <- function(n) switch(WT, none = rep(1, length(n)), n = n, sqrt = sqrt(n))
perm_p <- function(d, w = rep(1, length(d))) {   # P(weighted mean of randomly sign-flipped values <= observed), +1 correction
  set.seed(42); n <- length(d); w <- w / sum(w)
  s <- matrix(sample(c(-1, 1), N_PERM * n, replace = TRUE), N_PERM, n)
  (1 + sum(as.vector(s %*% (w * d)) <= sum(w * d))) / (N_PERM + 1)
}
LEARNERS <- list(XGB = list(lab = "XGBoost", f = file.path(PAPER_OUT, "XGB_predictions_repCV.csv")),
                 LGB = list(lab = "LightGBM", f = file.path(PAPER_OUT, "LGB_predictions_repCV.csv")),
                 # AutoGluon from the matching run: outputs/<set>_nothresh for the no-threshold variant (skipped until it exists)
                 AG  = list(lab = "AutoGluon", f = file.path(PAPER_ROOT, "AutoGluon/outputs",
                                                         paste0(PAPER_RESP_SET, if (PAPER_NO_THRESH) "_nothresh" else ""), "AG_predictions_repCV.csv")))
EFP_TEX <- c(GPPsat = "GPP$_\\mathrm{sat}$", NEPmax = "NEP$_\\mathrm{max}$", ETmax = "ET$_\\mathrm{max}$", WUE = "WUE",
             GPP_gs = "GPP", NEP_gs = "NEP", ET_gs = "ET", WUE_gs = "WUE")

# ---- site classes (relative disturbance, class buffer) --------------------------------
cl <- paper_class_data()[, .(v = max(relative_disturbance_cls, na.rm = TRUE)), by = SITE_ID]
mu <- mean(cl$v); s <- sd(cl$v); b_lo <- max(mu - 0.5 * s, 0); b_hi <- mu + 0.5 * s
cl[, cls := ifelse(v < b_lo, "Low", ifelse(v > b_hi, "High", "Mid"))]
SUBSETS <- list(all = cl$SITE_ID, midhigh = cl[cls != "Low", SITE_ID], high = cl[cls == "High", SITE_ID])

# ---- per-site RMSE, paired change, tests -----------------------------------------------
rows <- list()
for (L in names(LEARNERS)) {
  if (!file.exists(LEARNERS[[L]]$f)) { message("missing ", LEARNERS[[L]]$f); next }
  p <- fread(LEARNERS[[L]]$f)[response %in% EFP_ORDER & grepl("^M[34]_(12m|24m)$", model)]
  r <- p[, .(rmse = if (MET == "RMSE") sqrt(mean((observed - predicted)^2)) else mean(abs(observed - predicted)), n_sy = .N), by = .(model, response, SITE_ID)]
  r[, c("mod", "win") := tstrsplit(model, "_")]
  w <- dcast(r, response + win + SITE_ID + n_sy ~ mod, value.var = "rmse")[is.finite(M3) & is.finite(M4) & n_sy >= MINY]
  for (k in names(SUBSETS)) {
    x <- w[SITE_ID %in% SUBSETS[[k]]]
    rows[[length(rows) + 1]] <- x[, .(learner = L, n = .N, median_pct = median((M4 - M3) / M3 * 100),
                                      mean_pct = mean((M4 - M3) / M3 * 100),
                                      geomean_pct = 100 * (exp(weighted.mean(log(M4 / M3), wfun(n_sy))) - 1),
                                      improved_pct = 100 * mean(M4 < M3),
                                      p = if (TEST == "perm") perm_p((M4 - M3) / M3 * 100, wfun(n_sy)) else if (TEST == "permlog") perm_p(log(M4 / M3), wfun(n_sy)) else
                                        wilcox.test(M4, M3, paired = TRUE, alternative = "less", exact = FALSE)$p.value),
                                  by = .(response, window = win)][, subset := k]
  }
}
st <- rbindlist(rows)
st[, q := p.adjust(p, method = "BH")]
st[, sig := fifelse(q < 0.001, "***", fifelse(q < 0.01, "**", fifelse(q < 0.05, "*", "")))]
fwrite(st, file.path(PAPER_FIG, sprintf("table_fig2_d%s_total_mort%s_statistics.csv", MET, TSFX)))
n_sub <- if (MINY > 1) st[, max(n), by = subset][, setNames(V1, subset)] else sapply(SUBSETS, length)

# ---- LaTeX ---------------------------------------------------------------------------
SHOW <- switch(TEST, perm = "mean_pct", permlog = "geomean_pct", "median_pct")
cell_d <- function(x) sprintf("$%s%.1f$%s", ifelse(x[[SHOW]] < 0, "-", "+"), abs(x[[SHOW]]),
                              ifelse(x$sig == "", "", sprintf("$^{%s}$", x$sig)))
cell_i <- function(x) sprintf("%.0f", x$improved_pct)
L_ok <- intersect(names(LEARNERS), unique(st$learner))
# env TABLE1_SHOW (e.g. "AG"; 2026-10-09, coauthor draft): show only these learners. Since 2026-10-20 (draft reports
# AutoGluon only) the BH correction is redone over the tests of the shown learners; the statistics CSV keeps all learners.
SHOWL <- strsplit(Sys.getenv("TABLE1_SHOW", ""), ",")[[1]]
if (length(SHOWL)) {
  L_ok <- intersect(L_ok, SHOWL); TSFX <- paste0(TSFX, "_", paste(L_ok, collapse = ""))
  st <- st[learner %in% L_ok][, q := p.adjust(p, method = "BH")]
  st[, sig := fifelse(q < 0.001, "***", fifelse(q < 0.01, "**", fifelse(q < 0.05, "*", "")))]
}
BH_TXT <- sprintf("all %d tests in the table", nrow(st))
tex <- c(
  "\\begin{table}[p]",
  "\\centering",
  sprintf(paste0("\\caption{\\textbf{Adding mortality reduces the per-site prediction error} (site level; per-site errors in Fig.~A1). ",
    "For every site, ", MET, " of the model with climate, traits and mortality (C+T+M) is compared with the model without ",
    "mortality (C+T), both evaluated by repeated leave-one-site-out cross-validation (3 $\\times$ 80\\%%). ",
    if (TEST == "permlog") sprintf("$\\Delta$%s = geometric mean over sites of the paired error ratio, (exp(mean$_\\mathrm{sites}$ ln(%s$_\\mathrm{C+T+M}$ / %s$_\\mathrm{C+T}$)) $-$ 1) $\\times$ 100; ", MET, MET, MET) else
    sprintf(paste0("$\\Delta$%s = ", if (TEST == "perm") "mean" else "median", " over sites of the paired relative change, (%s$_\\mathrm{C+T+M}$ $-$ %s$_\\mathrm{C+T}$) / %s$_\\mathrm{C+T}$ $\\times$ 100; "), MET, MET, MET, MET),
    sprintf("negative values mean that mortality reduces the error. Improved = share of sites whose %s decreased. ", MET),
    if (MINY > 1) sprintf("Sensitivity: only sites with at least %d site-years. ", MINY) else "",
    switch(WT, n = "Sensitivity: sites weighted by their number of site-years (signs flipped per site). ",
           sqrt = "Sensitivity: sites weighted by the square root of their number of site-years (signs flipped per site). ", ""),
    sprintf("Sites are grouped by their maximum relative mortality across years (%s); ", if (CLASS_BUF == "avg") "averaged over the five buffers, 100--500\\,m" else paste0(CLASS_BUF, "\\,m buffer")),
    "High $>$ mean $+$ 0.5\\,SD (%.1f\\%%), Low $<$ mean $-$ 0.5\\,SD (%.1f\\%%), Mid in between. ",
    if (TEST == "permlog") "Stars: one-sided paired permutation test on the per-site log error ratios (100\\,000 random sign flips; C+T+M $<$ C+T), Benjamini--Hochberg corrected across %s " else
    if (TEST == "perm") "Stars: one-sided paired permutation test (100\\,000 random sign flips of the per-site changes; C+T+M $<$ C+T), Benjamini--Hochberg corrected across %s " else "Stars: one-sided paired Wilcoxon signed-rank test (C+T+M $<$ C+T), Benjamini--Hochberg corrected across %s ",
    "($^{*}$\\,$q<0.05$, $^{**}$\\,$q<0.01$, $^{***}$\\,$q<0.001$). Tree cover $\\geq$ 30\\%%, %d sites, %d site-years, %s.}"),
    b_hi, b_lo, BH_TXT, PN[["sites"]], PN[["sy"]], PAPER_MAPS),
  sprintf("\\label{tab:fig2_d%s_total_mort_%s%s}", MET, VTAG, TSFX),
  "\\scriptsize",
  "\\setlength{\\tabcolsep}{4pt}",
  "\\renewcommand{\\arraystretch}{0.92}",
  "\\begin{tabular}{lllccc}",
  "\\toprule",
  sprintf("Model & EFP & & All sites & Mid + High & High \\\\"),
  sprintf(" & & & ($n$ = %d) & ($n$ = %d) & ($n$ = %d) \\\\", n_sub[["all"]], n_sub[["midhigh"]], n_sub[["high"]]),
  "\\midrule")
first_block <- TRUE
for (L in L_ok) for (wn in WINDOWS) {
  if (!first_block) tex <- c(tex, "\\midrule"); first_block <- FALSE
  lab <- sprintf("\\multirow{%d}{*}{\\shortstack[l]{%s\\\\%s}}", 2 * length(EFP_ORDER), LEARNERS[[L]]$lab, sub("m$", "\\\\,months", wn))
  for (i in seq_along(EFP_ORDER)) {
    e <- EFP_ORDER[i]
    g <- function(k) st[learner == L & window == wn & response == e & subset == k]
    first <- if (i == 1) lab else ""
    tex <- c(tex,
      sprintf("%s & \\multirow{2}{*}{%s} & $\\Delta$%s (\\%%) & %s & %s & %s \\\\", first, EFP_TEX[[e]], MET,
              cell_d(g("all")), cell_d(g("midhigh")), cell_d(g("high"))),
      sprintf(" & & Improved (\\%%) & %s & %s & %s \\\\", cell_i(g("all")), cell_i(g("midhigh")), cell_i(g("high"))))
    if (i < length(EFP_ORDER)) tex <- c(tex, "\\cmidrule(l){2-6}")
  }
}
tex <- c(tex, "\\bottomrule", "\\end{tabular}", "\\end{table}")
out <- file.path(PAPER_ROOT, "manuscript/tables", sprintf("table_fig2_d%s_total_mort_%s%s.tex", MET, VTAG, TSFX))
writeLines(tex, out)
cat("wrote", out, "\n")
print(dcast(st, learner + window + response ~ subset, value.var = "median_pct", fun.aggregate = function(v) round(v, 1)))
