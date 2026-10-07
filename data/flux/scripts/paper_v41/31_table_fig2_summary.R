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
setwd("/mnt/gsdata/projects/panops/panops-data-registry/data/flux")
source("scripts/paper_v41/paper_common.R")
PN <- paper_n()
VTAG <- c(efp = "v41", efp_v5 = "v5", v10efp = "v22", flux = "v41_flux", v10flux = "v22_flux")[[PAPER_RESP_SET]]   # file / label tag
if (PAPER_NO_THRESH) VTAG <- paste0(VTAG, "_nothresh")   # never overwrite the main tables
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
  r <- p[, .(rmse = sqrt(mean((observed - predicted)^2))), by = .(model, response, SITE_ID)]
  r[, c("mod", "win") := tstrsplit(model, "_")]
  w <- dcast(r, response + win + SITE_ID ~ mod, value.var = "rmse")[is.finite(M3) & is.finite(M4)]
  for (k in names(SUBSETS)) {
    x <- w[SITE_ID %in% SUBSETS[[k]]]
    rows[[length(rows) + 1]] <- x[, .(learner = L, n = .N, median_pct = median((M4 - M3) / M3 * 100),
                                      improved_pct = 100 * mean(M4 < M3),
                                      p = wilcox.test(M4, M3, paired = TRUE, alternative = "less", exact = FALSE)$p.value),
                                  by = .(response, window = win)][, subset := k]
  }
}
st <- rbindlist(rows)
st[, q := p.adjust(p, method = "BH")]
st[, sig := fifelse(q < 0.001, "***", fifelse(q < 0.01, "**", fifelse(q < 0.05, "*", "")))]
fwrite(st, file.path(PAPER_FIG, "table_fig2_dRMSE_total_mort_statistics.csv"))
n_sub <- sapply(SUBSETS, length)

# ---- LaTeX ---------------------------------------------------------------------------
cell_d <- function(x) sprintf("$%s%.1f$%s", ifelse(x$median_pct < 0, "-", "+"), abs(x$median_pct),
                              ifelse(x$sig == "", "", sprintf("$^{%s}$", x$sig)))
cell_i <- function(x) sprintf("%.0f", x$improved_pct)
L_ok <- intersect(names(LEARNERS), unique(st$learner))
tex <- c(
  "\\begin{table}[p]",
  "\\centering",
  sprintf(paste0("\\caption{Effect of adding mortality predictors on per-site prediction error (summary of Fig.~2). ",
    "For every site, RMSE of the model with climate, traits and mortality (C+T+M) is compared with the model without ",
    "mortality (C+T), both evaluated by repeated leave-one-site-out cross-validation (3 $\\times$ 80\\%%). ",
    "$\\Delta$RMSE = median over sites of the paired relative change, (RMSE$_\\mathrm{C+T+M}$ $-$ RMSE$_\\mathrm{C+T}$) / RMSE$_\\mathrm{C+T}$ $\\times$ 100; ",
    "negative values mean that mortality reduces the error. Improved = share of sites whose RMSE decreased. ",
    sprintf("Sites are grouped by their maximum total mortality across years (%s); ", if (CLASS_BUF == "avg") "averaged over the five buffers, 100--500\\,m" else paste0(CLASS_BUF, "\\,m buffer")),
    "High $>$ mean $+$ 0.5\\,SD (%.1f\\%%), Low $<$ mean $-$ 0.5\\,SD (%.1f\\%%), Mid in between. ",
    "Stars: one-sided paired Wilcoxon signed-rank test (C+T+M $<$ C+T), Benjamini--Hochberg corrected across all %d tests in the table ",
    "($^{*}$\\,$q<0.05$, $^{**}$\\,$q<0.01$, $^{***}$\\,$q<0.001$). Tree cover $\\geq$ 30\\%%, %d sites, %d site-years, %s.}"),
    b_hi, b_lo, nrow(st), PN[["sites"]], PN[["sy"]], PAPER_MAPS),
  sprintf("\\label{tab:fig2_dRMSE_total_mort_%s}", VTAG),
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
      sprintf("%s & \\multirow{2}{*}{%s} & $\\Delta$RMSE (\\%%) & %s & %s & %s \\\\", first, EFP_TEX[[e]],
              cell_d(g("all")), cell_d(g("midhigh")), cell_d(g("high"))),
      sprintf(" & & Improved (\\%%) & %s & %s & %s \\\\", cell_i(g("all")), cell_i(g("midhigh")), cell_i(g("high"))))
    if (i < length(EFP_ORDER)) tex <- c(tex, "\\cmidrule(l){2-6}")
  }
}
tex <- c(tex, "\\bottomrule", "\\end{tabular}", "\\end{table}")
out <- file.path(PAPER_ROOT, "manuscript/tables", sprintf("table_fig2_dRMSE_total_mort_%s.tex", VTAG))
writeLines(tex, out)
cat("wrote", out, "\n")
print(dcast(st, learner + window + response ~ subset, value.var = "median_pct", fun.aggregate = function(v) round(v, 1)))
