#!/usr/bin/env Rscript
# ============================================================================
# LaTeX table of the paired bootstrap / block permutation (39_paired_bootstrap.R), 2026-10-09.
# Columns: site level (all sites) and site-year level (all sites, High sites, site-years with Mid+High and with
# High total mortality in that year). Cells: % change of the error [95 % bootstrap interval], stars = paired
# permutation with whole sites swapped, BH-corrected across all learners per metric and level (as in the CSV).
#   env TABLE_METRIC = MAE (default) | RMSE;  TABLE_SHOW = "AG" (default) | "XGB,LGB" | ...
# Output: manuscript/tables/table_bootstrap_d<MET>_<VTAG>_<learners>.tex
#   PAPER_RESP_SET=efp_v5 PAPER_NO_THRESH=1 Rscript 40_table_bootstrap.R
# ============================================================================
if (Sys.getenv("PAPER_RESP_SET") == "") Sys.setenv(PAPER_RESP_SET = "efp_v5", PAPER_NO_THRESH = "1")
suppressMessages(library(data.table))
setwd(Sys.getenv("PAPER_ROOT", "."))
source("scripts/paper_v41/paper_common.R")
MET <- toupper(Sys.getenv("TABLE_METRIC", "MAE")); SHOW <- strsplit(Sys.getenv("TABLE_SHOW", "AG"), ",")[[1]]
VTAG <- c(efp = "v41", efp_v5 = "v5", v10efp = "v22")[[PAPER_RESP_SET]]
if (PAPER_NO_THRESH) VTAG <- paste0(VTAG, "_nothresh")
AG_EXTRA <- Sys.getenv("AG_EXTRA", ""); ISFX <- if (AG_EXTRA != "") paste0("_", AG_EXTRA) else ""
st <- fread(file.path(PAPER_FIG, sprintf("paired_bootstrap_statistics%s.csv", ISFX)))[metric == MET]
LAB <- c(XGB = "XGBoost", LGB = "LightGBM", AG = "AutoGluon", AGx = "AutoGluon, EFP-specific climate")
EFP_TEX <- c(GPPsat = "GPP$_\\mathrm{sat}$", NEPmax = "NEP$_\\mathrm{max}$", ETmax = "ET$_\\mathrm{max}$", WUE = "WUE")
COLS <- list(c("site", "all"), c("siteyear", "all"), c("siteyear", "high"), c("siteyear", "yr_midhigh"), c("siteyear", "yr_high"))
# env TABLE_COLS = all (default) | siteyear (2026-10-09, draft main text: site-year columns only); suffix _siteyear
SY_ONLY <- Sys.getenv("TABLE_COLS", "all") == "siteyear"
if (SY_ONLY) COLS <- COLS[-1]
num <- function(x) sprintf("$%s%.1f$", ifelse(x < 0, "-", "+"), abs(x))
cell <- function(x) if (!nrow(x)) "--" else
  sprintf("%s {\\tiny[%s, %s]}%s", num(x$pct), num(x$pct_lo), num(x$pct_hi), ifelse(x$sig == "", "", sprintf("$^{%s}$", x$sig)))
nobs <- function(lv, sb) { x <- st[level == lv & subset == sb]; c(max(x$n_units), max(x$n_obs)) }
hdr_n <- vapply(COLS, function(c) { n <- nobs(c[1], c[2]); if (c[1] == "site") sprintf("%d sites", n[1]) else sprintf("%d / %d", n[1], n[2]) }, "")
b <- unique(st[, .(b_lo, b_hi)])
tex <- c("\\begin{table}[htbp]", "\\centering",
  sprintf(paste0("\\caption{Paired bootstrap of the change in %s when mortality is added (C+T $\\rightarrow$ C+T+M), %s. ",
    "Cells: \\%% change of the error, 100\\,(%s$_\\mathrm{C+T+M}$ / %s$_\\mathrm{C+T}$ $-$ 1), with the 95\\,\\%% bootstrap interval in brackets. ",
    if (SY_ONLY) "%s" else "\\emph{Site level}: one %s per site and model over its years, ratio of the mean site errors. ",
    "\\emph{Site-year level}: every site-year is an observation (pooled %s; no averaging over years). ",
    "Years of one site are not independent, so whole sites are resampled (10\\,000 bootstrap draws) and ",
    "the C+T / C+T+M labels are swapped per site for the paired permutation test (10\\,000 draws, one-sided; stars, ",
    if (AG_EXTRA != "") "Benjamini--Hochberg corrected across both runs: " else "Benjamini--Hochberg corrected across all three learners: ",
    "$^{*}$\\,$q<0.05$, $^{**}$\\,$q<0.01$, $^{***}$\\,$q<0.001$). ",
    "High sites: site maximum of total mortality $>$ %.1f\\,\\%%; the last two columns keep only the site-years whose own ",
    "total mortality exceeds %.1f\\,\\%% (Mid + High) or %.1f\\,\\%% (High). Column heads: sites / site-years. ",
    "%d sites, %d site-years, deadtrees v5.}"),
    MET, paste(LAB[SHOW], collapse = " and "), MET, MET, if (SY_ONLY) "" else MET, MET, b$b_hi, b$b_lo, b$b_hi, max(st$n_units), max(st$n_obs)),
  sprintf("\\label{tab:bootstrap_d%s_%s}", MET, paste(SHOW, collapse = "")),
  "\\scriptsize", "\\setlength{\\tabcolsep}{3pt}",
  "\\resizebox{\\linewidth}{!}{%",
  if (SY_ONLY) c("\\begin{tabular}{llcccc}", "\\toprule",
    " & & All sites & High sites & Mid + High years & High years \\\\") else
  c("\\begin{tabular}{llccccc}", "\\toprule",
    " & & Site level & \\multicolumn{4}{c}{Site-year level} \\\\", "\\cmidrule(lr){3-3}\\cmidrule(lr){4-7}",
    " & & All sites & All sites & High sites & Mid + High years & High years \\\\"),
  sprintf(" & & (%s) \\\\", paste(hdr_n, collapse = ") & (")), "\\midrule")
first <- TRUE
for (L in SHOW) for (w in WINDOWS) {
  if (!first) tex <- c(tex, "\\midrule"); first <- FALSE
  for (i in seq_along(EFP_ORDER)) {
    e <- EFP_ORDER[i]
    lab <- if (i == 1) sprintf("\\multirow{4}{*}{\\shortstack[l]{%s\\\\%s}}", LAB[[L]], sub("m$", "\\\\,months", w)) else ""
    cells <- vapply(COLS, function(c) cell(st[learner == L & window == w & response == e & level == c[1] & subset == c[2]]), "")
    tex <- c(tex, sprintf("%s & %s & %s \\\\", lab, EFP_TEX[[e]], paste(cells, collapse = " & ")))
  }
}
tex <- c(tex, "\\bottomrule", "\\end{tabular}}", "\\end{table}")
out <- file.path(PAPER_ROOT, "manuscript/tables", sprintf("table_bootstrap_d%s_%s_%s%s%s.tex", MET, VTAG, paste(SHOW, collapse = ""), if (SY_ONLY) "_siteyear" else "", ISFX))
writeLines(tex, out); cat("wrote", out, "\n")
