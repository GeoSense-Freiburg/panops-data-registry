#!/usr/bin/env Rscript
# ============================================================================
# LaTeX table of the paired bootstrap / block permutation (39_paired_bootstrap.R), 2026-10-09.
# Columns: site level (all sites) and site-year level (all sites, High sites, site-years with Mid+High and with
# High relative mortality in that year). Cells: % change of the error [95 % bootstrap interval], stars = paired
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
# BH correction over the tests of the shown learners only (2026-10-20: the draft reports AutoGluon only), per level
st <- st[learner %in% SHOW][, q := p.adjust(p_perm, "BH"), by = .(level)]
st[, sig := fifelse(q < 0.001, "***", fifelse(q < 0.01, "**", fifelse(q < 0.05, "*", "")))]
LAB <- c(XGB = "XGBoost", LGB = "LightGBM", AG = "AutoGluon", AGx = "AutoGluon, EFP-specific climate")
EFP_TEX <- c(GPPsat = "GPP$_\\mathrm{sat}$", NEPmax = "NEP$_\\mathrm{max}$", ETmax = "ET$_\\mathrm{max}$", WUE = "WUE")
COLS <- list(c("site", "all"), c("siteyear", "all"), c("siteyear", "midhigh"), c("siteyear", "high"), c("siteyear", "yr_midhigh"),
             c("siteyear", "yr_high"))   # midhigh = all years of the Mid + High sites (added 2026-10-15)
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
  sprintf(paste0("\\caption{\\textbf{Adding mortality reduces the prediction error (%s), most in the years in which trees died.} ",
    "Change in the %s when the mortality predictors are added to climate and traits (C+T $\\rightarrow$ C+T+M), %s, ",
    "leave-one-site-out predictions: 100\\,(%s$_\\mathrm{C+T+M}$ / %s$_\\mathrm{C+T}$ $-$ 1)\\,\\%%; negative = smaller error. ",
    "Brackets: 95\\,\\%% interval of a bootstrap that resamples whole sites (10\\,000 draws). ",
    "Stars: one-sided paired permutation test with the two models swapped per site (10\\,000 draws), ",
    if (AG_EXTRA != "") "Benjamini--Hochberg corrected across both runs: " else "Benjamini--Hochberg corrected across all tests of the same level: ",
    "$^{*}$\\,$q<0.05$, $^{**}$\\,$q<0.01$, $^{***}$\\,$q<0.001$. ",
    if (SY_ONLY) "" else "\\emph{Site level}: one error per site (mean over its years). ",
    "\\emph{Site-year level}: every site-year is one observation. ",
    "\\emph{All years of the sites}: all sites, or only the sites whose maximum relative mortality exceeds %.1f\\,\\%% (Mid + High) ",
    "or %.1f\\,\\%% (High), with all their years. \\emph{Only the years with mortality}: site-years whose own relative mortality ",
    "exceeds %.1f\\,\\%% or %.1f\\,\\%%. Numbers under the column heads: sites / site-years. ",
    "%d sites, %d site-years, deadtrees v5 maps.}"),
    MET, MET, paste(LAB[SHOW], collapse = " and "), MET, MET, b$b_lo, b$b_hi, b$b_lo, b$b_hi, max(st$n_units), max(st$n_obs)),
  sprintf("\\label{tab:bootstrap_d%s_%s}", MET, paste(SHOW, collapse = "")),
  "\\scriptsize", "\\setlength{\\tabcolsep}{3pt}",
  "\\resizebox{\\linewidth}{!}{%",
  if (SY_ONLY) c("\\begin{tabular}{llccccc}", "\\toprule",
    " & & \\multicolumn{3}{c}{All years of the sites} & \\multicolumn{2}{c}{Only the years with mortality} \\\\",
    "\\cmidrule(lr){3-5}\\cmidrule(lr){6-7}",
    sprintf(" & & All sites & Mid + High sites & High sites & $>$ %.1f\\,\\%% & $>$ %.1f\\,\\%% \\\\", b$b_lo, b$b_hi)) else
  c("\\begin{tabular}{llcccccc}", "\\toprule",
    " & & Site level & \\multicolumn{5}{c}{Site-year level} \\\\", "\\cmidrule(lr){3-3}\\cmidrule(lr){4-8}",
    " & & & \\multicolumn{3}{c}{All years of the sites} & \\multicolumn{2}{c}{Only the years with mortality} \\\\",
    "\\cmidrule(lr){4-6}\\cmidrule(lr){7-8}",
    sprintf(" & & All sites & All sites & Mid + High sites & High sites & $>$ %.1f\\,\\%% & $>$ %.1f\\,\\%% \\\\", b$b_lo, b$b_hi)),
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
