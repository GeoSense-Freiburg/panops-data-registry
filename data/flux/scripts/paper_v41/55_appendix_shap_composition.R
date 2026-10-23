#!/usr/bin/env Rscript
# ============================================================================
# Appendix figure: driver composition of the predictions per site (relative mean |SHAP| of climate, traits and
# mortality), AutoGluon, C+T+M, 12 and 24 months, in the Nature style of the main figures (fig_style.R), 2026-10-23.
# Redraws the "B" (|SHAP| composition) figure of 32_fig3_landscape.R (v4.1, old style) with the V5 maps.
# Sites sorted by their mean mortality share over the four EFPs (highest left); the strip below the bars shows the
# site's maximum relative mortality in the PiYG colours and class breaks of Fig. 3.
# Usage: Rscript 55_appendix_shap_composition.R        Output: plots/paper_v5_nothresh/nature/appendix/
#   shap_composition_AG_12m, shap_composition_AG_24m  (+ shap_composition_AG.csv, the numbers behind both)
# ============================================================================
suppressMessages({library(data.table); library(ggplot2); library(patchwork)})
ROOT <- Sys.getenv("PAPER_ROOT", "/mnt/gsdata/projects/panops/panops-data-registry/data/flux"); setwd(ROOT)
source("scripts/paper_v41/fig_style.R")
OUT <- "plots/paper_v5_nothresh/nature/appendix"; dir.create(OUT, showWarnings = FALSE, recursive = TRUE)
EFP <- EFP_ORDER_FIG
GROUP_COLS <- c(Climate = OKABE[["skyblue"]], Traits = OKABE[["green"]], Mortality = OKABE[["orange"]])

# relative mortality per site (maximum over years of the five-buffer mean) and the Low / Mid / High breaks, as in 50/51
ds <- fread("derived_tables/paper_v5/paper_dataset_full_lags_monthly_tc30_minrun2_buf100-200-300-400-500.csv")
ds[, tm := rowMeans(.SD), .SDcols = paste0("relative_disturbance_", c(100, 200, 300, 400, 500), "m")]
site <- ds[, .(tm_max = max(tm)), by = SITE_ID]
B_LO <- max(mean(site$tm_max) - 0.5 * sd(site$tm_max), 0); B_HI <- mean(site$tm_max) + 0.5 * sd(site$tm_max)

sh <- fread("AutoGluon/outputs/efp_v5_nothresh/AG_site_shap_M4.csv")[response %in% EFP]
sh[, grp := fifelse(group == "Disturbance", "Mortality", group)]
sh <- sh[, .(g = sum(mean_abs_shap, na.rm = TRUE)), by = .(window = sub("M4_", "", model), response, SITE_ID = test_site, grp)]
sh[, rel := g / sum(g), by = .(window, response, SITE_ID)]
fwrite(dcast(sh, window + response + SITE_ID ~ grp, value.var = "rel"), file.path(OUT, "shap_composition_AG.csv"))

for (WIN in c("12m", "24m")) {
  d <- sh[window == WIN]
  ord <- d[grp == "Mortality", .(m = mean(rel)), by = SITE_ID][order(-m), SITE_ID]
  d[, `:=`(x = match(SITE_ID, ord), grp = factor(grp, rev(names(GROUP_COLS))))]
  bars <- lapply(EFP, function(e) ggplot(d[response == e], aes(x, rel, fill = grp)) +
    geom_col(width = 0.8) +
    scale_fill_manual(values = GROUP_COLS, breaks = names(GROUP_COLS), name = "Driver group") +
    scale_x_continuous(expand = expansion(add = 0.6), breaks = NULL) +
    scale_y_continuous(breaks = c(0, 0.5, 1), labels = c("0", "0.5", "1"), expand = expansion(0)) +
    labs(x = NULL, y = parse(text = sprintf("atop(%s, Relative~'|SHAP|')", EFP_SHORT[[e]]))) +
    theme_pub() + theme(axis.line.x = element_blank()))
  st <- site[SITE_ID %in% ord][, x := match(SITE_ID, ord)]
  strip <- ggplot(st, aes(x, 1, fill = tm_max)) + geom_tile(width = 0.8, height = 1) +
    scale_fill_mortality_piyg(b_lo = B_LO, b_hi = B_HI) +
    scale_x_continuous(expand = expansion(add = 0.6), breaks = st$x, labels = st$SITE_ID) +
    scale_y_continuous(expand = expansion(0)) +
    labs(x = "Sites, ordered by the mortality share of the predictions (highest left)", y = NULL) + theme_pub() +
    theme(axis.text.x = element_text(size = 5, angle = 90, vjust = 0.5, hjust = 1), axis.ticks.x = element_line(linewidth = 0.2),
          axis.text.y = element_blank(), axis.ticks.y = element_blank(), axis.line = element_blank())
  f <- wrap_plots(c(bars, list(strip)), ncol = 1, heights = c(rep(1, 4), 0.1), guides = "collect") +
    plot_annotation(tag_levels = list(c(letters[1:4], ""))) &
    theme(legend.position = "bottom", legend.title.position = "top", legend.key.height = unit(2, "mm"),
          legend.key.width = unit(6, "mm"), legend.box.spacing = unit(0.5, "mm"))   # legends below: the 101 site codes need the full width
  save_pub(f, file.path(OUT, sprintf("shap_composition_AG_%s", WIN)), W2, 150)
}
cat("written:", file.path(OUT, "shap_composition_AG_{12m,24m}"), "\n")
