#!/usr/bin/env Rscript
# ============================================================================
# Site-specific delta RMSE, M3 -> M4 (diverging bars, layout of the V10 report section 5,
# plot_v10_RMSE_delta_bars.R), for the active dataset (PAPER_RESP_SET):
#   dRMSE = RMSE(M4, with mortality) - RMSE(M3, without) per site, repeated CV (3 x 80 %);
#   one bar per site, sorted by dRMSE within each panel; below 0 = mortality reduces the site's error.
#   rows = responses, columns = learner x window (XGBoost / LightGBM Optuna, 12m / 24m).
# Bars are coloured by the site's disturbance intensity (site maximum across years, 500 m) on the
# Fig 3b scale: dark teal (0) -> yellow (site mean) -> pink (max), colour-vision-deficiency checked
# (see 25_fig3b_site_signed_shap.R). One figure per intensity metric:
#   fig_site_dRMSE_reldist.png / .pdf   relative disturbance
#   fig_site_dRMSE_relmort.png / .pdf   relative mortality
#   fig_site_dRMSE_statistics.csv       % sites improved and median dRMSE per panel, plus Spearman rho
#                                       between dRMSE and each intensity metric
#   Rscript 29_site_delta_rmse.R
# ============================================================================
suppressMessages({library(data.table); library(ggplot2)})
setwd("/mnt/gsdata/projects/panops/panops-data-registry/data/flux")
source("scripts/paper_v41/paper_common.R")
OUT <- PAPER_FIG; dir.create(OUT, showWarnings = FALSE, recursive = TRUE)
PN <- paper_n()

SCALE_COLS <- c("#004D4D", "#F0E442", "#EE6FA8")   # dark teal -> yellow -> pink (as Fig 3b)
GRID <- "#D9D9D9"; TXT <- "#111111"; AX <- "#444444"
LEARNERS <- c(XGB = "XGBoost (Optuna)", LGB = "LightGBM (Optuna)")
METRICS <- list(reldist = list(col = "relative_disturbance_500m", lab = "Relative disturbance"),
                relmort = list(col = "relative_mortality_500m",   lab = "Relative mortality"))

# per-site RMSE of M3 and M4 -> dRMSE
d <- rbindlist(lapply(names(LEARNERS), function(L) {
  p <- fread(file.path(PAPER_OUT, sprintf("%s_predictions_repCV.csv", L)))[response %in% EFP_ORDER & grepl("^M[34]_(12m|24m)$", model)]
  r <- p[, .(rmse = sqrt(mean((observed - predicted)^2))), by = .(model, response, SITE_ID)]
  r[, c("mod", "win") := tstrsplit(model, "_")]
  w <- dcast(r, response + win + SITE_ID ~ mod, value.var = "rmse")
  w[is.finite(M3) & is.finite(M4), .(response, win, SITE_ID, dRMSE = M4 - M3, learner = L)]
}))
d[, col := factor(paste0(LEARNERS[learner], " · ", win), levels = c(outer(c("12m", "24m"), LEARNERS, function(w, l) paste0(l, " · ", w))))]
d[, response := factor(response, levels = EFP_ORDER)]

# site intensity = maximum across the site's years
raw <- fread(PAPER_DATA, select = c("SITE_ID", unname(sapply(METRICS, `[[`, "col"))))
na_inf <- function(x) { x[!is.finite(x)] <- NA; x }
site <- raw[, lapply(.SD, function(x) na_inf(suppressWarnings(max(x, na.rm = TRUE)))), by = SITE_ID]

th <- theme_bw(base_size = 11) + theme(
  panel.border = element_rect(colour = GRID, fill = NA, linewidth = 0.4),
  panel.grid.major.x = element_blank(), panel.grid.minor = element_blank(),
  panel.grid.major.y = element_line(colour = GRID, linewidth = 0.2),
  strip.background = element_rect(fill = "#EFEFEF", colour = GRID),
  strip.text = element_text(colour = TXT, size = 10, face = "bold"),
  strip.placement = "outside",
  axis.text.x = element_blank(), axis.ticks.x = element_blank(),
  axis.text.y = element_text(colour = AX, size = 9),
  axis.title = element_text(colour = AX, size = 10, face = "bold"),
  legend.position = "right", legend.title = element_text(size = 9), legend.text = element_text(size = 8),
  plot.title = element_text(colour = TXT, size = 14, face = "bold"),
  plot.subtitle = element_text(colour = AX, size = 9))

stats <- list()
for (k in names(METRICS)) {
  M <- METRICS[[k]]
  x <- merge(d, site[, .(SITE_ID, v = get(M$col))], by = "SITE_ID")
  # bar position = rank of dRMSE within each panel
  x[, pos := frank(dRMSE, ties.method = "first"), by = .(response, col)]
  mu <- mean(site[[M$col]], na.rm = TRUE); sdv <- sd(site[[M$col]], na.rm = TRUE)
  max_val <- max(ceiling(max(site[[M$col]], na.rm = TRUE) / 5) * 5, (mu + 0.5 * sdv) * 2)
  lab <- x[, .(txt = sprintf("%.0f%% improved", 100 * mean(dRMSE < 0)), ytop = max(dRMSE)), by = .(response, col)]
  lab[, ytop := max(ytop), by = response]
  st <- x[, .(n_sites = .N, pct_improved = round(100 * mean(dRMSE < 0), 1), median_dRMSE = signif(median(dRMSE), 3),
              rho = round(suppressWarnings(cor(dRMSE, v, method = "spearman", use = "complete.obs")), 3),
              p_rho = signif(suppressWarnings(cor.test(dRMSE, v, method = "spearman", exact = FALSE)$p.value), 3)),
          by = .(response, learner, window = win)]
  st[, metric := M$lab]; stats[[k]] <- st

  p <- ggplot(x, aes(pos, dRMSE, fill = v)) +
    geom_col(width = 1, colour = NA) +
    geom_hline(yintercept = 0, colour = "#555555", linewidth = 0.3) +
    geom_text(data = lab, aes(x = 1, y = ytop, label = txt), inherit.aes = FALSE, hjust = 0, vjust = 1,
              size = 2.9, colour = AX) +
    scale_fill_gradientn(colours = SCALE_COLS, values = c(0, min(max(mu / max_val, 0.01), 0.99), 1),
                         limits = c(0, max_val), na.value = "grey80",
                         name = sprintf("%s (%%)\nsite maximum, 500 m\nbreaks %.1f | %.1f", M$lab, max(mu - 0.5 * sdv, 0), mu + 0.5 * sdv)) +
    facet_grid(response ~ col, scales = "free_y", switch = "y", labeller = labeller(response = RESP_UNITS)) +
    labs(x = "Sites (sorted by ΔRMSE within each panel)", y = "ΔRMSE  =  RMSE(with D) − RMSE(without D)",
         title = "Per-site ΔRMSE from adding disturbance — M3 (C+T) vs M4 (C+T+D)",
         subtitle = sprintf("Below 0 = D reduces the site's RMSE | bars coloured by %s (site maximum) | repeated CV (3×80 %%) | tree cover ≥30 %% (%d sites, %d site-years) · %s",
                            tolower(M$lab), PN[["sites"]], PN[["sy"]], PAPER_MAPS)) +
    th + guides(fill = guide_colourbar(barheight = unit(4, "cm")))
  stem <- file.path(OUT, sprintf("fig_site_dRMSE_%s", k))
  ggsave(paste0(stem, ".png"), p, width = 15, height = 11, dpi = 200, bg = "white")
  ggsave(paste0(stem, ".pdf"), p, width = 15, height = 11, bg = "white", device = cairo_pdf)
  cat(sprintf("  OK %s (%d sites)\n", basename(stem), uniqueN(x$SITE_ID)))
}
fwrite(rbindlist(stats), file.path(OUT, "fig_site_dRMSE_statistics.csv"))
cat("site dRMSE figure done.\n")
