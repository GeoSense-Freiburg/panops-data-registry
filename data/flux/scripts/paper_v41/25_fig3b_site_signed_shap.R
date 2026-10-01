#!/usr/bin/env Rscript
# ============================================================================
# Fig 3b — variant of Fig 3 (22_fig3_site_shap.R): per site, the NET SIGNED
# disturbance-block SHAP (same quantity as Fig 4 and Fig 5, response units)
# instead of the relative mean |SHAP| composition. Bars are filled with the
# site's disturbance intensity. Sites sorted by intensity, shared across the 4
# EFP panels; dashed lines = the two natural breaks, mean -/+ 0.5 SD over sites
# (Low below the lower, Mid between, High above the upper).
# Colour scale: dark teal (0) -> yellow (site mean = centre of Mid) -> pink (max).
# Anchors chosen for colour-vision deficiency: pairwise CIEDE2000 >= 26 under
# simulated deuteranopia, protanopia and tritanopia (colorspace), and distinct
# in lightness (L* 29 / 89 / 64), so the scale also reads in greyscale.
# Usage: Rscript 25_fig3b_site_signed_shap.R <XGB|LGB>
#   env FIG3B_METRICS = comma list of abs_mort,rel_mort,rel_dist (default all)
# ============================================================================
suppressMessages({library(data.table); library(ggplot2); library(patchwork)})
setwd("/mnt/gsdata/projects/panops/panops-data-registry/data/flux")

source("scripts/paper_v41/paper_common.R")
OUT <- PAPER_FIG; dir.create(OUT, showWarnings = FALSE, recursive = TRUE)

args <- commandArgs(trailingOnly = TRUE)
LEARNER <- if (length(args) >= 1) args[1] else "XGB"
stopifnot(LEARNER %in% c("XGB", "LGB"))
LEARNER_LAB <- c(XGB = "XGBoost--Optuna", LGB = "LightGBM--Optuna")[[LEARNER]]

EFP_UNITS <- RESP_UNITS   # active response set (paper_common.R)

signed <- fread(file.path(PAPER_OUT, sprintf("%s_site_signed_shap_M4.csv", LEARNER)))[
  group == "Disturbance" & model %in% c("M4_12m", "M4_24m") & response %in% EFP_ORDER,
  .(model, response, test_site, dist_signed = mean_signed_shap)]

# site-level metric value = max across the site's years (same as Fig 3 / Fig 4)
dt_raw <- fread(PAPER_DATA, select = c("SITE_ID", "IGBP", "absolute_mortality_500m",
                                        "relative_mortality_500m", "relative_disturbance_500m"))
na_inf <- function(x) { x[!is.finite(x)] <- NA; x }
site_dist <- dt_raw[, .(IGBP = IGBP[1],
                        abs_mort = na_inf(max(absolute_mortality_500m, na.rm = TRUE)),
                        rel_mort = na_inf(max(relative_mortality_500m, na.rm = TRUE)),
                        rel_dist = na_inf(max(relative_disturbance_500m, na.rm = TRUE))), by = SITE_ID]

SCALE_COLS <- c("#004D4D", "#F0E442", "#EE6FA8")   # dark teal -> yellow -> pink
DIST_META <- list(
  abs_mort = list(label = "Absolute Mortality\n(500m, %)"),
  rel_mort = list(label = "Relative Mortality\n(%)"),
  rel_dist = list(label = "Relative Disturbance\n(%)"))
DIST_META <- DIST_META[intersect(names(DIST_META), strsplit(Sys.getenv("FIG3B_METRICS", "abs_mort,rel_mort,rel_dist"), ",")[[1]])]
for (k in names(DIST_META)) {     # natural breaks: Low < mean - 0.5 SD <= Mid <= mean + 0.5 SD < High
  x <- site_dist[[k]]
  DIST_META[[k]]$mean <- mean(x, na.rm = TRUE)
  DIST_META[[k]]$nb_low <- mean(x, na.rm = TRUE) - 0.5 * sd(x, na.rm = TRUE)
  DIST_META[[k]]$nb <- mean(x, na.rm = TRUE) + 0.5 * sd(x, na.rm = TRUE)
}

signed <- merge(signed, site_dist, by.x = "test_site", by.y = "SITE_ID", all.x = TRUE)

make_one_panel <- function(dt_sub, site_order, metric_key, igbp_vec, efp_label, max_val, n_high, n_low,
                           show_y = TRUE, show_igbp = FALSE) {
  meta <- DIST_META[[metric_key]]
  d <- copy(dt_sub)
  d[, metric_val := get(metric_key)]
  d[, test_site := factor(test_site, levels = site_order)]
  y_scale <- if (show_igbp) scale_y_discrete(sec.axis = dup_axis(labels = igbp_vec, name = "IGBP"))
             else scale_y_discrete()
  # sites run bottom -> top in factor order: Low = first n_low levels, High = last n_high levels
  nb_lines <- c(if (n_low > 0) n_low + 0.5, length(site_order) - n_high + 0.5)

  ggplot(d, aes(x = dist_signed, y = test_site, fill = metric_val)) +
    geom_vline(xintercept = 0, colour = "grey40", linewidth = 0.3) +
    geom_hline(yintercept = nb_lines, linetype = "dashed", colour = "grey20", linewidth = 0.4) +
    geom_col(width = 0.75, colour = "grey55", linewidth = 0.1) +
    scale_fill_gradientn(colours = SCALE_COLS, values = c(0, min(max(meta$mean / max_val, 0.01), 0.99), 1),
                         limits = c(0, max_val), na.value = "grey80",
                         name = sprintf("%s\nbreaks %.1f | %.1f", meta$label, max(meta$nb_low, 0), meta$nb)) +
    y_scale +
    labs(x = "Net signed disturbance SHAP\n(response units)", y = NULL, title = efp_label) +
    theme_bw(base_size = 9) +
    theme(
      plot.title = element_text(size = 9, face = "bold", hjust = 0.5),
      axis.text.y = if (show_y) element_text(size = 6.5) else element_blank(),
      axis.ticks.y = if (show_y) element_line() else element_blank(),
      axis.text.y.right = if (show_igbp) element_text(size = 6, colour = "grey30") else element_blank(),
      legend.position = "right", legend.key.size = unit(0.4, "cm"),
      legend.title = element_text(size = 7.5), legend.text = element_text(size = 7),
      panel.grid.major.y = element_blank(), panel.grid.minor = element_blank()
    ) +
    guides(fill = guide_colourbar(barheight = unit(2.5, "cm")))
}

MODEL_LABELS <- list(M4_12m = sprintf("M4 (C+T+D), 12m window, %s", LEARNER_LAB),
                     M4_24m = sprintf("M4 (C+T+D), 24m window, %s", LEARNER_LAB))

cat(sprintf("Fig 3b signed disturbance SHAP (%s):\n", LEARNER_LAB))
for (metric_key in names(DIST_META)) {
  for (this_model in names(MODEL_LABELS)) {
    win <- sub("M4_", "", this_model)
    dt_model <- signed[model == this_model]
    if (nrow(dt_model) == 0) { cat("  No data:", this_model, "\n"); next }

    sites <- unique(dt_model[, .(test_site, v = get(metric_key), IGBP)])
    setorder(sites, v, na.last = FALSE)            # lowest at bottom, highest at top
    site_order <- sites$test_site
    igbp_vec <- sites$IGBP; igbp_vec[is.na(igbp_vec)] <- "NA"
    nb <- DIST_META[[metric_key]]$nb
    n_high <- sum(sites$v > nb, na.rm = TRUE)
    n_low <- sum(sites$v < DIST_META[[metric_key]]$nb_low, na.rm = TRUE)
    n_mid <- nrow(sites) - n_high - n_low
    max_val <- max(ceiling(max(sites$v, na.rm = TRUE) / 5) * 5, nb * 2)

    panels <- lapply(seq_along(EFP_ORDER), function(i) {
      efp <- EFP_ORDER[i]
      make_one_panel(dt_model[response == efp], site_order, metric_key, igbp_vec, EFP_UNITS[efp],
                     max_val, n_high, n_low, show_y = (i == 1), show_igbp = (i == length(EFP_ORDER)))
    })
    metric_title <- strsplit(DIST_META[[metric_key]]$label, "\n")[[1]][1]
    p <- Reduce(`|`, panels) +
      plot_annotation(
        title = sprintf("%s  |  %s", MODEL_LABELS[[this_model]], metric_title),
        subtitle = sprintf("Sites sorted by site-maximum %s; dashed lines = natural breaks, mean \u00b1 0.5 SD (top to bottom: %d High, %d Mid, %d Low)",
                           tolower(metric_title), n_high, n_mid, n_low),
        theme = theme(plot.title = element_text(face = "bold", size = 11, hjust = 0.5),
                      plot.subtitle = element_text(size = 8.5, hjust = 0.5))) +
      plot_layout(guides = "collect") & theme(legend.position = "right")

    h <- max(6, nrow(sites) * 0.17 + 2)
    stem <- file.path(OUT, sprintf("fig3b_signed_%s_%s_%s", metric_key, win, LEARNER))
    ggsave(paste0(stem, ".png"), p, width = 20, height = h, dpi = 150, limitsize = FALSE)
    ggsave(paste0(stem, ".pdf"), p, width = 20, height = h, limitsize = FALSE, device = cairo_pdf)
    cat(sprintf("  OK %s (%d sites: %d High, %d Mid, %d Low)\n", basename(stem), nrow(sites), n_high, n_mid, n_low))
  }
}
cat("\nFig 3b done.\n")
