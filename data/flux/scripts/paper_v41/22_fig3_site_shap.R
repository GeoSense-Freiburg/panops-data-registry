# PORT for the paper_v41 dataset (deadtrees v4.1, 106 sites): input/output paths and site-count labels only.
#!/usr/bin/env Rscript
# ============================================================================
# Coauthor draft — Fig 3, restructured: metric becomes the file-split axis,
# EFPs become the side-by-side panels (transpose of the previous 4-file,
# 3-panel layout: 4 files [one per EFP] x 3 panels [one per disturbance
# metric] -> 3 files [one per metric] x 4 panels [one per EFP]).
#
# Same visual design as plot_v10_site_shap_distmetrics.R, unchanged:
# per-site stacked driver-group composition (Climate/Traits/Disturbance) +
# one dot-column per panel giving that site's value of the fixed metric,
# IGBP on the right axis of the last panel. M4 only, both windows,
# XGBoost-Optuna (matches the figure as already captioned in the draft).
#
# 24m draws on the lag2-corrected true_24m data; 12m is unchanged.
# ============================================================================
suppressMessages({library(data.table); library(ggplot2); library(patchwork)})
setwd("/mnt/gsdata/projects/panops/panops-data-registry/data/flux")

source("scripts/paper_v41/paper_common.R")
OUT <- PAPER_FIG; dir.create(OUT, showWarnings = FALSE, recursive = TRUE)

args <- commandArgs(trailingOnly = TRUE)
LEARNER <- if (length(args) >= 1) args[1] else "XGB"   # XGB | LGB
stopifnot(LEARNER %in% c("XGB", "LGB"))
LEARNER_LAB <- c(XGB = "XGBoost--Optuna", LGB = "LightGBM--Optuna")[[LEARNER]]

shap_file <- file.path(PAPER_OUT, sprintf("%s_site_shap_M4.csv", LEARNER))
harm_file <- PAPER_DATA   # site-level metric values (max across a site's years); window-independent

GROUP_COLOURS <- c(Climate = "#4A90D9", Traits = "#3DBDAA", Disturbance = "#D4A017", Memory = "#9B5DE5")
GROUP_LEVELS  <- c("Climate", "Traits", "Disturbance", "Memory")
EFP_UNITS <- RESP_UNITS   # active response set (paper_common.R)
# env FIG3_LAYOUT = paper (default, 2026-10-05: 7 x 10 in print size, alternating site labels, as Fig 3b) | wide (old 20 in)
PAPER_LAYOUT <- Sys.getenv("FIG3_LAYOUT", "paper") == "paper"

shap_raw <- fread(shap_file)
shap_grp <- shap_raw[group %in% GROUP_LEVELS & model %in% c("M4_12m", "M4_24m") & response %in% EFP_ORDER,
                     .(grp_shap = sum(mean_abs_shap, na.rm = TRUE)),
                     by = .(model, response, test_site, group)]
site_tot <- shap_grp[, .(total = sum(grp_shap)), by = .(model, response, test_site)]
shap_grp <- merge(shap_grp, site_tot, by = c("model", "response", "test_site"))
shap_grp[, rel_shap := grp_shap / total]

dt_raw <- paper_class_data()   # <metric>_cls = class buffer (paper_common.R)
na_inf <- function(x) { x[!is.finite(x)] <- NA; x }
site_dist <- dt_raw[, .(abs_mort = na_inf(max(absolute_mortality_cls, na.rm = TRUE)),
                        rel_mort = na_inf(max(relative_mortality_cls, na.rm = TRUE)),
                        rel_dist = na_inf(max(relative_disturbance_cls, na.rm = TRUE))), by = SITE_ID]

nb_high <- function(x) mean(x, na.rm = TRUE) + 0.5 * sd(x, na.rm = TRUE)
NB <- list(abs_mort = nb_high(site_dist$abs_mort), rel_mort = nb_high(site_dist$rel_mort),
          rel_dist = nb_high(site_dist$rel_dist))

DIST_META <- list(
  abs_mort = list(col = "abs_mort", label = sprintf("Absolute Deadwood\n(%s, %%)  NB=%.1f", CLASS_LAB, NB$abs_mort), mid_v = NB$abs_mort),
  rel_mort = list(col = "rel_mort", label = sprintf("Relative Deadwood\n(%s, %%)  NB=%.1f", CLASS_LAB, NB$rel_mort), mid_v = NB$rel_mort),
  rel_dist = list(col = "rel_dist", label = sprintf("Total Mortality\n(%s, %%)  NB=%.1f", CLASS_LAB, NB$rel_dist), mid_v = NB$rel_dist)
)

shap_grp <- merge(shap_grp, site_dist, by.x = "test_site", by.y = "SITE_ID", all.x = TRUE)

igbp_src <- fread(PAPER_DATA, select = c("SITE_ID", "IGBP"))
igbp_map <- unique(igbp_src)[, .(IGBP = IGBP[1]), by = SITE_ID]

# one panel = one EFP's stacked composition + the FIXED metric's dot column
make_one_panel <- function(dt_sub, site_order, metric_key, igbp_vec, efp_label,
                           show_y = TRUE, show_igbp = FALSE) {
  meta <- DIST_META[[metric_key]]; col_var <- meta$col
  dt_plot <- copy(dt_sub); dt_plot[, test_site := factor(test_site, levels = site_order)]
  dot_dt <- unique(dt_plot[, c("test_site", col_var), with = FALSE])
  dot_dt[, test_site := factor(test_site, levels = site_order)]
  setnames(dot_dt, col_var, "metric_val")
  max_val <- ceiling(max(dot_dt$metric_val, na.rm = TRUE) / 5) * 5
  max_val <- max(max_val, meta$mid_v * 2)
  if (PAPER_LAYOUT) {   # print size: site labels alternate left / right as "SITE (IGBP)" (as Fig 3b)
    lab <- sprintf("%s (%s)", site_order, igbp_vec); odd <- seq_along(site_order) %% 2 == 1
    y_scale <- if (show_igbp) scale_y_discrete(labels = NULL, sec.axis = dup_axis(labels = ifelse(odd, "", lab), name = NULL))
               else if (show_y) scale_y_discrete(labels = ifelse(odd, lab, "")) else scale_y_discrete(labels = NULL)
  } else {
    y_scale <- if (show_igbp) scale_y_discrete(sec.axis = dup_axis(labels = igbp_vec, name = "IGBP")) else scale_y_discrete()
  }

  if (PAPER_LAYOUT) {   # dots only next to the site names: odd rows left of the first panel, even rows right of the last
    pos <- as.integer(dot_dt$test_site)
    dot_dt <- if (show_y) dot_dt[pos %% 2 == 1][, dx := -0.05] else if (show_igbp) dot_dt[pos %% 2 == 0][, dx := 1.06] else dot_dt[0][, dx := numeric(0)]
  } else dot_dt[, dx := -0.05]
  ggplot() +
    geom_col(data = dt_plot, aes(x = rel_shap, y = test_site, fill = group), position = "stack", width = 0.75) +
    geom_point(data = dot_dt, aes(x = dx, y = test_site, colour = metric_val, size = metric_val)) +
    scale_fill_manual(values = GROUP_COLOURS, labels = c(Climate = "Climate", Traits = "Traits", Disturbance = "Mortality", Memory = "Memory"), name = "Driver group", drop = TRUE) +
    (if (PAPER_LAYOUT)   # same scale as Fig 3b: dark teal (0) -> yellow (site mean) -> pink (max) (user, 2026-10-05)
       scale_colour_gradientn(colours = c("#004D4D", "#F0E442", "#EE6FA8"),
                              values = c(0, min(max(mean(site_dist[[col_var]], na.rm = TRUE) / max_val, 0.01), 0.99), 1),
                              limits = c(0, max_val), na.value = "grey80", name = meta$label)
     else scale_colour_gradient2(low = "#1B4965", mid = "#F5F5F5", high = "#C41E3A",
                                 midpoint = meta$mid_v, limits = c(0, max_val), na.value = "grey80", name = meta$label)) +
    scale_size_continuous(name = meta$label, range = if (PAPER_LAYOUT) c(0.3, 2.2) else c(0.5, 4.5), limits = c(0, max_val)) +
    y_scale +
    scale_x_continuous(limits = if (!PAPER_LAYOUT) c(-0.09, 1.02) else if (show_y) c(-0.09, 1.02) else if (show_igbp) c(0, 1.1) else c(0, 1.02), breaks = if (PAPER_LAYOUT) c(0, 0.5, 1) else seq(0, 1, 0.25),
                       labels = if (PAPER_LAYOUT) c("0", "0.5", "1") else c("0", "0.25", "0.50", "0.75", "1.00"),
                       expand = expansion(0)) +
    labs(x = "Relative mean |SHAP|", y = NULL, title = efp_label) +
    (if (PAPER_LAYOUT) list(theme_bw(base_size = 7), theme(
      plot.title = element_text(size = 7, face = "bold", hjust = 0.5),
      axis.text.y.left = if (show_y) element_text(size = 7, colour = "grey15") else element_blank(),
      axis.text.y.right = if (show_igbp) element_text(size = 7, colour = "grey15") else element_blank(),
      axis.ticks.y = element_blank(), axis.text.x = element_text(size = 6), axis.title.x = element_text(size = 6.5),
      legend.title = element_text(size = 8), legend.text = element_text(size = 7.5), legend.key.size = unit(0.42, "cm"),
      panel.grid.major.y = element_blank(), panel.grid.minor = element_blank(), plot.margin = margin(2, 2, 2, 2)),
      guides(fill = guide_legend(order = 1, override.aes = list(size = 3)),
             colour = guide_colourbar(order = 2, barwidth = unit(5, "cm"), barheight = unit(0.35, "cm"), title.position = "top"),
             size = "none"))
     else list(theme_bw(base_size = 9), theme(
      plot.title = element_text(size = 9, face = "bold", hjust = 0.5),
      axis.text.y = if (show_y) element_text(size = 6.5) else element_blank(),
      axis.ticks.y = if (show_y) element_line() else element_blank(),
      axis.text.y.right = if (show_igbp) element_text(size = 6, colour = "grey30") else element_blank(),
      legend.position = "right", legend.key.size = unit(0.4, "cm"),
      legend.title = element_text(size = 7.5), legend.text = element_text(size = 7),
      panel.grid.major.y = element_blank(), panel.grid.minor = element_blank()
    ),
    guides(fill = guide_legend(order = 1, override.aes = list(size = 3.5)),
          colour = guide_colourbar(order = 2, barheight = unit(2.5, "cm")), size = "none")))
}

make_combined_plot <- function(dt_model, metric_key, model_label) {
  # shared site order across all 4 EFP panels: mean disturbance SHAP share,
  # pooled across the 4 EFPs for this model/window (same principle as the
  # production script's per-EFP ordering, extended to be shared across panels)
  site_order <- dt_model[group == "Disturbance", .(dist_share = mean(rel_shap)), by = test_site
                         ][order(-dist_share), test_site]
  igbp_vec <- igbp_map[match(site_order, SITE_ID), IGBP]; igbp_vec[is.na(igbp_vec)] <- "NA"

  panels <- lapply(seq_along(EFP_ORDER), function(i) {
    efp <- EFP_ORDER[i]
    dt_sub <- dt_model[response == efp]
    dt_sub[, group := factor(group, levels = GROUP_LEVELS)]
    make_one_panel(dt_sub, site_order, metric_key, igbp_vec, EFP_UNITS[efp],
                   show_y = (i == 1), show_igbp = (i == length(EFP_ORDER)))
  })

  metric_title <- strsplit(DIST_META[[metric_key]]$label, "\n")[[1]][1]
  title_str <- sprintf("%s  |  %s", model_label, metric_title)

  Reduce(`|`, panels) +
    plot_annotation(title = title_str,
                    theme = theme(plot.title = element_text(face = "bold", size = if (PAPER_LAYOUT) 8 else 11, hjust = 0.5))) +
    plot_layout(guides = "collect", axis_titles = if (PAPER_LAYOUT) "collect_x" else "keep") &
    theme(legend.position = if (PAPER_LAYOUT) "bottom" else "right")
}

MODEL_LABELS <- list(M4_12m = sprintf("C+T+M, 12m window, %s", LEARNER_LAB),
                     M4_24m = sprintf("C+T+M, 24m window, %s", LEARNER_LAB))

cat(sprintf("Generating metric-keyed Fig 3 panels (M4, %s, both windows):\n", LEARNER_LAB))
for (metric_key in names(DIST_META)) {
  for (this_model in names(MODEL_LABELS)) {
    win <- if (grepl("12m$", this_model)) "12m" else "24m"
    dt_model <- shap_grp[model == this_model]
    if (nrow(dt_model) == 0) { cat("  No data:", this_model, "\n"); next }

    p <- make_combined_plot(dt_model, metric_key, MODEL_LABELS[[this_model]])
    n_sites <- uniqueN(dt_model$test_site)
    stem <- file.path(OUT, sprintf("fig3_%s_%s_%s%s", FILE_KEY[[metric_key]], win, LEARNER, CLASS_TAG))
    if (PAPER_LAYOUT) {
      h <- min(10, max(6, n_sites * 0.082 + 1.4))
      ggsave(paste0(stem, ".png"), p, width = 7, height = h, dpi = 300)
      ggsave(paste0(stem, ".pdf"), p, width = 7, height = h, device = cairo_pdf)
    } else {
      h <- max(6, n_sites * 0.17 + 2)
      ggsave(paste0(stem, ".png"), p, width = 20, height = h, dpi = 150, limitsize = FALSE)
      ggsave(paste0(stem, ".pdf"), p, width = 20, height = h, limitsize = FALSE)
    }
    cat(sprintf("  OK %s (%d sites)\n", basename(stem), n_sites))
  }
}
cat("\nFig 3 (by metric) done.\n")
