#!/usr/bin/env Rscript
# ============================================================================
# Fig 3, alternative LANDSCAPE layout (2026-10-05, offered as an option to the portrait figures of
# 22_fig3_site_shap.R and 25_fig3b_site_signed_shap.R): sites run along the x axis, the four EFPs are
# stacked as rows that share that axis, and every site name is written ONCE, vertically, under the
# bottom row - no alternation, and a site is found by reading straight up its column. Meant for a
# rotated (sideways) page.
#   fig3b_signed_<metric>_<window>_<L>_landscape   net signed mortality SHAP per site (as Fig 3b);
#                                                  bars coloured by the metric, dashed lines = Low/Mid/High breaks
#   fig3_<metric>_<window>_<L>_landscape           relative |SHAP| composition per site (as Fig 3);
#                                                  a coloured strip under the bars shows the metric (Fig 3b scale)
#   Rscript 32_fig3_landscape.R <XGB|LGB|AG> [window = 24m | all] [metric = rel_dist | all]
#   (AG: set PAPER_OUT_DIR / PAPER_FIG_DIR to the AutoGluon staging / plot folders, as 05_figures.sh does)
# ============================================================================
suppressMessages({library(data.table); library(ggplot2); library(patchwork)})
setwd("/mnt/gsdata/projects/panops/panops-data-registry/data/flux")
source("scripts/paper_v41/paper_common.R")
args <- commandArgs(trailingOnly = TRUE)
LEARNER <- if (length(args) >= 1) args[1] else "LGB"
WINS    <- if (length(args) >= 2 && args[2] != "all") args[2] else c("12m", "24m")
MKEYS   <- if (length(args) >= 3 && args[3] != "all") args[3] else c("rel_dist", "rel_mort", "abs_mort")
for (WIN in WINS) for (MKEY in MKEYS) {
LEARNER_LAB <- c(XGB = "XGBoost--Optuna", LGB = "LightGBM--Optuna", AG = "AutoGluon")[[LEARNER]]
OUT <- PAPER_FIG
MODEL <- paste0("M4_", WIN)
SCALE_COLS <- c("#004D4D", "#F0E442", "#EE6FA8")   # dark teal -> yellow (site mean) -> pink, as Fig 3b
GROUP_COLOURS <- c(Climate = "#4A90D9", Traits = "#3DBDAA", Disturbance = "#D4A017")
METRIC_LAB <- c(abs_mort = "Absolute deadwood", rel_mort = "Relative deadwood", rel_dist = "Total mortality")[[MKEY]]

# site metric (max over years, class buffer) and IGBP
na_inf <- function(x) { x[!is.finite(x)] <- NA; x }
site <- paper_class_data("IGBP")[, .(IGBP = IGBP[1],
          abs_mort = na_inf(max(absolute_mortality_cls, na.rm = TRUE)),
          rel_mort = na_inf(max(relative_mortality_cls, na.rm = TRUE)),
          rel_dist = na_inf(max(relative_disturbance_cls, na.rm = TRUE))), by = SITE_ID]
site[, v := get(MKEY)]
mu <- mean(site$v, na.rm = TRUE); s <- sd(site$v, na.rm = TRUE); b_lo <- max(mu - 0.5 * s, 0); b_hi <- mu + 0.5 * s
max_val <- max(ceiling(max(site$v, na.rm = TRUE) / 5) * 5, b_hi * 2)
fill_scale <- function(aes_name)
  get(paste0("scale_", aes_name, "_gradientn"))(colours = SCALE_COLS, values = c(0, min(max(mu / max_val, 0.01), 0.99), 1),
      limits = c(0, max_val), na.value = "grey80",
      name = sprintf("%s (%s, %%)   breaks %.1f | %.1f", METRIC_LAB, CLASS_LAB, b_lo, b_hi))

th <- theme_bw(base_size = 7) + theme(
  panel.grid.minor = element_blank(), panel.grid.major.x = element_blank(),
  panel.grid.major.y = element_line(colour = "grey90", linewidth = 0.15),
  strip.background = element_rect(fill = "#EFEFEF", colour = "grey80"),
  strip.text.y = element_text(size = 6.5, face = "bold"), strip.placement = "outside",
  axis.text.x = element_text(size = 6, angle = 90, hjust = 1, vjust = 0.5, colour = "grey15"),
  axis.ticks.x = element_blank(), axis.text.y = element_text(size = 6),
  axis.title = element_text(size = 7), legend.position = "bottom",
  legend.title = element_text(size = 7.5), legend.text = element_text(size = 7),
  plot.title = element_text(size = 9, face = "bold", hjust = 0.5),
  plot.subtitle = element_text(size = 6.5, hjust = 0.5), plot.margin = margin(4, 4, 4, 4))
lab_of <- function(ids) setNames(ids, ids)   # site code only (user, 2026-10-05: no IGBP)
EFP_LAB <- setNames(sub("  ", "\n", RESP_UNITS[EFP_ORDER]), EFP_ORDER)

# ---- A. signed mortality SHAP (as Fig 3b) ----------------------------------------------
sg <- fread(file.path(PAPER_OUT, sprintf("%s_site_signed_shap_M4.csv", LEARNER)))[
  group == "Disturbance" & model == MODEL & response %in% EFP_ORDER, .(response, test_site, val = mean_signed_shap)]
ord <- site[SITE_ID %in% sg$test_site][order(-v), SITE_ID]          # highest mortality on the left
sg <- merge(sg, site[, .(test_site = SITE_ID, v)], by = "test_site")
sg[, `:=`(test_site = factor(test_site, levels = ord), response = factor(response, levels = EFP_ORDER))]
n_high <- sum(site[SITE_ID %in% ord]$v > b_hi); n_low <- sum(site[SITE_ID %in% ord]$v < b_lo)
brk <- c(n_high + 0.5, length(ord) - n_low + 0.5)
pA <- ggplot(sg, aes(test_site, val, fill = v)) +
  geom_hline(yintercept = 0, colour = "grey40", linewidth = 0.25) +
  geom_vline(xintercept = brk, linetype = "dashed", colour = "grey20", linewidth = 0.35) +
  geom_col(width = 0.78, colour = NA) +
  fill_scale("fill") +
  scale_x_discrete(labels = lab_of(ord)) +
  scale_y_continuous(breaks = scales::breaks_pretty(n = 3), labels = scales::label_number(drop0trailing = TRUE)) +
  facet_grid(response ~ ., scales = "free_y", labeller = labeller(response = EFP_LAB)) +   # EFP names on the right, SHAP axis title on the left
  labs(x = NULL, y = "Mortality SHAP (response units)",
       title = sprintf("C+T+M, %s window, %s  |  %s", WIN, LEARNER_LAB, METRIC_LAB),
       subtitle = sprintf("Sites sorted by site-maximum %s, highest on the left; dashed lines = breaks, mean ± 0.5 SD (left to right: %d High, %d Mid, %d Low)",
                          tolower(METRIC_LAB), n_high, length(ord) - n_high - n_low, n_low)) +
  th + guides(fill = guide_colourbar(barwidth = unit(6, "cm"), barheight = unit(0.3, "cm"), title.position = "top"))
stemA <- file.path(OUT, sprintf("fig3b_signed_%s_%s_%s%s_landscape", FILE_KEY[[MKEY]], WIN, LEARNER, CLASS_TAG))
ggsave(paste0(stemA, ".png"), pA, width = 10, height = 6.6, dpi = 300)
ggsave(paste0(stemA, ".pdf"), pA, width = 10, height = 6.6, device = cairo_pdf)

# ---- A2. compact variant (2026-10-06): no site names, narrower bars, page-width figure; the three classes
#          are named above the first row instead.
cls_x <- c(High = (n_high + 1) / 2, Mid = (n_high + length(ord) - n_low + 1) / 2, Low = length(ord) - (n_low - 1) / 2)
cls_lab <- data.table(response = factor(EFP_ORDER[1], levels = EFP_ORDER), x = cls_x,
                      lab = sprintf("%s (%d)", names(cls_x), c(n_high, length(ord) - n_high - n_low, n_low)))
ytop <- sg[response == EFP_ORDER[1], max(val)]
pC <- ggplot(sg, aes(test_site, val, fill = v)) +
  geom_hline(yintercept = 0, colour = "grey40", linewidth = 0.25) +
  geom_vline(xintercept = brk, linetype = "dashed", colour = "grey30", linewidth = 0.3) +
  geom_col(width = 0.55, colour = NA) +
  geom_text(data = cls_lab, aes(x = x, y = ytop * 1.12, label = lab), inherit.aes = FALSE, size = 2.4, colour = "grey25", fontface = "bold") +
  fill_scale("fill") +
  scale_x_discrete(labels = NULL, expand = expansion(add = 0.8)) +
  scale_y_continuous(breaks = scales::breaks_pretty(n = 3), labels = scales::label_number(drop0trailing = TRUE),
                     expand = expansion(mult = c(0.05, 0.18))) +
  facet_grid(response ~ ., scales = "free_y", labeller = labeller(response = EFP_LAB)) +
  labs(x = sprintf("Sites, sorted by site-maximum %s (highest left)", tolower(METRIC_LAB)), y = "Mortality SHAP (response units)",
       title = sprintf("C+T+M, %s window, %s  |  %s", WIN, LEARNER_LAB, METRIC_LAB)) +
  th + theme(axis.text.x = element_blank(), panel.grid.major.y = element_line(colour = "grey92", linewidth = 0.15),
             panel.spacing.y = unit(2, "pt")) +
  guides(fill = guide_colourbar(barwidth = unit(5, "cm"), barheight = unit(0.25, "cm"), title.position = "top"))
stemC <- paste0(stemA, "_compact")
ggsave(paste0(stemC, ".png"), pC, width = 7, height = 5.2, dpi = 300)
ggsave(paste0(stemC, ".pdf"), pC, width = 7, height = 5.2, device = cairo_pdf)

# ---- B. |SHAP| composition (as Fig 3) ---------------------------------------------------
shap_f <- file.path(PAPER_OUT, sprintf("%s_site_shap_M4.csv", LEARNER))
if (file.exists(shap_f)) {   # variable-level SHAP is not available for every dataset (e.g. V2.2 paper figures)
sh <- fread(shap_f)[
  group %in% names(GROUP_COLOURS) & model == MODEL & response %in% EFP_ORDER,
  .(g = sum(mean_abs_shap, na.rm = TRUE)), by = .(response, test_site, group)]
sh[, rel := g / sum(g), by = .(response, test_site)]
ordB <- sh[group == "Disturbance", .(m = mean(rel)), by = test_site][order(-m), test_site]   # highest mortality share left
sh[, `:=`(test_site = factor(test_site, levels = ordB), response = factor(response, levels = EFP_ORDER),
          group = factor(group, levels = rev(names(GROUP_COLOURS))))]
strip <- site[SITE_ID %in% ordB, .(test_site = factor(SITE_ID, levels = ordB), v)]
pB_bars <- ggplot(sh, aes(test_site, rel, fill = group)) +
  geom_col(width = 0.85, colour = NA) +
  scale_fill_manual(values = GROUP_COLOURS, breaks = names(GROUP_COLOURS),
                    labels = c(Climate = "Climate", Traits = "Traits", Disturbance = "Mortality"), name = "Driver group") +
  scale_y_continuous(breaks = c(0, 0.5, 1), labels = c("0", "0.5", "1"), expand = expansion(0)) +
  facet_grid(response ~ ., labeller = labeller(response = EFP_LAB)) +
  labs(x = NULL, y = "Relative mean |SHAP|",
       title = sprintf("C+T+M, %s window, %s  |  driver composition", WIN, LEARNER_LAB),
       subtitle = sprintf("Sites sorted by mean mortality share over the four EFPs, highest on the left; strip below = site %s", tolower(METRIC_LAB))) +
  th + theme(axis.text.x = element_blank(), plot.margin = margin(4, 4, 0, 4))
pB_strip <- ggplot(strip, aes(test_site, 1, fill = v)) +
  geom_tile(height = 1, width = 0.85) + fill_scale("fill") +
  scale_x_discrete(labels = lab_of(ordB)) + scale_y_continuous(expand = expansion(0)) +
  labs(x = NULL, y = NULL) + th +
  theme(axis.text.y = element_blank(), axis.ticks.y = element_blank(), panel.grid = element_blank(),
        panel.border = element_blank(), plot.margin = margin(0, 4, 4, 4)) +
  guides(fill = guide_colourbar(barwidth = unit(6, "cm"), barheight = unit(0.3, "cm"), title.position = "top"))
pB <- (pB_bars / pB_strip) + plot_layout(heights = c(1, 0.035), guides = "collect") & theme(legend.position = "bottom")
stemB <- file.path(OUT, sprintf("fig3_%s_%s_%s%s_landscape", FILE_KEY[[MKEY]], WIN, LEARNER, CLASS_TAG))
ggsave(paste0(stemB, ".png"), pB, width = 10, height = 6.6, dpi = 300)
ggsave(paste0(stemB, ".pdf"), pB, width = 10, height = 6.6, device = cairo_pdf)
} else stemB <- "(no composition: variable-level SHAP missing)"
cat("written:", basename(stemA), basename(stemB), "\n")
}
