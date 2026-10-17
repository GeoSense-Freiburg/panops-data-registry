#!/usr/bin/env Rscript
# ============================================================================
# V10 Fig. 2 style figure for the flux-totals part (predict_fluxes/): effect of
# adding disturbance (M3 -> M4) on per-site RMSE, growing-season accumulated
# GPP / NEP / RECO / ET / WUE. Layout, colours and statistics copied from
# scripts/plot_draft_fig2_M3M4.R: paired violins (cyan = without D, pink = with D),
# rows = response, columns = learner, x = window; label = median PAIRED % change in
# per-site RMSE + one-sided paired Wilcoxon stars (BH-FDR across the whole figure) +
# share of sites that improved; y zoomed to the 95th percentile (no data removed).
# Input: predict_fluxes/derived_tables/flux_totals_predictions_<XGB|LGB>_repeated.csv
# (site_predictions_flux_totals.R).
# ============================================================================
suppressMessages({library(data.table); library(ggplot2); library(ggh4x)})
setwd("/mnt/gsdata/projects/panops/panops-data-registry/data/flux")
OUT <- "predict_fluxes/plots"; dir.create(OUT, showWarnings = FALSE, recursive = TRUE)

DARK_BG <- "white"; PANEL_BG <- "white"; GRID_COL <- "#D9D9D9"
TEXT_COL <- "#111111"; AXIS_COL <- "#444444"
COL_WO <- "#22C4E0"; COL_W <- "#E8257A"
COL_UP <- "#2ECC71"; COL_NC <- "#9AA0A6"; COL_DN <- "#E74C3C"

RESP_ORDER <- c("GPP_sum_gC", "NEP_sum_gC", "RECO_sum_gC", "ET_sum_mm", "WUE_annual")
RESP_LABELS <- c(GPP_sum_gC = "GPP  (g C m⁻²)", NEP_sum_gC = "NEP  (g C m⁻²)", RECO_sum_gC = "RECO  (g C m⁻²)",
                 ET_sum_mm = "ET  (mm)", WUE_annual = "WUE  (g C mm⁻¹)")
LEARNER_LAB <- c(XGB = "XGBoost", LGB = "LightGBM")

pr <- rbindlist(lapply(c("XGB", "LGB"), function(l)
  fread(sprintf("predict_fluxes/derived_tables/flux_totals_predictions_%s_repeated.csv", l))))
site_rmse <- pr[, .(rmse = sqrt(mean((observed - predicted)^2))), by = .(learner, window, response, model, SITE_ID)]

rows <- list(); labs <- list(); k <- 1
for (L in c("XGB", "LGB")) for (w in c("12m", "24m")) for (resp in RESP_ORDER) {
  a <- site_rmse[learner == L & window == w & response == resp & model == "M3", .(SITE_ID, wo = rmse)]
  b <- site_rmse[learner == L & window == w & response == resp & model == "M4", .(SITE_ID, wd = rmse)]
  pw <- merge(a, b, by = "SITE_ID"); pw <- pw[is.finite(wo) & is.finite(wd)]
  if (!nrow(pw)) next
  rows[[length(rows) + 1]] <- data.table(response = resp, learner = LEARNER_LAB[[L]], grp = w, model_type = "Without D", rmse = pw$wo)
  rows[[length(rows) + 1]] <- data.table(response = resp, learner = LEARNER_LAB[[L]], grp = w, model_type = "With D", rmse = pw$wd)
  labs[[k]] <- data.table(response = resp, learner = LEARNER_LAB[[L]], grp = w, n_sites = nrow(pw),
    pct = median((pw$wd - pw$wo) / pw$wo * 100),
    p = wilcox.test(pw$wd, pw$wo, paired = TRUE, alternative = "less", exact = FALSE)$p.value,
    imp = mean(pw$wd < pw$wo) * 100); k <- k + 1
}
dt <- rbindlist(rows); labs <- rbindlist(labs)
labs[, q := p.adjust(p, method = "BH")]
labs[, sig := ifelse(q < 0.001, "***", ifelse(q < 0.01, "**", ifelse(q < 0.05, "*", "ns")))]
labs[, lcol := ifelse(pct < 0 & q < 0.05, COL_UP, ifelse(pct > 0, COL_DN, COL_NC))]
labs[, label := sprintf("%+.1f%%%s\n%.0f%%↓", pct, sig, imp)]

dt[, response := factor(response, RESP_ORDER)]; labs[, response := factor(response, RESP_ORDER)]
dt[, grp := factor(grp, c("12m", "24m"))]; labs[, grp := factor(grp, c("12m", "24m"))]
dt[, learner := factor(learner, LEARNER_LAB)]; labs[, learner := factor(learner, LEARNER_LAB)]
dt[, model_type := factor(model_type, c("Without D", "With D"))]

ymax <- dt[, .(ytop = quantile(rmse, 0.95, na.rm = TRUE)), by = response]
labs <- merge(labs, ymax, by = "response"); labs[, ylab := ytop * 0.94]

th <- theme_bw(base_size = 11) + theme(
  plot.background = element_rect(fill = DARK_BG, colour = NA),
  panel.background = element_rect(fill = PANEL_BG, colour = NA),
  panel.border = element_rect(colour = GRID_COL, fill = NA, linewidth = 0.4),
  panel.grid.major = element_line(colour = GRID_COL, linewidth = 0.2),
  panel.grid.minor = element_blank(),
  strip.background = element_rect(fill = "#EFEFEF", colour = GRID_COL),
  strip.text = element_text(colour = TEXT_COL, size = 10, face = "bold"),
  axis.text.x = element_text(colour = AXIS_COL, size = 8),
  axis.text.y = element_text(colour = AXIS_COL, size = 9),
  legend.background = element_rect(fill = NA, colour = NA),
  legend.key = element_rect(fill = NA, colour = NA),
  legend.text = element_text(colour = TEXT_COL, size = 11),
  legend.title = element_blank(), legend.position = "bottom",
  plot.title = element_text(colour = TEXT_COL, size = 14, face = "bold"),
  plot.subtitle = element_text(colour = AXIS_COL, size = 9))

p <- ggplot(dt, aes(x = grp, y = rmse, fill = model_type)) +
  geom_violin(position = position_dodge(width = 0.8), colour = NA, width = 0.78, alpha = 0.75, trim = TRUE) +
  geom_boxplot(aes(group = interaction(grp, model_type)), position = position_dodge(width = 0.8), width = 0.15,
               outlier.shape = NA, colour = "#222222", fill = NA, linewidth = 0.3) +
  geom_text(data = labs, aes(x = grp, y = ylab, label = label, colour = lcol), inherit.aes = FALSE,
            size = 2.5, fontface = "bold") +
  scale_fill_manual(values = setNames(c(COL_WO, COL_W), c("Without D", "With D"))) +
  scale_colour_identity() +
  facet_grid(response ~ learner, scales = "free_y", labeller = labeller(response = RESP_LABELS), switch = "y") +
  ggh4x::facetted_pos_scales(y = lapply(RESP_ORDER, function(r)
    scale_y_continuous(limits = c(0, ymax[response == r, ytop] * 1.08), oob = scales::oob_keep))) +
  coord_cartesian(clip = "on") +
  labs(x = NULL, y = NULL,
       title = "Effect of adding disturbance predictors on per-site RMSE: growing-season flux totals",
       subtitle = paste0("Tree cover ≥30 % (93 sites, 378 site-years) · C+T vs C+T+D · repeated CV (3×80 % of sites) · cyan = without D, pink = with D\n",
                         "% = median PAIRED change in per-site RMSE (negative = less error) · stars = one-sided paired Wilcoxon, BH-FDR across the figure (*** q<0.001, ** q<0.01, * q<0.05)\n",
                         "%↓ = share of sites whose RMSE improved · y axis zoomed to the 95th percentile (no data removed) · hyperparameters: GPPsat's tuned config reused for every response")) +
  th + theme(strip.placement = "outside") +
  guides(colour = "none", fill = guide_legend(override.aes = list(alpha = 0.9)))

ggsave(file.path(OUT, "fig2_style_RMSE_change_flux_totals.png"), p, width = 15, height = 14.5, dpi = 300, bg = DARK_BG)
ggsave(file.path(OUT, "fig2_style_RMSE_change_flux_totals.pdf"), p, width = 15, height = 14.5, bg = DARK_BG)
fwrite(labs[, .(learner, response, window = grp, n_sites, median_pct = round(pct, 2), sites_improved = round(imp, 1),
                p = signif(p, 3), q = signif(q, 3), sig)], file.path(OUT, "fig2_style_statistics.csv"))
cat(sprintf("Saved | %d panels | %d tests\n", uniqueN(dt[, .(response, learner)]), nrow(labs)))
print(labs[order(learner, response, grp), .(learner, response, grp, n_sites, pct = round(pct, 1), q = signif(q, 2), sig, improved = round(imp))])
