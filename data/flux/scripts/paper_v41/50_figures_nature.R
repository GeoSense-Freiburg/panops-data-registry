#!/usr/bin/env Rscript
# ============================================================================
# Main figures of the paper in Nature house style (fig_style.R), 2026-10-14. Reuses the model outputs and statistics;
# nothing is refitted. V5 maps, mortality block without thresholded metrics.
#   Fig. 1  sites (map) and distributions of the four ecosystem functional properties by IGBP class
#   Fig. 2  net mortality SHAP per site, sites ordered by relative mortality (compact)            [learner, window]
#   Fig. 3  net mortality SHAP, High vs Low+Mid relative-mortality sites, 12 and 24 months          [learner]
#   Fig. 4  site maximum of relative mortality vs net mortality SHAP                               [learner, window]
#   Fig. 5  mean residual per class of the site-year's relative mortality, without / with mortality [learner, window]
# Usage: Rscript 50_figures_nature.R [AG|XGB|LGB] [24m|12m]     (default AG 24m = the main-text versions)
# Output: plots/paper_v5_nothresh/nature/<learner>_<window>/figN_*.pdf/.png
# ============================================================================
suppressMessages({library(data.table); library(ggplot2); library(patchwork)})
ROOT <- Sys.getenv("PAPER_ROOT", "/mnt/gsdata/projects/panops/panops-data-registry/data/flux"); setwd(ROOT)
source("scripts/paper_v41/fig_style.R")
a <- commandArgs(trailingOnly = TRUE); L <- if (length(a) >= 1) a[1] else "AG"; WIN <- if (length(a) >= 2) a[2] else "24m"
LAB <- c(AG = "AutoGluon", XGB = "XGBoost", LGB = "LightGBM")[[L]]
OUT <- file.path("plots/paper_v5_nothresh/nature", paste0(L, "_", WIN))
EFP <- EFP_ORDER_FIG

# ---- data --------------------------------------------------------------------------------------------
ds <- fread("derived_tables/paper_v5/paper_dataset_full_lags_monthly_tc30_minrun2_buf100-200-300-400-500.csv")
ds[, tm := rowMeans(.SD), .SDcols = paste0("relative_disturbance_", c(100, 200, 300, 400, 500), "m")]
site <- ds[, .(tm_max = max(tm)), by = SITE_ID]
mu <- mean(site$tm_max); s <- sd(site$tm_max); B_LO <- max(mu - 0.5 * s, 0); B_HI <- mu + 0.5 * s
site[, cls := factor(fifelse(tm_max < B_LO, "Low", fifelse(tm_max > B_HI, "High", "Mid")), c("Low", "Mid", "High"))]
meta <- fread("fluxnet_2017_2025_V02/fluxnet_shuttle_snapshot_20260512T115423.csv")[, .(SITE_ID = site_id, lat = location_lat, lon = location_long, igbp)]
meta <- unique(meta, by = "SITE_ID")[SITE_ID %in% site$SITE_ID]
site <- merge(site, meta, by = "SITE_ID")
shap_file <- if (L == "AG") "AutoGluon/outputs/efp_v5_nothresh/AG_site_signed_shap_M4.csv" else
  sprintf("derived_tables/paper_v5_nothresh/models/%s_site_signed_shap_M4.csv", L)
sh <- fread(shap_file)[group == "Disturbance" & response %in% EFP]
sh <- merge(sh[, .(SITE_ID = test_site, response, window = sub("M4_", "", model), shap = mean_signed_shap)], site, by = "SITE_ID")
sh[, response := factor(response, EFP)]
efp_lab <- function(x) EFP_EXPR[as.character(x)]

# ---- Fig. 1: map + EFP distributions -------------------------------------------------------------------
if (L == "AG" && WIN == "24m") {
  # Robinson projection (curved, as in Jung et al. 2020 Sci. Data Fig. 2), computed with robinson() in fig_style.R
  world <- as.data.table(map_data("world", wrap = c(-180, 180)))[region != "Antarctica"]
  world[, c("x", "y") := robinson(long, lat)]
  sitep <- cbind(site, robinson(site$lon, site$lat))
  pmap <- ggplot() +
    geom_polygon(data = world, aes(x, y, group = group), fill = "#E3E3E3", colour = "white", linewidth = 0.08) +
    geom_point(data = sitep, aes(x, y, fill = igbp), shape = 21, size = 1.4, stroke = 0.2, colour = "black") +
    scale_fill_manual(values = PAL$igbp, breaks = names(PAL$igbp), name = NULL) +
    coord_fixed(ylim = range(robinson(0, c(-58, 84))$y), expand = FALSE, clip = "off") +
    guides(fill = guide_legend(nrow = 1, override.aes = list(size = 1.8))) +
    labs(x = NULL, y = NULL) + theme_pub() +
    theme(axis.line = element_blank(), axis.ticks = element_blank(), axis.text = element_blank(), legend.position = "bottom")
  dl <- melt(merge(ds[, c("SITE_ID", EFP), with = FALSE], site[, .(SITE_ID, igbp)], by = "SITE_ID"),
             id.vars = c("SITE_ID", "igbp"), variable.name = "response")
  dl[, igbp := factor(igbp, names(PAL$igbp))]
  pe <- lapply(EFP, function(e) ggplot(dl[response == e], aes(igbp, value, colour = igbp)) +
    geom_jitter(width = 0.18, size = 0.35, alpha = 0.6, stroke = 0) +
    geom_boxplot(outlier.shape = NA, fill = NA, colour = "black", width = 0.55, linewidth = 0.25) +
    scale_colour_manual(values = PAL$igbp, guide = "none") +
    labs(x = NULL, y = parse(text = EFP_EXPR[[e]])) + theme_pub() +
    theme(axis.text.x = element_text(size = 5.5, angle = 90, vjust = 0.5, hjust = 1)))
  f1 <- pmap / wrap_plots(pe, nrow = 1) + plot_layout(heights = c(1.45, 1)) + plot_annotation(tag_levels = "a")
  save_pub(f1, file.path(OUT, "fig1_sites_efps"), W2, 120)
}

# ---- Fig. 2: net mortality SHAP per site -----------------------------------------------------------------
# env FIG2_PALETTE = orrd (default) | piyg (green -> pink); FIG2_NAMES = 1 adds site codes under the last panel
FIG2_PAL <- Sys.getenv("FIG2_PALETTE", "orrd"); FIG2_NAMES <- Sys.getenv("FIG2_NAMES", "0") == "1"
fill2 <- if (FIG2_PAL == "piyg") scale_fill_mortality_piyg(b_lo = B_LO, b_hi = B_HI) else scale_fill_mortality()
s2 <- sh[window == WIN]
ord <- site[order(-tm_max), SITE_ID]
s2[, x := match(SITE_ID, ord)]
n_cls <- site[, .N, by = cls]
brk <- c(sum(site$cls == "High") + 0.5, sum(site$cls %in% c("High", "Mid")) + 0.5)
cls_lab <- data.table(x = c(brk[1] / 2, mean(brk), (brk[2] + nrow(site)) / 2),
                      lab = sprintf("%s (%d)", c("High", "Mid", "Low"), n_cls[match(c("High", "Mid", "Low"), cls), N]))
# site codes under the last panel in two staggered rows (zig-zag: every other code one row lower), which doubles
# the space per label (6 pt instead of 5 pt); FIG2_NAMES = top: odd ranks below, even ranks above the panels instead
NAMES_MODE <- Sys.getenv("FIG2_NAMES_MODE", "zigzag")
odd <- seq(1, length(ord), 2); even <- seq(2, length(ord), 2)
x_scale <- function(e) {
  if (FIG2_NAMES && NAMES_MODE == "top" && e == EFP[1])
    return(scale_x_continuous(expand = expansion(add = 0.6), breaks = NULL,
                              sec.axis = dup_axis(breaks = even, labels = ord[even], name = NULL)))
  if (FIG2_NAMES && NAMES_MODE == "top" && e == EFP[length(EFP)])
    return(scale_x_continuous(expand = expansion(add = 0.6), breaks = odd, labels = ord[odd]))
  if (FIG2_NAMES && e == EFP[length(EFP)])
    return(scale_x_continuous(expand = expansion(add = 0.6), breaks = seq_along(ord), labels = ord,
                              guide = guide_axis(n.dodge = 2)))
  scale_x_continuous(expand = expansion(add = 0.6), breaks = NULL)
}
p2 <- lapply(EFP, function(e) {
  d <- s2[response == e]
  g <- ggplot(d, aes(x, shap, fill = tm_max)) +
    geom_hline(yintercept = 0, colour = PAL$zero, linewidth = 0.25) +
    geom_vline(xintercept = brk, linetype = "22", linewidth = 0.25, colour = PAL$neutral) +
    geom_col(width = 0.75) + fill2 +
    x_scale(e) +
    labs(x = NULL, y = parse(text = sprintf("atop(%s~SHAP, %s)", EFP_SHORT[[e]], EFP_UNIT[[e]]))) + theme_pub() +
    theme(axis.text.x = element_blank(), axis.ticks.x = element_blank())
  if (e == EFP[1]) g <- g + geom_text(data = cls_lab, aes(x, Inf, label = lab), inherit.aes = FALSE, vjust = 1.2)
  if (FIG2_NAMES && NAMES_MODE == "top" && e == EFP[1]) g <- g + theme(axis.text.x.top = element_text(size = 6, angle = 90, vjust = 0.5, hjust = 0),
                                                axis.ticks.x.top = element_line(linewidth = 0.2), axis.line.x.top = element_blank())
  if (e == EFP[length(EFP)]) {
    g <- g + labs(x = "Sites, ordered by maximum relative mortality (highest left)")
    if (FIG2_NAMES) g <- g + theme(axis.text.x = element_text(size = 6, angle = 90, vjust = 0.5, hjust = 1),
                                   axis.ticks.x = element_line(linewidth = 0.2))
  }
  g })
f2 <- wrap_plots(p2, ncol = 1, guides = "collect") + plot_annotation(tag_levels = "a") &
  theme(legend.position = "right", legend.key.height = unit(10, "mm"), legend.key.width = unit(2, "mm"))
f2 <- f2 + plot_annotation(caption = NULL)
f2_stem <- paste0("fig2_net_mortality_shap_per_site", if (FIG2_PAL == "piyg") "_piyg" else "", if (FIG2_NAMES) "_names" else "")
save_pub(f2, file.path(OUT, f2_stem), W2, if (FIG2_NAMES) 150 else 125)

# ---- Fig. 3: High vs Low+Mid -------------------------------------------------------------------------------
# env FIG3_PALETTE = piyg (default since 2026-10-15, matches Fig 2) | orrd
FIG3_PAL <- Sys.getenv("FIG3_PALETTE", "piyg")
FILL3 <- PAL$class2; PT3 <- c("Low+Mid" = "#B35806", High = "#3D0000")
s3 <- copy(sh)[, grp := factor(fifelse(cls == "High", "High", "Low+Mid"), c("Low+Mid", "High"))]
s3[, win := factor(sub("m", " months", window), c("12 months", "24 months"))]
if (FIG3_PAL == "piyg") {   # exact Fig. 2 colours: box = scale colour at the group's median site maximum, point = the site's bar colour
  med3 <- s3[, .(m = median(unique(data.table(SITE_ID, tm_max))$tm_max)), by = grp]
  FILL3 <- setNames(mortality_piyg_col(med3$m, B_LO, B_HI), as.character(med3$grp))[c("Low+Mid", "High")]
  s3[, pcol := mortality_piyg_col(tm_max, B_LO, B_HI)]
} else s3[, pcol := PT3[as.character(grp)]]
st3 <- s3[, .(p = wilcox.test(shap[grp == "High"], shap[grp == "Low+Mid"], exact = FALSE)$p.value,
              ytop = max(shap)), by = .(response, win)]
st3[, q := p.adjust(p, "BH")]
p3 <- lapply(EFP, function(e) ggplot(s3[response == e], aes(win, shap, fill = grp)) +
  geom_hline(yintercept = 0, colour = PAL$zero, linewidth = 0.25) +
  geom_boxplot(outlier.shape = NA, width = 0.62, linewidth = 0.25, position = position_dodge(0.72)) +
  geom_point(aes(colour = pcol, group = grp), position = position_jitterdodge(jitter.width = 0.12, dodge.width = 0.72),
             shape = 21, fill = NA, size = 0.45, stroke = 0.35, show.legend = FALSE) +
  geom_text(data = st3[response == e], aes(win, ytop, label = stars(q)), inherit.aes = FALSE, vjust = -0.2) +
  scale_fill_manual(values = FILL3, name = "Relative mortality") + scale_colour_identity() +
  scale_y_continuous(expand = expansion(mult = c(0.05, 0.12))) +
  labs(x = NULL, y = if (e == EFP[1]) "Net mortality SHAP (EFP units)" else NULL, title = parse(text = EFP_EXPR[[e]])) +
  theme_pub())
f3 <- wrap_plots(p3, nrow = 1, guides = "collect") + plot_annotation(tag_levels = "a") & theme(legend.position = "bottom")
save_pub(f3, file.path(OUT, "fig3_high_vs_lowmid"), W2, 62)

# ---- Fig. 4: relative mortality vs direction ------------------------------------------------------------------
s4 <- sh[window == WIN]
st4 <- s4[, {ct <- suppressWarnings(cor.test(tm_max, shap, method = "spearman", exact = FALSE)); .(rho = unname(ct$estimate), p = ct$p.value)}, by = response]
st4[, q := p.adjust(p, "BH")][, lab := sprintf("rho == '%s%.2f%s'", ifelse(rho < 0, "\u2212", ""), abs(rho), stars(q))]
p4 <- lapply(EFP, function(e) ggplot(s4[response == e], aes(tm_max, shap)) +
  geom_hline(yintercept = 0, colour = PAL$zero, linewidth = 0.25, linetype = "22") +
  geom_smooth(method = "lm", formula = y ~ x, colour = "black", fill = "#BDBDBD", linewidth = 0.4, alpha = 0.6) +
  geom_point(aes(fill = igbp), shape = 21, size = 1.0, stroke = 0.15, colour = "black", alpha = 0.85) +
  geom_text(data = st4[response == e], aes(x = Inf, y = Inf, label = lab), parse = TRUE, inherit.aes = FALSE, hjust = 1.05, vjust = 1.3) +
  scale_fill_manual(values = PAL$igbp, breaks = names(PAL$igbp), name = NULL) +
  labs(x = "Site maximum relative mortality (%)",
       y = if (e == EFP[1]) "Net mortality SHAP (EFP units)" else NULL, title = parse(text = EFP_EXPR[[e]])) +
  theme_pub())
f4 <- wrap_plots(p4, nrow = 1, guides = "collect") + plot_annotation(tag_levels = "a") &
  theme(legend.position = "bottom") & guides(fill = guide_legend(nrow = 1, override.aes = list(size = 1.8)))
save_pub(f4, file.path(OUT, "fig4_mortality_vs_direction"), W2, 62)

# ---- Fig. 5: residual by mortality class ---------------------------------------------------------------------
rf <- sprintf("plots/paper_v5_nothresh/dose_response/residual_by_mortality_%s_%s.csv", L, WIN)
cf <- sprintf("plots/paper_v5_nothresh/dose_response/residual_correction_by_mortality_%s_%s.csv", L, WIN)
if (file.exists(rf) && file.exists(cf)) {
  r5 <- fread(rf); c5 <- fread(cf)
  r5[, model := fifelse(grepl("^Without", model), "C+T", "C+T+M")]
  BL <- c("0-10 %", "10-20 %", "20-30 %", "> 30 %")
  r5[, bin := factor(bin, BL)]; c5[, bin := factor(bin, BL)]
  nn <- r5[model == "C+T", .(response, bin, lab = sprintf("%d\n(%d)", n_sy, n_sites))]
  pd <- position_dodge(0.45)
  p5 <- lapply(EFP, function(e) {
    d <- r5[response == e]; yr <- range(c(d$lo, d$hi)); pad <- diff(yr) * 0.12
    ggplot(d, aes(bin, mean, colour = model, group = model)) +
      geom_hline(yintercept = 0, colour = PAL$zero, linewidth = 0.25) +
      geom_line(position = pd, linewidth = 0.35, alpha = 0.7) +
      geom_errorbar(aes(ymin = lo, ymax = hi), width = 0, linewidth = 0.4, position = pd) +
      geom_point(position = pd, size = 1.1) +
      geom_text(data = c5[response == e], aes(bin, yr[2] + pad * 0.9, label = stars(q_perm)), inherit.aes = FALSE) +
      geom_text(data = nn[response == e], aes(bin, yr[1] - pad * 1.15, label = lab), inherit.aes = FALSE, size = 5 / .pt, lineheight = 0.85, colour = "#4D4D4D") +
      scale_colour_manual(values = PAL$model, labels = c("C+T" = "Without mortality (C+T)", "C+T+M" = "With mortality (C+T+M)"), name = NULL) +
      scale_y_continuous(limits = c(yr[1] - pad * 1.9, yr[2] + pad * 1.5)) +
      labs(x = "Site-year relative mortality",
           y = if (e == EFP[1]) "Mean residual (observed \u2212 predicted)" else NULL, title = parse(text = EFP_EXPR[[e]])) +
      theme_pub() + theme(axis.text.x = element_text(size = 5.5))
  })
  f5 <- wrap_plots(p5, nrow = 1, guides = "collect") + plot_annotation(tag_levels = "a") & theme(legend.position = "bottom")
  save_pub(f5, file.path(OUT, "fig5_residual_by_mortality_class"), W2, 66)
}
cat(sprintf("Nature-style figures for %s %s -> %s (class breaks %.1f / %.1f %%)\n", LAB, WIN, OUT, B_LO, B_HI))
