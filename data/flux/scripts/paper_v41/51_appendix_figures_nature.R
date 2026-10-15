#!/usr/bin/env Rscript
# ============================================================================
# Appendix (Extended Data / Supplementary) figures in the Nature style and colours of the main figures (fig_style.R),
# 2026-10-15. Redraws the figures that 21, 23, 24, 26/38, 29, 41, 42 and 43 drew in the old style; reuses their model
# outputs and statistics (nothing is refitted). V5 maps, mortality block without thresholded metrics.
#   site_mae_<AG|all>[_high]     per-site MAE without / with mortality (old 21 MAE_perm)
#   site_drmse_AG                per-site change in RMSE, sites sorted (old 29)
#   direction_rel_deadwood_AG    Fig. 4 with the sites grouped by relative deadwood (old 23)
#   igbp_<L>_<win>               mortality effect by IGBP class and tree cover (old 24 board)
#   mortality_block_<L>          share of each mortality metric in the block's |SHAP| (old 26)
#   dropcol                      SHAP share, unique and alone contribution per metric (old 38)
#   dose_robustness_AG           slopes per +10 % relative mortality under each robustness check (old 42)
#   dose_siteyear_AG             every site-year with the mixed-model slopes (old 41)
#   residual_efpclim_AG_<win>    Fig. 2 with the EFP-specific climate block (old 43 efpclim)
# Usage: Rscript 51_appendix_figures_nature.R        Output: plots/paper_v5_nothresh/nature/appendix/
# ============================================================================
suppressMessages({library(data.table); library(ggplot2); library(patchwork)})
Sys.setenv(PAPER_RESP_SET = "efp_v5", PAPER_NO_THRESH = "1")
ROOT <- Sys.getenv("PAPER_ROOT", "/mnt/gsdata/projects/panops/panops-data-registry/data/flux"); setwd(ROOT)
source("scripts/paper_v41/paper_common.R")
source("scripts/paper_v41/fig_style.R")
OUT <- "plots/paper_v5_nothresh/nature/appendix"; dir.create(OUT, showWarnings = FALSE, recursive = TRUE)
EFP <- EFP_ORDER_FIG; WINS <- c("12m", "24m"); WLAB <- c("12m" = "12 months", "24m" = "24 months")
LAB <- c(AG = "AutoGluon", XGB = "XGBoost", LGB = "LightGBM")
FI <- "AutoGluon/outputs/efp_v5_nothresh/fig_inputs"                     # predictions and SHAP of all three learners
BOX <- function(...) geom_boxplot(outlier.shape = NA, linewidth = 0.25, ...)
efp_title <- function(e) parse(text = EFP_EXPR[[e]])
win_lab <- function(x) factor(WLAB[x], WLAB)

# ---- shared data: relative mortality per site-year (mean of the five buffers), classes as in the main figures ----
ds <- fread("derived_tables/paper_v5/paper_dataset_full_lags_monthly_tc30_minrun2_buf100-200-300-400-500.csv")
ds[, tm := rowMeans(.SD), .SDcols = paste0("relative_disturbance_", c(100, 200, 300, 400, 500), "m")]
ds[, rd := rowMeans(.SD), .SDcols = paste0("relative_mortality_", c(100, 200, 300, 400, 500), "m")]
site <- ds[, .(tm_max = max(tm), rd_max = max(rd, na.rm = TRUE), igbp = IGBP[1],
               tc = mean(tc_site_mean_500m, na.rm = TRUE)), by = SITE_ID]
brk <- function(x) { m <- mean(x); s <- sd(x); c(max(m - 0.5 * s, 0), m + 0.5 * s) }
B <- brk(site$tm_max); B_LO <- B[1]; B_HI <- B[2]
site[, cls := factor(fifelse(tm_max < B_LO, "Low", fifelse(tm_max > B_HI, "High", "Mid")), c("Low", "Mid", "High"))]
site[, col := mortality_piyg_col(tm_max, B_LO, B_HI)]                    # each site in its Fig. 3 colour
site[, igbp := factor(igbp, names(PAL$igbp))]
pred <- function(L) {
  p <- fread(file.path(FI, sprintf("%s_predictions_repCV.csv", L)))[response %in% EFP & grepl("^M[34]_(12m|24m)$", model)]
  p[, c("mod", "window") := tstrsplit(model, "_")][is.finite(observed) & is.finite(predicted)][, learner := LAB[[L]]][]
}
P <- rbindlist(lapply(names(LAB), pred))

# ---- 1. per-site MAE without / with mortality ------------------------------------------------------------------
mae <- P[, .(mae = mean(abs(observed - predicted))), by = .(learner, response, window, mod, SITE_ID)]
mae[, model := factor(fifelse(mod == "M3", "C+T", "C+T+M"), c("C+T", "C+T+M"))]
mae_fig <- function(learners, high = FALSE, stem) {
  d <- mae[learner %in% learners]; if (high) d <- d[SITE_ID %in% site[cls == "High", SITE_ID]]
  sf <- sprintf("AutoGluon/plots/efp_v5_nothresh/%s%s_statistics.csv", if (length(learners) == 1) "ag_only/" else "",
                if (high) "fig2_high_total_mort_MAE_perm" else "fig2_MAE_perm")
  st <- fread(sf)[, learner := sub(" \\(.*", "", learner)][learner %in% learners]
  st[, lab := sprintf("%s%.1f %%%s\n%.0f %% improved", ifelse(median_pct < 0, "−", "+"), abs(median_pct), fifelse(sig == "ns", "", sig), sites_improved)]
  d[, win := win_lab(window)]; st[, win := win_lab(grp)]
  ylim <- d[, .(top = quantile(mae, 0.95)), by = .(learner, response)]       # zoom: 95th percentile (stated in the caption)
  rows <- lapply(learners, function(l) {
    lapply(EFP, function(e) {
      x <- d[learner == l & response == e]; yt <- ylim[learner == l & response == e, top]
      ggplot(x, aes(win, mae, fill = model)) +
        BOX(width = 0.6, position = position_dodge(0.7)) +
        geom_point(aes(colour = model), position = position_jitterdodge(jitter.width = 0.1, dodge.width = 0.7),
                   size = 0.3, alpha = 0.5, stroke = 0, show.legend = FALSE) +
        geom_label(data = st[learner == l & response == e], aes(win, yt * 1.13, label = lab), inherit.aes = FALSE,
                   size = 5 / .pt, lineheight = 0.85, label.size = 0, fill = "white", label.padding = unit(0.5, "pt")) +
        scale_fill_manual(values = alpha(PAL$model, 0.35), labels = c("Without mortality (C+T)", "With mortality (C+T+M)"), name = NULL) +
        scale_colour_manual(values = PAL$model) +
        coord_cartesian(ylim = c(0, yt * 1.22)) +
        labs(x = NULL, y = if (e == EFP[1]) (if (length(learners) > 1) paste0(l, "\nper-site MAE") else "Per-site MAE") else NULL,
             title = if (l == learners[1]) efp_title(e) else NULL) +
        theme_pub()
    })
  })
  f <- wrap_plots(unlist(rows, recursive = FALSE), ncol = 4, guides = "collect") + plot_annotation(tag_levels = "a") &
    theme(legend.position = "bottom")
  save_pub(f, file.path(OUT, stem), W2, if (length(learners) > 1) 150 else 62)
}
mae_fig("AutoGluon", FALSE, "site_mae_AG")
mae_fig("AutoGluon", TRUE, "site_mae_AG_high")
mae_fig(c("AutoGluon", "XGBoost", "LightGBM"), FALSE, "site_mae_all")

# ---- 2. per-site change in RMSE, sites sorted -------------------------------------------------------------------
rm <- P[learner == "AutoGluon", .(rmse = sqrt(mean((observed - predicted)^2))), by = .(response, window, mod, SITE_ID)]
rm <- dcast(rm, response + window + SITE_ID ~ mod, value.var = "rmse")[, d := M4 - M3]
rm <- merge(rm, site[, .(SITE_ID, col)], by = "SITE_ID")
rm[, rk := frank(d, ties.method = "first"), by = .(response, window)]
imp <- rm[, .(lab = sprintf("%.0f%% of sites improved", 100 * mean(d < 0))), by = .(response, window)]
pr <- unlist(lapply(WINS, function(w) lapply(EFP, function(e) {      # EFPs as columns, windows as rows (2026-10-15)
  ggplot(rm[response == e & window == w], aes(rk, d, fill = col)) +
    geom_hline(yintercept = 0, colour = PAL$zero, linewidth = 0.25) + geom_col(width = 0.8) + scale_fill_identity() +
    geom_text(data = imp[response == e & window == w], aes(x = 1, y = Inf, label = lab), inherit.aes = FALSE, hjust = 0, vjust = 1.3) +
    scale_y_continuous(expand = expansion(mult = c(0.05, 0.15))) +
    labs(x = if (w == "24m") "Sites, sorted by the change" else NULL,
         y = if (e == EFP[1]) paste0(WLAB[[w]], "\n\u0394RMSE (EFP units)") else NULL,
         title = if (w == "12m") efp_title(e) else NULL) +
    theme_pub() + theme(axis.text.x = element_blank(), axis.ticks.x = element_blank())
})), recursive = FALSE)
save_pub(wrap_plots(pr, ncol = 4) + plot_annotation(tag_levels = "a"), file.path(OUT, "site_drmse_AG"), W2, 85)

# ---- 3. Fig. 4 with the sites grouped by relative deadwood ------------------------------------------------------
sh <- function(L) fread(file.path(FI, sprintf("%s_site_signed_shap_M4.csv", L)))[group == "Disturbance" & response %in% EFP][
  , .(SITE_ID = test_site, response, window = sub("M4_", "", model), shap = mean_signed_shap, learner = LAB[[L]])]
SH <- rbindlist(lapply(names(LAB), sh))
Brd <- brk(site$rd_max)
s3 <- merge(SH[learner == "AutoGluon"], site[, .(SITE_ID, rd_max)], by = "SITE_ID")
s3[, grp := factor(fifelse(rd_max > Brd[2], "High", "Low+Mid"), c("Low+Mid", "High"))][, win := win_lab(window)]
st3 <- s3[, .(p = wilcox.test(shap[grp == "High"], shap[grp == "Low+Mid"], exact = FALSE)$p.value, ytop = max(shap)), by = .(response, win)]
st3[, q := p.adjust(p, "BH")]
p3 <- lapply(EFP, function(e) ggplot(s3[response == e], aes(win, shap, fill = grp)) +
  geom_hline(yintercept = 0, colour = PAL$zero, linewidth = 0.25) +
  BOX(width = 0.62, position = position_dodge(0.72)) +
  geom_point(aes(colour = grp), position = position_jitterdodge(jitter.width = 0.12, dodge.width = 0.72), size = 0.45,
             shape = 21, fill = NA, stroke = 0.35, show.legend = FALSE) +
  geom_text(data = st3[response == e], aes(win, ytop, label = stars(q)), inherit.aes = FALSE, vjust = -0.2) +
  scale_fill_manual(values = PAL$class2_piyg, name = "Relative deadwood") + scale_colour_manual(values = PAL$class2_piyg_pt) +
  scale_y_continuous(expand = expansion(mult = c(0.05, 0.12))) +
  labs(x = NULL, y = if (e == EFP[1]) "Net mortality SHAP (EFP units)" else NULL, title = efp_title(e)) + theme_pub())
save_pub(wrap_plots(p3, nrow = 1, guides = "collect") + plot_annotation(tag_levels = "a") & theme(legend.position = "bottom"),
         file.path(OUT, "direction_rel_deadwood_AG"), W2, 62)

# ---- 4. mortality effect by IGBP class and tree cover ------------------------------------------------------------
igbp_fig <- function(Lk, w) {
  l <- LAB[[Lk]]
  r <- P[learner == l & window == w, .(rmse = sqrt(mean((observed - predicted)^2))), by = .(response, mod, SITE_ID)]
  r <- dcast(r, response + SITE_ID ~ mod, value.var = "rmse")[, d := M4 - M3]
  x <- merge(merge(r, SH[learner == l & window == w, .(SITE_ID, response, shap)], by = c("SITE_ID", "response")),
             site[, .(SITE_ID, igbp, tc, tm_max)], by = "SITE_ID")
  cor_lab <- function(a, b) { ct <- suppressWarnings(cor.test(a, b, method = "spearman", exact = FALSE)); c(rho = unname(ct$estimate), p = ct$p.value) }
  cs <- rbindlist(lapply(EFP, function(e) { z <- x[response == e]
    rbind(data.table(response = e, row = "c", t(cor_lab(z$tc, z$d))), data.table(response = e, row = "d", t(cor_lab(z$tc, z$shap))),
          data.table(response = e, row = "e", t(cor_lab(z$tm_max, z$shap)))) }))
  cs[, q := p.adjust(p, "BH")][, lab := sprintf("rho == '%s%.2f%s'", ifelse(rho < 0, "−", ""), abs(rho), stars(q))]
  box_row <- function(yv, ylab) lapply(EFP, function(e) ggplot(x[response == e], aes(igbp, .data[[yv]])) +
    geom_hline(yintercept = 0, colour = PAL$zero, linewidth = 0.25) +
    BOX(width = 0.6, fill = NA) + geom_point(aes(fill = igbp), shape = 21, size = 0.8, stroke = 0.15, colour = "black",
                                              position = position_jitter(width = 0.15, seed = 1)) +
    scale_fill_manual(values = PAL$igbp, drop = FALSE, name = NULL) + scale_x_discrete(drop = FALSE) +
    labs(x = NULL, y = if (e == EFP[1]) ylab else NULL, title = efp_title(e)) + theme_pub() +
    theme(axis.text.x = element_text(size = 5, angle = 90, vjust = 0.5, hjust = 1)))
  sc_row <- function(xv, yv, xlab, ylab, rw) lapply(EFP, function(e) ggplot(x[response == e], aes(.data[[xv]], .data[[yv]])) +
    geom_hline(yintercept = 0, colour = PAL$zero, linewidth = 0.25, linetype = "22") +
    geom_smooth(method = "lm", formula = y ~ x, colour = "black", fill = "#BDBDBD", linewidth = 0.4) +
    geom_point(aes(fill = igbp), shape = 21, size = 0.8, stroke = 0.15, colour = "black") +
    geom_text(data = cs[response == e & row == rw], aes(x = ifelse(rho > 0, -Inf, Inf), y = Inf, label = lab, hjust = ifelse(rho > 0, -0.05, 1.05)),
              parse = TRUE, inherit.aes = FALSE, vjust = 1.3) +
    scale_fill_manual(values = PAL$igbp, drop = FALSE, name = NULL) +
    labs(x = xlab, y = if (e == EFP[1]) ylab else NULL) + theme_pub())
  rows <- list(box_row("d", "ΔRMSE\n(EFP units)"), box_row("shap", "Net mortality\nSHAP"),
               sc_row("tc", "d", "Tree cover (%)", "ΔRMSE\n(EFP units)", "c"),
               sc_row("tc", "shap", "Tree cover (%)", "Net mortality\nSHAP", "d"),
               sc_row("tm_max", "shap", "Site maximum relative mortality (%)", "Net mortality\nSHAP", "e"))
  rows <- lapply(seq_along(rows), function(i) lapply(seq_along(rows[[i]]), function(j) rows[[i]][[j]] +
    labs(tag = if (j == 1) letters[i] else NULL) + if (i > 1) labs(title = NULL) else NULL))
  f <- wrap_plots(unlist(rows, recursive = FALSE), ncol = 4, guides = "collect") &
    theme(legend.position = "bottom") & guides(fill = guide_legend(nrow = 1, override.aes = list(size = 1.8)))
  save_pub(f, file.path(OUT, sprintf("igbp_%s_%s", Lk, w)), W2, 170)
}
igbp_fig("AG", "24m"); igbp_fig("AG", "12m"); igbp_fig("XGB", "24m")

# ---- 5. share of each mortality metric in the block, and the drop-column test ------------------------------------
FAM <- c(relative_disturbance = "Relative mortality", relative_mortality = "Relative deadwood",
         absolute_mortality = "Absolute deadwood", new_deadwood_gain_pp = "Deadwood gain",
         relative_tree_loss_pct = "Relative tree-cover loss")
FAM_COL <- setNames(c("#CC79A7", "#E69F00", "#F0E442", "#56B4E9", "#009E73"), FAM)
fam_of <- function(v) sub("_[0-9]+m(_lag[12])?$", "", v)
share <- function(L) {
  s <- fread(file.path(FI, sprintf("%s_site_shap_M4.csv", L)))[group == "Disturbance" & response %in% EFP]
  s[, family := fam_of(variable)]
  s <- s[family %in% names(FAM), .(v = sum(mean_abs_shap)), by = .(model, response, test_site, family)]
  s[, share := 100 * v / sum(v), by = .(model, response, test_site)]
  s[, .(m = mean(share), se = sd(share) / sqrt(.N)), by = .(model, response, family)][
    , `:=`(learner = LAB[[L]], window = sub("M4_", "", model), metric = factor(FAM[family], rev(FAM)))][]
}
SHR <- rbindlist(lapply(names(LAB), share))
for (Lk in names(LAB)) {
  p5 <- unlist(lapply(EFP, function(e) lapply(WINS, function(w) ggplot(SHR[learner == LAB[[Lk]] & response == e & window == w],
      aes(m, metric, fill = metric)) + geom_col(width = 0.7, show.legend = FALSE) +
      geom_errorbarh(aes(xmin = m - se, xmax = m + se), height = 0, linewidth = 0.3) +
      scale_fill_manual(values = FAM_COL) + scale_x_continuous(limits = c(0, 45), expand = c(0, 0)) +
      labs(x = if (e == EFP[4]) "Share of the mortality block's |SHAP| (%)" else NULL, y = NULL,
           title = if (w == "12m") efp_title(e) else NULL, subtitle = if (e == EFP[1]) WLAB[[w]] else NULL) +
      theme_pub() + theme(axis.text.y = if (w == "12m") element_text() else element_blank()))), recursive = FALSE)
  save_pub(wrap_plots(p5, ncol = 2) + plot_annotation(tag_levels = "a"), file.path(OUT, sprintf("mortality_block_%s", Lk)), W15, 150)
}
D <- rbindlist(lapply(c("XGB", "LGB"), function(L) fread(sprintf("derived_tables/paper_v5_nothresh/dropcol/%s_dropcol_rmse.csv", L))))
w <- dcast(D, learner + window + response ~ config, value.var = "overall_rmse")
U <- rbindlist(lapply(names(FAM), function(f) w[, .(learner = LAB[learner], window, response, metric = FAM[[f]],
       unique = 100 * (get(paste0("minus:", f)) / `C+T+M` - 1), alone = 100 * (1 - get(paste0("only:", f)) / `C+T`))]))
agg <- function(d, v) d[, .(m = mean(get(v)), lo = min(get(v)), hi = max(get(v))), by = .(learner, response, metric)]
A <- SHR[, .(m = mean(m), lo = min(m), hi = max(m)), by = .(learner, response, metric = as.character(metric))]
DC <- rbind(A[, panel := "SHAP share in the block (%)"], agg(U, "unique")[, panel := "RMSE increase when removed (%)"],
            agg(U, "alone")[, panel := "RMSE decrease when added alone to C+T (%)"])
DC[, `:=`(metric = factor(metric, rev(FAM)), learner = factor(learner, c("AutoGluon", "XGBoost", "LightGBM")))]
PN3 <- unique(DC$panel)
pd <- position_dodge(0.75)
p6 <- unlist(lapply(EFP, function(e) lapply(PN3, function(pn) ggplot(DC[response == e & panel == pn], aes(m, metric, fill = learner)) +
    geom_vline(xintercept = 0, colour = PAL$zero, linewidth = 0.25) +
    geom_col(width = 0.7, position = pd, show.legend = pn == PN3[1] && e == EFP[1]) +
    geom_errorbarh(aes(xmin = lo, xmax = hi), height = 0, linewidth = 0.25, position = pd, show.legend = FALSE) +
    scale_fill_manual(values = PAL$learner, name = NULL, drop = FALSE) +
    labs(x = if (e == EFP[4]) pn else NULL, y = if (pn == PN3[1]) efp_title(e) else NULL) + theme_pub() +
    theme(axis.text.y = if (pn == PN3[1]) element_text() else element_blank()))), recursive = FALSE)
save_pub(wrap_plots(p6, ncol = 3, guides = "collect") + plot_annotation(tag_levels = "a") & theme(legend.position = "bottom"),
         file.path(OUT, "dropcol"), W2, 150)

# ---- 6. robustness of the slopes per +10 % relative mortality ---------------------------------------------------
R <- fread("plots/paper_v5_nothresh/dose_response/dose_response_robustness_AG.csv")
ML <- c("Mixed model", "Site-block bootstrap", "Leave one site out (range)", "Without the 3 most extreme sites")
G <- rbind(
  R[, .(response, window, test, method = ML[1], est = slope, lo = slope - 1.96 * se_lme, hi = slope + 1.96 * se_lme, sig = q_lme < 0.05)],
  R[, .(response, window, test, method = ML[2], est = slope, lo = boot_lo, hi = boot_hi, sig = q_boot < 0.05)],
  R[, .(response, window, test, method = ML[3], est = slope, lo = loso_min, hi = loso_max, sig = loso_share_p05 >= 0.95)],
  R[, .(response, window, test, method = ML[4], est = noext_slope, lo = noext_slope - 1.96 * noext_se, hi = noext_slope + 1.96 * noext_se, sig = q_noext < 0.05)])
TL <- c(t1a = "Error change", t2a_CT = "Residual, C+T", t2a_CTM = "Residual, C+T+M")
G[, `:=`(method = factor(method, ML), y = match(window, WINS) + (match(method, ML) - 2.5) * -0.17)]
MC <- setNames(c("#000000", "#E69F00", "#56B4E9", "#009E73"), ML)
p7 <- unlist(lapply(names(TL), function(t) lapply(EFP, function(e) ggplot(G[test == t & response == e], aes(est, y, colour = method)) +
    geom_vline(xintercept = 0, colour = PAL$zero, linewidth = 0.25) +
    geom_segment(aes(x = lo, xend = hi, yend = y), linewidth = 0.4) +
    geom_point(aes(shape = sig), size = 1.1, fill = "white", stroke = 0.4) +
    scale_shape_manual(values = c(`TRUE` = 16, `FALSE` = 21), limits = c(TRUE, FALSE), labels = c("significant", "not significant"), name = NULL) +
    scale_x_continuous(n.breaks = 4) +
    scale_colour_manual(values = MC, name = NULL) +
    scale_y_continuous(breaks = 1:2, labels = WLAB, expand = expansion(add = 0.45)) +
    guides(shape = if (t == "t1a" && e == EFP[1]) guide_legend() else "none") +
    labs(x = if (t == "t2a_CTM") "Slope per +10 % relative mortality" else NULL, y = if (e == EFP[1]) TL[[t]] else NULL,
         title = if (t == "t1a") efp_title(e) else NULL) +
    theme_pub() + theme(axis.text.y = if (e == EFP[1]) element_text() else element_blank()))), recursive = FALSE)
save_pub(wrap_plots(p7, ncol = 4, guides = "collect") + plot_annotation(tag_levels = "a") &
           theme(legend.position = "bottom", legend.box = "vertical"), file.path(OUT, "dose_robustness_AG"), W2, 110)

# ---- 7. every site-year with the mixed-model slopes --------------------------------------------------------------
cd <- paper_class_data()[, .(SITE_ID, YEAR, tmc = relative_disturbance_cls)]
wy <- dcast(P[learner == "AutoGluon"], response + window + SITE_ID + YEAR + observed ~ mod, value.var = "predicted")
wy <- merge(wy, cd, by = c("SITE_ID", "YEAR"))[is.finite(M3) & is.finite(M4) & is.finite(tmc)]
wy[, `:=`(r3 = observed - M3, r4 = observed - M4)][, dd := abs(r4) - abs(r3)]
DR <- fread("plots/paper_v5_nothresh/dose_response/dose_response_AG.csv")
xs <- seq(0, max(wy$tmc), length.out = 50)
qs <- function(q) stars(q)
p8 <- list()
for (w in WINS) for (e in EFP) {
  z <- wy[response == e & window == w]; r <- DR[response == e & window == w]
  lim <- quantile(z$dd, c(0.01, 0.99))
  p8[[length(p8) + 1]] <- ggplot(z, aes(tmc, dd)) + geom_hline(yintercept = 0, colour = PAL$zero, linewidth = 0.25, linetype = "22") +
    geom_point(size = 0.35, alpha = 0.5, colour = "#7F7F7F", stroke = 0) +
    geom_line(data = data.table(tmc = xs, dd = r$t1a_int + r$t1a_slope * xs / 10), colour = "black", linewidth = 0.5) +
    annotate("text", x = Inf, y = Inf, hjust = 1.05, vjust = 1.3, size = 5 / .pt,
             label = sprintf("%s%.3f per 10 %%%s", ifelse(r$t1a_slope < 0, "−", "+"), abs(r$t1a_slope), qs(r$t1a_q))) +
    coord_cartesian(ylim = lim) +
    labs(x = NULL, y = if (e == EFP[1]) sprintf("Error change, %s", WLAB[[w]]) else NULL, title = if (w == "12m") efp_title(e) else NULL) +
    theme_pub()
}
for (w in WINS) for (e in EFP) {
  z <- wy[response == e & window == w]; r <- DR[response == e & window == w]
  # difference of the two slopes = trend of the correction (residual C+T+M - residual C+T), mixed model with a site-block
  # bootstrap (500 draws, 43_residual_by_mortality_bin.R); q shown at least 1/500 because the bootstrap cannot resolve less
  tr <- fread(sprintf("plots/paper_v5_nothresh/dose_response/residual_correction_trend_AG_%s.csv", w))[response == e]
  zz <- rbind(z[, .(tmc, y = r3, model = "C+T")], z[, .(tmc, y = r4, model = "C+T+M")])
  fl <- rbind(data.table(tmc = xs, y = r$t2a_int_CT + r$t2a_slope_CT * xs / 10, model = "C+T"),
              data.table(tmc = xs, y = r$t2a_int_CTM + r$t2a_slope_CTM * xs / 10, model = "C+T+M"))
  lim <- quantile(zz$y, c(0.01, 0.99))
  p8[[length(p8) + 1]] <- ggplot(zz, aes(tmc, y, colour = model)) +
    geom_hline(yintercept = 0, colour = PAL$zero, linewidth = 0.25, linetype = "22") +
    geom_point(size = 0.35, alpha = 0.35, stroke = 0) + geom_line(data = fl, linewidth = 0.55) +
    annotate("text", x = Inf, y = Inf, hjust = 1.05, vjust = 1.2, size = 5 / .pt, lineheight = 0.9,
             label = gsub("-", "−", sprintf("C+T %+.3f%s\nC+T+M %+.3f%s\n\u0394 %+.3f%s", r$t2a_slope_CT, qs(r$t2a_q_CT), r$t2a_slope_CTM,
                     qs(r$t2a_q_CTM), tr$slope, qs(pmax(tr$q_boot, 1 / 500))))) +
    scale_colour_manual(values = PAL$model, labels = c("Without mortality (C+T)", "With mortality (C+T+M)"), name = NULL) +
    coord_cartesian(ylim = lim) +
    labs(x = if (w == "24m") "Relative mortality of the site-year (%)" else NULL,
         y = if (e == EFP[1]) sprintf("Residual, %s", WLAB[[w]]) else NULL) + theme_pub()
}
save_pub(wrap_plots(p8, ncol = 4, guides = "collect") + plot_annotation(tag_levels = "a") & theme(legend.position = "bottom"),
         file.path(OUT, "dose_siteyear_AG"), W2, 165)

# ---- 8. Fig. 2 with the EFP-specific climate block ---------------------------------------------------------------
for (w in WINS) {
  r5 <- fread(sprintf("plots/paper_v5_nothresh/dose_response/residual_by_mortality_AG_efpclim_%s.csv", w))
  c5 <- fread(sprintf("plots/paper_v5_nothresh/dose_response/residual_correction_by_mortality_AG_efpclim_%s.csv", w))
  r5[, model := fifelse(grepl("^Without", model), "C+T", "C+T+M")]
  BL <- c("0-10 %", "10-20 %", "20-30 %", "> 30 %"); r5[, bin := factor(bin, BL)]; c5[, bin := factor(bin, BL)]
  nn <- r5[model == "C+T", .(response, bin, lab = sprintf("%d\n(%d)", n_sy, n_sites))]
  pdd <- position_dodge(0.45)
  p9 <- lapply(EFP, function(e) {
    d <- r5[response == e]; yr <- range(c(d$lo, d$hi)); pad <- diff(yr) * 0.12
    ggplot(d, aes(bin, mean, colour = model, group = model)) +
      geom_hline(yintercept = 0, colour = PAL$zero, linewidth = 0.25) +
      geom_line(position = pdd, linewidth = 0.35, alpha = 0.7) +
      geom_errorbar(aes(ymin = lo, ymax = hi), width = 0, linewidth = 0.4, position = pdd) + geom_point(position = pdd, size = 1.1) +
      geom_text(data = c5[response == e], aes(bin, yr[2] + pad * 0.9, label = stars(q_perm)), inherit.aes = FALSE) +
      geom_text(data = nn[response == e], aes(bin, yr[1] - pad * 1.15, label = lab), inherit.aes = FALSE, size = 5 / .pt, lineheight = 0.85, colour = "#4D4D4D") +
      scale_colour_manual(values = PAL$model, labels = c("C+T" = "Without mortality (C+T)", "C+T+M" = "With mortality (C+T+M)"), name = NULL) +
      scale_y_continuous(limits = c(yr[1] - pad * 1.9, yr[2] + pad * 1.5)) +
      labs(x = "Site-year relative mortality", y = if (e == EFP[1]) "Mean residual (observed − predicted)" else NULL, title = efp_title(e)) +
      theme_pub() + theme(axis.text.x = element_text(size = 5.5))
  })
  save_pub(wrap_plots(p9, nrow = 1, guides = "collect") + plot_annotation(tag_levels = "a") & theme(legend.position = "bottom"),
           file.path(OUT, sprintf("residual_efpclim_AG_%s", w)), W2, 66)
}
cat("appendix figures ->", OUT, "\n")
