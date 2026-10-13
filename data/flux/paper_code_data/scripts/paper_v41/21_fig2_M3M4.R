# PORT of scripts/plot_draft_fig2_M3M4.R for the paper_v41 dataset (paths and labels only).
#!/usr/bin/env Rscript
# ============================================================================
# Coauthor draft — Fig 2: effect of adding disturbance (M3 -> M4) on per-site RMSE
#
# Filtered from the existing V10 results. Scope fixed for this draft:
#   dataset  tree cover >=30 % (93 sites)
#   models   M3 (C+T) vs M4 (C+T+D) only
#   EFPs     GPPsat, NEPmax, ETmax, WUE   (uWUE dropped)
#   learners RF / XGBoost / LightGBM, all Optuna-tuned
#   windows  12 m and 24 m, under BOTH leave-one-site-out and repeated CV
#
# Style follows plots/V10 Section 3 (paired violin, same colours and statistics).
# ============================================================================
suppressMessages({library(data.table); library(ggplot2); library(ggh4x)})
setwd(Sys.getenv("PAPER_ROOT", "."))

source("scripts/paper_v41/paper_common.R")   # paper_v41 port: paths only, style/statistics unchanged
OUT <- PAPER_FIG
dir.create(OUT, showWarnings = FALSE, recursive = TRUE)

DARK_BG <- "white"; PANEL_BG <- "white"; GRID_COL <- "#D9D9D9"
TEXT_COL <- "#111111"; AXIS_COL <- "#444444"
COL_WO <- "#22C4E0"; COL_W <- "#E8257A"
COL_UP <- "#2ECC71"; COL_NC <- "#9AA0A6"; COL_DN <- "#E74C3C"

EFP_LABELS <- RESP_UNITS   # active response set (paper_common.R)
PN <- paper_n()

# Optional site subset:  Rscript 21_fig2_M3M4.R [high_reldist | high_relmort]
# = the same figure and statistics for HIGH-disturbance sites only (natural break as in Fig 3b / Fig 4:
# site maximum across years > mean + 0.5 SD over sites, 500 m). Models are unchanged (trained on all
# sites); only the sites shown and tested differ. Without an argument: all sites, original file names.
SUBSET <- commandArgs(trailingOnly = TRUE)[1]
if (is.na(SUBSET)) SUBSET <- "all"
stopifnot(SUBSET %in% c("all", "high_reldist", "high_relmort"))
KEEP_SITES <- NULL; SUB_TXT <- ""; FILE_TAG <- ""
if (SUBSET != "all") {
  mcol <- c(high_reldist = "relative_disturbance_cls", high_relmort = "relative_mortality_cls")[[SUBSET]]
  mlab <- c(high_reldist = "total mortality", high_relmort = "relative deadwood")[[SUBSET]]
  sm <- paper_class_data()[, c("SITE_ID", mcol), with = FALSE]
  setnames(sm, mcol, "v")
  sm <- sm[, .(v = suppressWarnings(max(v, na.rm = TRUE))), by = SITE_ID][is.finite(v)]
  nb <- mean(sm$v) + 0.5 * sd(sm$v)
  KEEP_SITES <- sm[v > nb, SITE_ID]
  SUB_TXT <- sprintf("HIGH-mortality sites only: %d of %d sites (site-maximum %s, %s, > %.1f %% = mean + 0.5 SD); models trained on all sites\n",
                     length(KEEP_SITES), PN[["sites"]], mlab, CLASS_LAB, nb)
  FILE_TAG <- paste0("_", FILE_KEY[[SUBSET]], CLASS_TAG)
}

# env FIG2_STAT (2026-10-09, coauthor draft): "wilcoxon" (default: per-site RMSE, median change, paired Wilcoxon) or
# "mae_perm": per-site MAE, geometric-mean change and a one-sided paired sign-flip permutation test on the per-site
# log error ratios (100 000 flips), as Table 2 of the draft; file names get the suffix _MAE_perm
FIG2_STAT <- Sys.getenv("FIG2_STAT", "wilcoxon"); stopifnot(FIG2_STAT %in% c("wilcoxon", "mae_perm"))
MET <- if (FIG2_STAT == "mae_perm") "MAE" else "RMSE"
if (FIG2_STAT == "mae_perm") FILE_TAG <- paste0(FILE_TAG, "_MAE_perm")
perm_p <- function(d, n_perm = 100000L) {   # P(mean of sign-flipped values <= observed mean), +1 correction
  set.seed(42); n <- length(d)
  s <- matrix(sample(c(-1, 1), n_perm * n, replace = TRUE), n_perm, n)
  (1 + sum(as.vector(s %*% d) / n <= mean(d))) / (n_perm + 1)
}

LEARNERS <- list(   # random forest dropped at the user's request
  # 2026-09-09: repointed to the lag2-corrected 24m data (true_24m/*) - the
  # production dirs' 24m branch lacked disturbance lag2 entirely. 12m rows in
  # true_24m are byte-identical to production; see scripts/build_true24m_folders.R.
  list(lab = "XGBoost (Optuna)",  fam = "XGB"),
  list(lab = "LightGBM (Optuna)", fam = "LGB"))

site_rmse <- function(f) {
  p <- fread(f)
  if (MET == "MAE") return(p[, .(rmse = mean(abs(observed - predicted))), by = .(model, response, SITE_ID)])
  p[, .(rmse = sqrt(mean((observed - predicted)^2))), by = .(model, response, SITE_ID)]
}

rows <- list(); labs <- list(); k <- 1; lk <- 1
for (L in LEARNERS) {
  for (cv in c("Repeated CV")) {   # LOO dropped at the user's request
    sr <- site_rmse(file.path(PAPER_OUT, sprintf("%s_predictions_repCV.csv", L$fam)))
    for (w in c("12m", "24m")) {
      for (resp in EFP_ORDER) {
        a <- sr[model == paste0("M3_", w) & response == resp, .(SITE_ID, wo = rmse)]
        b <- sr[model == paste0("M4_", w) & response == resp, .(SITE_ID, wd = rmse)]
        pw <- merge(a, b, by = "SITE_ID")          # pair explicitly on site
        pw <- pw[is.finite(wo) & is.finite(wd)]
        if (!is.null(KEEP_SITES)) pw <- pw[SITE_ID %in% KEEP_SITES]
        if (!nrow(pw)) next
        grp <- w
        rows[[k]] <- data.table(response = resp, learner = L$lab, grp = grp,
                                model_type = "Without mortality", rmse = pw$wo); k <- k + 1
        rows[[k]] <- data.table(response = resp, learner = L$lab, grp = grp,
                                model_type = "With mortality",    rmse = pw$wd); k <- k + 1
        if (FIG2_STAT == "mae_perm") {
          pct  <- 100 * (exp(mean(log(pw$wd / pw$wo))) - 1)
          pval <- perm_p(log(pw$wd / pw$wo))
        } else {
          pct  <- median((pw$wd - pw$wo) / pw$wo * 100)
          pval <- wilcox.test(pw$wd, pw$wo, paired = TRUE, alternative = "less", exact = FALSE)$p.value
        }
        labs[[lk]] <- data.table(response = resp, learner = L$lab, grp = grp,
                                 pct = pct, p = pval,
                                 imp = mean(pw$wd < pw$wo) * 100); lk <- lk + 1
      }
    }
  }
}
dt   <- rbindlist(rows)
labs <- rbindlist(labs)
labs[, q := p.adjust(p, method = "BH")]            # FDR across every test in the figure
labs[, sig := ifelse(q < 0.001, "***", ifelse(q < 0.01, "**", ifelse(q < 0.05, "*", "ns")))]
labs[, lcol := ifelse(pct < 0 & q < 0.05, COL_UP, ifelse(pct > 0, COL_DN, COL_NC))]
labs[, label := sprintf("%+.1f%%%s\n%.0f%%↓", pct, sig, imp)]

dt[, response := factor(response, levels = EFP_ORDER)]
labs[, response := factor(response, levels = EFP_ORDER)]
LEV <- c("12m", "24m")
dt[, grp := factor(grp, levels = LEV)]; labs[, grp := factor(grp, levels = LEV)]
dt[, learner := factor(learner, levels = sapply(LEARNERS, `[[`, "lab"))]
labs[, learner := factor(learner, levels = sapply(LEARNERS, `[[`, "lab"))]
dt[, model_type := factor(model_type, levels = c("Without mortality", "With mortality"))]

# view limit: p95 per EFP row, applied as a ZOOM (oob_keep) so densities and all
# statistics are still computed on every site
ymax <- dt[, .(ytop = quantile(rmse, 0.95, na.rm = TRUE)), by = response]
labs <- merge(labs, ymax, by = "response")
labs[, ylab := ytop * 0.94]

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
  geom_violin(position = position_dodge(width = 0.8), colour = NA,
              width = 0.78, alpha = 0.75, trim = TRUE) +
  geom_boxplot(aes(group = interaction(grp, model_type)),
               position = position_dodge(width = 0.8), width = 0.15,
               outlier.shape = NA, colour = "#222222", fill = NA, linewidth = 0.3) +
  geom_text(data = labs, aes(x = grp, y = ylab, label = label, colour = lcol),
            inherit.aes = FALSE, size = 2.5, fontface = "bold") +
  scale_fill_manual(values = setNames(c(COL_WO, COL_W), c("Without mortality", "With mortality"))) +
  scale_colour_identity() +
  facet_grid(response ~ learner, scales = "free_y",
             labeller = labeller(response = EFP_LABELS), switch = "y") +
  ggh4x::facetted_pos_scales(
    y = lapply(EFP_ORDER, function(r)
      scale_y_continuous(limits = c(0, ymax[response == r, ytop] * 1.08),
                         oob = scales::oob_keep))) +
  coord_cartesian(clip = "on") +
  labs(x = NULL, y = NULL,
       title = paste0("Effect of adding mortality predictors on per-site ", MET, if (SUBSET != "all") " — high-mortality sites" else ""),
       subtitle = paste0(SUB_TXT, "Tree cover ≥30 % (", PN[["sites"]], " sites, ", PN[["sy"]], " site-years) · ", PAPER_MAPS, " · repeated CV (3×80 %) · C+T vs C+T+M · cyan = without mortality, pink = with mortality\n",
                         if (FIG2_STAT == "mae_perm") "% = geometric-mean PAIRED change in per-site MAE (negative = less error) · stars = one-sided paired permutation test on the log MAE ratios, BH-FDR across the figure (*** q<0.001, ** q<0.01, * q<0.05)\n"
                         else "% = median PAIRED change in per-site RMSE (negative = less error) · stars = one-sided paired Wilcoxon, BH-FDR across the figure (*** q<0.001, ** q<0.01, * q<0.05)\n",
                         sprintf("%%↓ = share of sites whose %s improved · y axis zoomed to the 95th percentile (no data removed)", MET))) +
  th + theme(strip.placement = "outside") +
  guides(colour = "none", fill = guide_legend(override.aes = list(alpha = 0.9)))

ggsave(file.path(OUT, sprintf("fig2%s_mortality_effect.png", FILE_TAG)), p, width = 15, height = 12, dpi = 300, bg = DARK_BG)
ggsave(file.path(OUT, sprintf("fig2%s_mortality_effect.pdf", FILE_TAG)), p, width = 15, height = 12, bg = DARK_BG)
fwrite(labs[, .(learner, response, grp, median_pct = round(pct,2), sites_improved = round(imp,1),
                p = signif(p,3), q = signif(q,3), sig)],
       file.path(OUT, sprintf("fig2%s_statistics.csv", FILE_TAG)))
cat(sprintf("\nFig 2 saved | %d panels | %d tests\n", uniqueN(dt$response) * uniqueN(dt$learner), nrow(labs)))
