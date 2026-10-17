#!/usr/bin/env Rscript
# ============================================================================
# PORT of scripts/plot_v10_D_block_composition_v2.R for the paper_v41 dataset
# (deadtrees v4.1, 106 sites / 388 site-years, tree cover >= 30 %).
# Composition of the disturbance block, split by measurement type:
#   Mortality (deadwood) - absolute_mortality, relative_mortality,
#                          new_deadwood_gain_pp (+ _thresh)
#   Tree cover loss      - relative_tree_loss_pct (+ _thresh)
#   Combined index       - relative_disturbance (deadwood + cover loss)
# v4.1 has no new_mortality_rate_pct and no mortality_loss_severity_pct.
# D block per window: 12m = current + lag1 (70 vars), 24m = + lag2 (105 vars),
# every metric x 5 buffers. No retraining: re-aggregates the per-variable
# TreeSHAP of the M4 fits ({L}_site_shap_M4.csv), D block renormalised to 100 %.
# Output: plots/paper_v41/disturbance_split/
# ============================================================================
suppressMessages({library(data.table); library(ggplot2)})
setwd(Sys.getenv("PAPER_ROOT", "."))
source("scripts/paper_v41/paper_common.R")
OUT <- file.path(PAPER_FIG, "mortality_split")
dir.create(OUT, showWarnings = FALSE, recursive = TRUE)

BG <- "white"; GRID <- "#D9D9D9"; TXT <- "#111111"; AX <- "#444444"
# AutoGluon (2026-10-08): permutation SHAP of the matching AutoGluon run, same file layout
AG_SHAP <- file.path(PAPER_ROOT, "AutoGluon/outputs", paste0(if (PAPER_RESP_SET == "efp_v5") "efp_v5" else "efp",
                                                          if (PAPER_NO_THRESH) "_nothresh" else ""), "AG_site_shap_M4.csv")
LEARNERS <- list(list(lab = "XGBoost (Optuna)",  fam = "XGB"),
                 list(lab = "LightGBM (Optuna)", fam = "LGB"),
                 list(lab = "AutoGluon (permutation SHAP)", fam = "AG", f = AG_SHAP))
FAM_LAB <- c(absolute_mortality = "Absolute deadwood", relative_mortality = "Relative deadwood",
             new_deadwood_gain_pp = "Deadwood gain", new_deadwood_gain_pp_thresh = "Deadwood gain (thresholded)",
             relative_tree_loss_pct = "Relative tree-cover loss", relative_tree_loss_pct_thresh = "Relative tree-cover loss (thresholded)",
             relative_disturbance = "Relative mortality")

# Spearman rho between the combined index and relative mortality, site-years
# of this dataset (500 m), used in the legend label
dd <- fread(PAPER_DATA, select = c("relative_disturbance_500m", "relative_mortality_500m"))
RHO <- cor(dd$relative_disturbance_500m, dd$relative_mortality_500m, method = "spearman", use = "complete.obs")
CMB <- sprintf("Relative mortality (rho = %.2f with relative deadwood)", RHO)
PD  <- fread(PAPER_DATA, select = "SITE_ID"); N_SITES <- uniqueN(PD$SITE_ID); N_SY <- nrow(PD)
# family = drop the buffer and lag suffix, keep the _thresh distinction
famof <- function(v) sub("_[0-9]+00m(_lag[12])?$", "", v)
concept <- function(f)
  fifelse(grepl("^relative_tree_loss", f),
          "Tree cover loss",
  fifelse(grepl("^relative_disturbance|^mortality_loss_severity", f),
          CMB,
          "Mortality (deadwood)"))   # absolute/relative_mortality, new_deadwood_gain_pp
CC_ORDER <- c("Mortality (deadwood)", "Tree cover loss",
              CMB)
CC_COLS  <- c("#D1495B", "#2A9D8F", "#9AA0A6"); names(CC_COLS) <- CC_ORDER

th <- theme_bw(base_size = 11) + theme(
  plot.background = element_rect(fill = BG, colour = NA),
  panel.background = element_rect(fill = BG, colour = NA),
  panel.border = element_rect(colour = GRID, fill = NA, linewidth = 0.4),
  panel.grid.major.y = element_blank(),
  panel.grid.major.x = element_line(colour = GRID, linewidth = 0.2),
  panel.grid.minor = element_blank(),
  strip.background = element_rect(fill = "#EFEFEF", colour = GRID),
  strip.text = element_text(colour = TXT, size = 10, face = "bold"),
  axis.text = element_text(colour = AX, size = 9),
  axis.title = element_text(colour = AX, size = 10, face = "bold"),
  legend.position = "bottom", legend.title = element_blank(),
  legend.background = element_rect(fill = NA, colour = NA),
  legend.key = element_rect(fill = NA, colour = NA),
  legend.text = element_text(colour = TXT, size = 10),
  plot.title = element_text(colour = TXT, size = 14, face = "bold"),
  plot.subtitle = element_text(colour = AX, size = 9),
  plot.caption = element_text(colour = AX, size = 8, hjust = 0))

tables <- list()
for (L in LEARNERS) {
  f <- if (!is.null(L[["f", exact = TRUE]])) L[["f"]] else file.path(PAPER_OUT, sprintf("%s_site_shap_M4.csv", L$fam))
  if (!file.exists(f)) { message("missing: ", f); next }
  s <- fread(f)[model %in% c("M4_12m", "M4_24m") & response %in% EFP_ORDER]
  s <- s[group == "Disturbance"]
  stopifnot(nrow(s) > 0)
  s[, family := famof(variable)][, cc := concept(family)]

  s[, window := factor(sub("M4_", "", model), levels = c("12m", "24m"),
                       labels = c("12 months", "24 months"))]
  # variable counts are window-dependent (24m adds lag2)
  nvar <- unique(s[, .(window, variable, family, cc)])
  nv_fam <- nvar[, .(n_vars = .N), by = .(window, family, cc)]
  nv_cc  <- nvar[, .(n_vars = .N), by = .(window, cc)]
  NV <- nvar[, .N, by = .(window, cc)]
  nv_txt <- function(w) paste(sprintf("%s %d", sub(" \\(rho.*", "", NV[window == w, cc]), NV[window == w, N]), collapse = ", ")
  tot <- s[, .(tot = sum(mean_abs_shap)), by = .(window, response, test_site)]

  agg <- function(by) {
    a <- s[, .(v = sum(mean_abs_shap)), by = c("window", "response", "test_site", by)]
    a <- merge(a, tot, by = c("window", "response", "test_site"))
    a[, share := 100 * v / tot]
    a[, .(share = mean(share), se = sd(share) / sqrt(.N)),
      by = c("window", "response", by)]
  }
  by_cc  <- merge(agg("cc"),     nv_cc,  by = c("window", "cc"))
  by_fam <- merge(agg("family"), nv_fam, by = c("window", "family"))
  by_cc[,  per_var := share / n_vars]
  by_fam[, per_var := share / n_vars]
  by_cc[,  learner := L$lab]; by_fam[, learner := L$lab]
  tables[[L$lab]] <- list(cc = by_cc, fam = by_fam)

  ## ---- Figure 1: two sub-blocks, both normalisations ----------------------
  m <- melt(by_cc, id.vars = c("window", "response", "cc", "n_vars"),
            measure.vars = c("share", "per_var"), variable.name = "norm")
  m[, norm := factor(norm, c("share", "per_var"),
                     c("% of D-block |SHAP|",
                       "per variable  (% / variable)"))]
  m[, response := factor(response, EFP_ORDER)]
  m[, cc := factor(cc, CC_ORDER)]
  m[, lab := sprintf("%s (%d vars)", sub(" /.*| \\(rho.*", "", cc), n_vars)]

  p <- ggplot(m, aes(response, value, fill = cc)) +
    geom_col(position = position_dodge(0.78), width = 0.7) +
    geom_text(aes(label = sprintf("%.1f", value)), position = position_dodge(0.78),
              vjust = -0.35, size = 2.5, colour = AX) +
    facet_grid(norm ~ window, scales = "free_y", switch = "y") +
    scale_fill_manual(values = CC_COLS) +
    scale_y_continuous(expand = expansion(mult = c(0, 0.14))) +
    labs(title = "What kind of mortality information does the model use?",
         subtitle = sprintf("%s · C+T+M · %s, %s · tree cover >= 30%% (%d sites, %d site-years)\nMortality block renormalised to 100%%; mean over the %d sites",
                            L$lab, .sub, PAPER_MAPS, N_SITES, N_SY, N_SITES),
         x = NULL, y = NULL,
         caption = sprintf(paste0("Top row: total share of the mortality signal. Variables per sub-block: 12m %s; 24m %s.
",
                          "Bottom row divides by the number of variables. Every metric enters at 5 buffers x current + lag1 (12m) or + lag2 (24m).
",
                          "|SHAP| is magnitude only, not direction."), nv_txt("12 months"), nv_txt("24 months"))) +
    th + theme(panel.grid.major.x = element_blank(),
               panel.grid.major.y = element_line(colour = GRID, linewidth = 0.2),
               strip.placement = "outside",
               strip.background.y = element_blank(),
               strip.text.y.left = element_text(angle = 90, size = 10, face = "bold",
                                                colour = TXT, margin = margin(r = 6)),
               plot.margin = margin(10, 14, 8, 10))
  ggsave(sprintf("%s/mortality_composition_concept_%s.png", OUT, L$fam), p,
         width = 10, height = 7.5, dpi = 320, bg = BG)

  ## ---- Figure 2: metric-family detail -------------------------------------
  by_fam[, response := factor(response, EFP_ORDER)]
  by_fam[, cc := factor(cc, CC_ORDER)]
  ord <- by_fam[, .(m = mean(share)), by = family][order(m)]$family
  by_fam[, family := factor(family, ord)]
  p2 <- ggplot(by_fam, aes(family, share, fill = cc)) +
    geom_col(width = 0.72) +
    geom_errorbar(aes(ymin = share - se, ymax = share + se), width = 0, linewidth = 0.3,
                  colour = AX) +
    geom_text(aes(label = sprintf("%.1f  (n=%d)", share, n_vars)), hjust = -0.1,
              size = 2.3, colour = AX) +
    coord_flip() +
    scale_x_discrete(labels = function(x) ifelse(x %in% names(FAM_LAB), FAM_LAB[x], x)) +
    facet_grid(response ~ window) +
    scale_fill_manual(values = CC_COLS) +
    scale_y_continuous(expand = expansion(mult = c(0, 0.30))) +
    labs(title = "Mortality block, metric by metric",
         subtitle = sprintf("%s · C+T+M · %s (%d sites) · share of the mortality-block |SHAP| (%%)\nn = variables contributed by that metric (5 buffers x current + lag1 at 12m, + lag2 at 24m)",
                            L$lab, .sub, N_SITES),
         x = NULL, y = "% of mortality-block |SHAP|") +
    th
  ggsave(sprintf("%s/mortality_composition_family_%s.png", OUT, L$fam), p2,
         width = 11, height = 8, dpi = 320, bg = BG)
  message("  written: ", L$fam)
}

cc  <- rbindlist(lapply(tables, `[[`, "cc"))
fam <- rbindlist(lapply(tables, `[[`, "fam"))
fwrite(cc,  sprintf("%s/mortality_composition_by_concept.csv", OUT))
fwrite(fam, sprintf("%s/mortality_composition_by_family.csv", OUT))

cat("\n=== XGBoost (Optuna), share of the disturbance block (%) ===\n")
print(dcast(cc[learner == "XGBoost (Optuna)"], window + response ~ cc,
            value.var = "share"), digits = 3)
cat("\n=== same, per variable ===\n")
print(dcast(cc[learner == "XGBoost (Optuna)"], window + response ~ cc,
            value.var = "per_var"), digits = 3)
cat("\nfigures + csv in", OUT, "\n")
