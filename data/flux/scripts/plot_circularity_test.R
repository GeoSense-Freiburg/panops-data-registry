#!/usr/bin/env Rscript
# ============================================================================
# Figure for the circularity check (scripts/circularity_test_climate_exclusion.R):
# does the disturbance gain (M3 -> M4 dRMSE%) survive removing climate
# predictors embedded in each EFP's own calculation?
#
# Scenarios (x-axis): baseline (all climate) -> own_vars (drop that EFP's own
# derivation variables) -> precip (drop TA/VPD/SW_IN, only P remains).
# Pools every run committed so far: XGB/LOSO has all 3 scenarios; XGB/repeated
# and LGB/repeated only have own_vars (baseline/precip not yet run under
# repeated CV) - shown as-is, gaps are not filled in or implied.
# ============================================================================
suppressMessages({library(data.table); library(ggplot2)})
setwd("/mnt/gsdata/projects/panops/panops-data-registry/data/flux")
OUT <- "plots/V10/circularity_test"

loso  <- fread(file.path(OUT, "circularity_test_XGB_LOSO.csv"))
loso[, cv_mode := "loso"]
loso_perm <- fread(file.path(OUT, "circularity_test_XGB_LOSO_perm99_own_vars-precip.csv"))
loso_perm[, cv_mode := "loso"]
xgb_rep <- fread(file.path(OUT, "circularity_test_XGB_repeated_perm99_own_vars.csv"))
lgb_rep <- fread(file.path(OUT, "circularity_test_LGB_repeated_perm99_own_vars.csv"))

# LOSO baseline never had a permutation run - keep it, with p_perm = NA
loso_baseline <- loso[scenario == "baseline"]
dt <- rbindlist(list(loso_baseline, loso_perm, xgb_rep, lgb_rep), fill = TRUE, use.names = TRUE)

EFP_ORDER <- c("GPPsat","NEPmax","ETmax","WUE")
SCN_ORDER <- c("baseline","own_vars","precip")
SCN_LAB <- c(baseline = "all climate", own_vars = "own-derivation\nvars dropped", precip = "precip only")
dt[, response := factor(response, EFP_ORDER)]
dt[, scenario := factor(scenario, SCN_ORDER)]
dt[, config := factor(sprintf("%s, %s", learner, cv_mode),
                      levels = c("XGB, loso", "XGB, repeated", "LGB, repeated"),
                      labels = c("XGBoost, LOSO", "XGBoost, repeated (3x80%)", "LightGBM, repeated (3x80%)"))]
dt[, sig := fifelse(is.na(p_perm), "no permutation run", fifelse(p_perm < 0.05, "p < 0.05", "n.s."))]
dt[, sig := factor(sig, levels = c("p < 0.05", "n.s.", "no permutation run"))]
dt[, plab := fifelse(is.na(p_perm), "", sprintf("p=%.2f", p_perm))]
# stagger label height per config so labels sharing an x-position don't collide
dt[, lab_vjust := c("XGBoost, LOSO" = -3.8, "XGBoost, repeated (3x80%)" = -2.3,
                     "LightGBM, repeated (3x80%)" = -0.8)[as.character(config)]]

BG <- "white"; GRID <- "#D9D9D9"; TXT <- "#111111"; AX <- "#444444"
CFG_COL <- c("XGBoost, LOSO" = "#2E6F40", "XGBoost, repeated (3x80%)" = "#5AA469", "LightGBM, repeated (3x80%)" = "#B0413E")

th <- theme_bw(base_size = 11) + theme(
  plot.background = element_rect(fill = BG, colour = NA),
  panel.background = element_rect(fill = BG, colour = NA),
  panel.border = element_rect(colour = GRID, fill = NA, linewidth = 0.4),
  panel.grid.major.x = element_blank(),
  panel.grid.minor = element_blank(),
  strip.background = element_rect(fill = "#EFEFEF", colour = GRID),
  strip.text = element_text(colour = TXT, size = 10, face = "bold"),
  axis.text = element_text(colour = AX, size = 8.5),
  axis.title = element_text(colour = AX, size = 10, face = "bold"),
  plot.title = element_text(colour = TXT, size = 13, face = "bold"),
  plot.subtitle = element_text(colour = AX, size = 9),
  plot.caption = element_text(colour = AX, size = 8, hjust = 0),
  legend.position = "bottom", legend.box = "vertical", legend.title = element_text(size = 9))

p <- ggplot(dt, aes(x = scenario, y = delta_rmse_pct, colour = config, group = config)) +
  geom_hline(yintercept = 0, colour = "#888888", linewidth = 0.35, linetype = "dashed") +
  geom_line(linewidth = 0.5, alpha = 0.6, position = position_dodge(width = 0.35)) +
  geom_point(aes(shape = sig), size = 3, position = position_dodge(width = 0.35)) +
  geom_text(aes(label = plab, vjust = lab_vjust), position = position_dodge(width = 0.35),
            size = 2.5, fontface = "bold", show.legend = FALSE) +
  scale_colour_manual(values = CFG_COL, name = NULL) +
  scale_shape_manual(values = c("p < 0.05" = 16, "n.s." = 17, "no permutation run" = 1), name = "permutation test") +
  scale_x_discrete(labels = SCN_LAB) +
  scale_y_continuous(expand = expansion(mult = c(0.12, 0.28))) +
  facet_wrap(~response, nrow = 1, scales = "free_y") +
  labs(title = "Circularity check: does the disturbance gain survive removing each EFP's own climate predictors?",
       subtitle = "tc>=30 (93 sites / 395 site-years), Optuna hyperparameters unchanged | y = M3->M4 RMSE change (negative = disturbance improves prediction)",
       x = NULL, y = "Delta RMSE (%), M3 -> M4",
       caption = "own-derivation vars: GPPsat -SW_IN/TA, NEPmax -SW_IN, ETmax -TA, WUE -TA/VPD. Baseline (XGB, LOSO) has no permutation run (open circle).\nprecip-only and baseline are not yet run under repeated CV - missing points, not zeros. p-values from 99-rep block permutation of the disturbance block.") +
  th

ggsave(file.path(OUT, "circularity_test_summary.png"), p, width = 13, height = 5.5, dpi = 300, bg = BG)
ggsave(file.path(OUT, "circularity_test_summary.pdf"), p, width = 13, height = 5.5, bg = BG)
cat("Saved:", file.path(OUT, "circularity_test_summary.png"), "\n")
