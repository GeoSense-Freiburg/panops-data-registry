#!/usr/bin/env Rscript
# ============================================================================
# Simplified circularity-check figure for external sharing (Miguel email,
# 2026-09-22): actual RMSE (not percent change), XGBoost + LightGBM under
# repeated CV only (drops the XGB/LOSO series and the precip-only scenario,
# which were never run for repeated CV anyway), no permutation p-values,
# descriptive labels instead of "M3"/"M4". Complements, does not replace,
# circularity_test_summary.png (the full technical version).
# ============================================================================
suppressMessages({library(data.table); library(ggplot2)})
setwd("/mnt/gsdata/projects/panops/panops-data-registry/data/flux")
OUT <- "plots/V10/circularity_test"  # output location unchanged

xgb <- fread(file.path(OUT, "circularity_test_XGB_repeated_perm99_own_vars.csv"))
lgb <- fread(file.path(OUT, "circularity_test_LGB_repeated_perm99_own_vars.csv"))
dt <- rbindlist(list(xgb, lgb))

EFP_ORDER <- c("GPPsat","NEPmax","ETmax","WUE")
dt[, response := factor(response, EFP_ORDER)]
dt[, learner := factor(learner, c("XGB","LGB"), labels = c("XGBoost","LightGBM"))]

long <- rbindlist(list(
  dt[, .(response, learner, model = "Without mortality\n(climate + traits)", rmse = rmse_M3)],
  dt[, .(response, learner, model = "With mortality\n(+ disturbance)", rmse = rmse_M4)]
))
long[, model := factor(model, c("Without mortality\n(climate + traits)", "With mortality\n(+ disturbance)"))]

BG <- "white"; GRID <- "#D9D9D9"; TXT <- "#111111"; AX <- "#444444"
LEARNER_COL <- c("XGBoost" = "#2E6F40", "LightGBM" = "#B0413E")
th <- theme_bw(base_size = 12) + theme(
  plot.background = element_rect(fill = BG, colour = NA),
  panel.background = element_rect(fill = BG, colour = NA),
  panel.border = element_rect(colour = GRID, fill = NA, linewidth = 0.4),
  panel.grid.major.x = element_blank(), panel.grid.minor = element_blank(),
  strip.background = element_rect(fill = "#EFEFEF", colour = GRID),
  strip.text = element_text(colour = TXT, size = 11, face = "bold"),
  axis.text = element_text(colour = AX, size = 9.5),
  axis.title = element_text(colour = AX, size = 11, face = "bold"),
  plot.title = element_text(colour = TXT, size = 14, face = "bold"),
  plot.subtitle = element_text(colour = AX, size = 9.5),
  plot.caption = element_text(colour = AX, size = 8, hjust = 0),
  legend.position = "bottom")

p <- ggplot(long, aes(x = model, y = rmse, fill = learner)) +
  geom_col(position = position_dodge(width = 0.65), width = 0.6) +
  scale_fill_manual(values = LEARNER_COL, name = NULL) +
  facet_wrap(~response, nrow = 1, scales = "free_y") +
  labs(title = "Does mortality improve prediction, even without the climate variables each EFP is derived from?",
       subtitle = "tc>=30, 93 sites, repeated leave-one-site-out cross-validation",
       x = NULL, y = "Prediction error (RMSE, lower is better)",
       caption = "Each EFP's own climate driver is excluded from both models (GPPsat: radiation & temperature; NEPmax: radiation; ETmax: temperature; WUE: temperature & VPD),\nso the comparison cannot be explained by that circularity. Lower bar = better prediction.") +
  th

ggsave(file.path(OUT, "circularity_test_for_miguel.png"), p, width = 12, height = 5.5, dpi = 300, bg = BG)
ggsave(file.path(OUT, "circularity_test_for_miguel.pdf"), p, width = 12, height = 5.5, bg = BG)
cat("Saved:", file.path(OUT, "circularity_test_for_miguel.png"), "\n")
