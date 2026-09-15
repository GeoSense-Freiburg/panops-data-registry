#!/usr/bin/env Rscript
# ============================================================================
# Figure for the block-permutation test (scripts/permutation_test_disturbance.R),
# addressing Rene's meeting question: is the M3->M4 RMSE improvement real
# disturbance information, or just an artifact of M4 having ~100 more columns?
#
# For each EFP: the null distribution is built by block-permuting the D
# block's rows (breaking its link to y/climate/traits, keeping its own
# internal structure) and retraining M4 under identical LOSO folds/
# hyperparameters, N_PERM=10 times. Real M4 is plotted against that null,
# with the M3 (no disturbance) baseline as a reference line.
# ============================================================================
suppressMessages({library(data.table); library(ggplot2)})
setwd("/mnt/gsdata/projects/panops/panops-data-registry/data/flux")
OUT <- "plots/V10/disturbance_split"

# per-EFP null RMSEs are only in the run log, not the summary CSV - reparse
log_lines <- readLines("logs/permutation_test_disturbance.log")
resp_hdr <- grep("^=== (GPPsat|NEPmax|ETmax|WUE) ===$", log_lines)
resp_names <- gsub("=== | ===", "", log_lines[resp_hdr])
perm_rows <- list()
for (i in seq_along(resp_hdr)) {
  seg_end <- if (i < length(resp_hdr)) resp_hdr[i+1] - 1 else length(log_lines)
  seg <- log_lines[resp_hdr[i]:seg_end]
  vals <- as.numeric(gsub(".*RMSE = ", "", grep("perm ", seg, value = TRUE)))
  perm_rows[[resp_names[i]]] <- data.table(response = resp_names[i], rmse_perm = vals)
}
perm_dt <- rbindlist(perm_rows)

summary_dt <- fread(file.path(OUT, "permutation_test_disturbance_12m.csv"))
EFP_ORDER <- c("GPPsat","NEPmax","ETmax","WUE")
EFP_LAB <- c(GPPsat="GPPsat", NEPmax="NEPmax", ETmax="ETmax", WUE="WUE")
perm_dt[, response := factor(response, EFP_ORDER)]
summary_dt[, response := factor(response, EFP_ORDER)]
summary_dt[, sig_lab := sprintf("p=%.3f%s", p_perm, ifelse(p_perm <= 1/(n_perm+1)+1e-9, "  (best possible at n=10)", ""))]

BG <- "white"; GRID <- "#D9D9D9"; TXT <- "#111111"; AX <- "#444444"
th <- theme_bw(base_size = 11) + theme(
  plot.background = element_rect(fill = BG, colour = NA),
  panel.background = element_rect(fill = BG, colour = NA),
  panel.border = element_rect(colour = GRID, fill = NA, linewidth = 0.4),
  panel.grid.major.x = element_blank(),
  panel.grid.minor = element_blank(),
  strip.background = element_rect(fill = "#EFEFEF", colour = GRID),
  strip.text = element_text(colour = TXT, size = 10, face = "bold"),
  axis.text = element_text(colour = AX, size = 9),
  axis.title = element_text(colour = AX, size = 10, face = "bold"),
  plot.title = element_text(colour = TXT, size = 14, face = "bold"),
  plot.subtitle = element_text(colour = AX, size = 9),
  plot.caption = element_text(colour = AX, size = 8, hjust = 0),
  legend.position = "bottom")

p <- ggplot() +
  geom_jitter(data = perm_dt, aes(x = "Permuted D\n(null, n=10)", y = rmse_perm, colour = "Permuted D (null)"),
              width = 0.08, size = 1.8, alpha = 0.7) +
  geom_boxplot(data = perm_dt, aes(x = "Permuted D\n(null, n=10)", y = rmse_perm),
               width = 0.35, outlier.shape = NA, fill = NA, colour = "#555555", linewidth = 0.4) +
  geom_point(data = summary_dt, aes(x = "M3\n(no disturbance)", y = rmse_M3, colour = "M3 baseline"),
             size = 3, shape = 18) +
  geom_point(data = summary_dt, aes(x = "M4 real D\n(actual data)", y = rmse_M4_real, colour = "M4 real disturbance"),
             size = 3.2, shape = 18) +
  geom_text(data = summary_dt, aes(x = "M4 real D\n(actual data)", y = rmse_M4_real, label = sig_lab),
            vjust = -1.3, size = 2.9, colour = TXT, fontface = "bold") +
  scale_colour_manual(values = c("Permuted D (null)" = "#8C8C8C", "M3 baseline" = "#B0413E",
                                  "M4 real disturbance" = "#2E6F40"), name = NULL) +
  facet_wrap(~response, scales = "free_y", nrow = 1, labeller = labeller(response = EFP_LAB)) +
  labs(title = "Permutation test: is the disturbance-block RMSE gain real information, or added flexibility?",
       subtitle = "XGBoost-Optuna, tc>=30 (93 sites / 395 site-years), 12-month window, LOSO CV | Null = M4 retrained with the disturbance block's rows block-permuted (10 reps)",
       x = NULL, y = "LOSO RMSE",
       caption = "p = (# permuted reps with RMSE <= real M4 RMSE + 1) / (n_perm + 1), one-sided. p=0.091 = real M4 beat all 10/10 permutations (the floor at n_perm=10).\nETmax's real M4 falling inside the null cloud is expected: disturbance did not improve ETmax in the main results either - an internal negative control.") +
  th

ggsave(file.path(OUT, "permutation_test_disturbance_12m.png"), p, width = 13, height = 5.5, dpi = 300, bg = BG)
ggsave(file.path(OUT, "permutation_test_disturbance_12m.pdf"), p, width = 13, height = 5.5, bg = BG)
cat("Saved:", file.path(OUT, "permutation_test_disturbance_12m.png"), "\n")
