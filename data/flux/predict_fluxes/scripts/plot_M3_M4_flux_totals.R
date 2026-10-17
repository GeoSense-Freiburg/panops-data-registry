#!/usr/bin/env Rscript
# ============================================================================
# Figure for model_M3_M4_flux_totals.R: circularity-free response (growing-
# season accumulated flux totals) vs the EFP-based results elsewhere in this
# project. Same house style/convention as the permutation test figures
# (plots/V10/disturbance_split/plot_permutation_test_disturbance.R).
# ============================================================================
suppressMessages({library(data.table); library(ggplot2)})
setwd("/mnt/gsdata/projects/panops/panops-data-registry/data/flux")
OUT <- "predict_fluxes/plots"; dir.create(OUT, showWarnings = FALSE, recursive = TRUE)

log_lines <- readLines("predict_fluxes/logs/model_M3_M4_perm99.log")
resp_hdr <- grep("^=== (GPP_sum_gC|RECO_sum_gC|NEP_sum_gC|ET_sum_mm|WUE_annual) :", log_lines)
resp_names <- gsub("^=== ([A-Za-z_]+) .*$", "\\1", log_lines[resp_hdr])
perm_rows <- list()
for (i in seq_along(resp_hdr)) {
  seg_end <- if (i < length(resp_hdr)) resp_hdr[i+1] - 1 else length(log_lines)
  seg <- log_lines[resp_hdr[i]:seg_end]
  m3 <- as.numeric(gsub(".*RMSE=([0-9.]+).*", "\\1", grep("M3 \\(C\\+T\\)", seg, value = TRUE)))
  m4 <- as.numeric(gsub(".*RMSE=([0-9.]+).*", "\\1", grep("M4 \\(C\\+T\\+D\\)", seg, value = TRUE)))
  perm_rows[[resp_names[i]]] <- data.table(response = resp_names[i], rmse_m3 = m3, rmse_m4 = m4)
}

summary_dt <- fread("predict_fluxes/derived_tables/model_M3_M4_flux_totals_perm99.csv")
RESP_ORDER <- c("GPP_sum_gC","NEP_sum_gC","RECO_sum_gC","ET_sum_mm","WUE_annual")
RESP_LAB <- c(GPP_sum_gC="GPP (g C/m2)", NEP_sum_gC="NEP (g C/m2)", RECO_sum_gC="RECO (g C/m2)",
              ET_sum_mm="ET (mm)", WUE_annual="WUE (g C/mm)")
summary_dt[, response := factor(response, RESP_ORDER)]
summary_dt[, sig_lab := sprintf("p=%.2f%s", p_perm, ifelse(p_perm <= 0.01+1e-9, "  (floor at n=99)", ""))]

BG <- "white"; GRID <- "#D9D9D9"; TXT <- "#111111"; AX <- "#444444"
th <- theme_bw(base_size = 11) + theme(
  plot.background = element_rect(fill = BG, colour = NA),
  panel.background = element_rect(fill = BG, colour = NA),
  panel.border = element_rect(colour = GRID, fill = NA, linewidth = 0.4),
  panel.grid.major.x = element_blank(), panel.grid.minor = element_blank(),
  strip.background = element_rect(fill = "#EFEFEF", colour = GRID),
  strip.text = element_text(colour = TXT, size = 10, face = "bold"),
  axis.text = element_text(colour = AX, size = 8.5),
  axis.title = element_text(colour = AX, size = 10, face = "bold"),
  plot.title = element_text(colour = TXT, size = 13, face = "bold"),
  plot.subtitle = element_text(colour = AX, size = 9),
  plot.caption = element_text(colour = AX, size = 8, hjust = 0),
  legend.position = "bottom")

p <- ggplot(summary_dt, aes(x = response)) +
  geom_hline(yintercept = 0, colour = "#888888", linewidth = 0.35, linetype = "dashed") +
  geom_segment(aes(xend = response, y = 0, yend = delta_rmse_pct, colour = p_perm < 0.05), linewidth = 6) +
  geom_text(aes(y = delta_rmse_pct, label = sig_lab),
            vjust = ifelse(summary_dt$delta_rmse_pct < 0, 1.4, -0.8), size = 3, fontface = "bold", colour = TXT) +
  scale_colour_manual(values = c("TRUE" = "#2E6F40", "FALSE" = "#B0413E"),
                       labels = c("TRUE" = "p < 0.05", "FALSE" = "n.s."), name = "permutation test") +
  scale_x_discrete(labels = RESP_LAB) +
  labs(title = "M3 -> M4: does disturbance improve growing-season accumulated flux totals?",
       subtitle = "Circularity-free response (see daily_reports/2026-09-17-efp-calculation-and-circularity.md) | tc>=30, 93 sites, n=378 site-years\nXGBoost (GPPsat's tuned hyperparameters, reused as a first pass), LOSO CV, climate lag1+lag2 + traits + disturbance",
       x = NULL, y = "Delta RMSE (%), M3 -> M4 (negative = disturbance improves prediction)",
       caption = "p from 99-rep block permutation of the disturbance block. GPP/NEP/WUE match the EFP-based result's significance pattern (GPPsat/NEPmax/WUE real, ETmax null) with smaller magnitudes;\nRECO joins ET as a null. NEP's M3 R2 is negative (-0.075) - a small residual of two much larger fluxes, hard to predict at all.") +
  th

ggsave(file.path(OUT, "M3_M4_flux_totals_summary.png"), p, width = 13, height = 6.5, dpi = 300, bg = BG)
ggsave(file.path(OUT, "M3_M4_flux_totals_summary.pdf"), p, width = 13, height = 6.5, bg = BG)
cat("Saved:", file.path(OUT, "M3_M4_flux_totals_summary.png"), "\n")
