#!/usr/bin/env Rscript
# ============================================================================
# 12m vs 24m window comparison for the flux-totals M3/M4 results
# (model_M3_M4_flux_totals.R). Same house style as
# plot_M3_M4_flux_totals.R, which this complements (not replaces) - that
# plot only shows the 24m run.
# ============================================================================
suppressMessages({library(data.table); library(ggplot2)})
setwd("/mnt/gsdata/projects/panops/panops-data-registry/data/flux")
OUT <- "predict_fluxes/plots"; dir.create(OUT, showWarnings = FALSE, recursive = TRUE)

d24 <- fread("predict_fluxes/derived_tables/model_M3_M4_flux_totals_perm99.csv")
d24[, window := "24m"]
d12 <- fread("predict_fluxes/derived_tables/model_M3_M4_flux_totals_12m_perm99.csv")
dt <- rbindlist(list(d12, d24), fill = TRUE)

RESP_ORDER <- c("GPP_sum_gC","NEP_sum_gC","RECO_sum_gC","ET_sum_mm","WUE_annual")
RESP_LAB <- c(GPP_sum_gC="GPP (g C/m2)", NEP_sum_gC="NEP (g C/m2)", RECO_sum_gC="RECO (g C/m2)",
              ET_sum_mm="ET (mm)", WUE_annual="WUE (g C/mm)")
dt[, response := factor(response, RESP_ORDER)]
dt[, window := factor(window, c("12m","24m"))]
dt[, sig_lab := sprintf("p=%.2f", p_perm)]

BG <- "white"; GRID <- "#D9D9D9"; TXT <- "#111111"; AX <- "#444444"
WIN_COL <- c("12m" = "#2E6F40", "24m" = "#5AA469")
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

p <- ggplot(dt, aes(x = window, y = delta_rmse_pct, fill = window)) +
  geom_hline(yintercept = 0, colour = "#888888", linewidth = 0.35, linetype = "dashed") +
  geom_col(width = 0.55) +
  geom_text(aes(label = sig_lab, vjust = ifelse(delta_rmse_pct < 0, 1.3, -0.6)),
            size = 3, fontface = "bold", colour = TXT) +
  scale_fill_manual(values = WIN_COL, name = "climate + disturbance window") +
  facet_wrap(~response, nrow = 1, scales = "free_y", labeller = labeller(response = RESP_LAB)) +
  labs(title = "M3 -> M4 on flux totals: 12m vs 24m climate+disturbance window",
       subtitle = "tc>=30, 93 sites, n=378 site-years | 12m: climate=lag1, disturbance=contemporaneous+lag1 (100 cols) | 24m: +lag2 for both (110 disturbance cols)\nXGBoost (GPPsat's tuned hyperparameters), LOSO CV | p from 99-rep block permutation of the disturbance block",
       x = NULL, y = "Delta RMSE (%), M3 -> M4 (negative = disturbance improves prediction)",
       caption = "12m shows consistently larger (or equal-direction) effects than 24m for every response. RECO moves from clearly null (24m, p=0.13) to borderline (12m, p=0.06) but neither clears p<0.05.\nSame significance pattern otherwise: GPP/NEP/WUE real at both windows, ET null (and slightly positive, i.e. disturbance makes it marginally worse) at both.") +
  th

ggsave(file.path(OUT, "M3_M4_flux_totals_window_compare.png"), p, width = 13, height = 6.5, dpi = 300, bg = BG)
ggsave(file.path(OUT, "M3_M4_flux_totals_window_compare.pdf"), p, width = 13, height = 6.5, bg = BG)
cat("Saved:", file.path(OUT, "M3_M4_flux_totals_window_compare.png"), "\n")
