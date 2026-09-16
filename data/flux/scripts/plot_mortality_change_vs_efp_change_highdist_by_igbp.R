#!/usr/bin/env Rscript
# ============================================================================
# Same extension as plot_mortality_change_vs_efp_change_by_igbp.R, applied to
# the High-disturbance-sites-only sample (see
# plot_mortality_change_vs_efp_change_highdist.R): IGBP-coloured points with
# a per-IGBP lm fit line (n_pairs>=8 & n_sites>=3 only), plus the pooled OLS
# regression (d_val ~ d_dist) reported per EFP.
# Reuses the already-computed, already-committed diff pairs
# (yearly_diff_pairs_highdist.csv) rather than recomputing them.
# ============================================================================
suppressMessages({library(data.table); library(ggplot2)})
setwd("/mnt/gsdata/projects/panops/panops-data-registry/data/flux")
OUT <- "plots/V10/mortality_change_vs_efp_change"

EFP_ORDER <- c("GPPsat","NEPmax","ETmax","WUE")
EFP_LAB <- c(GPPsat="Delta GPPsat  (µmol m⁻² s⁻¹)", NEPmax="Delta NEPmax  (µmol m⁻² s⁻¹)",
             ETmax="Delta ETmax  (mm d⁻¹)", WUE="Delta WUE  (g C mm⁻¹)")
IGBP_ORDER <- c("ENF","EBF","DNF","DBF","MF","CSH","OSH","WSA","SAV","WET")
IGBP_COL <- c(ENF="#1F6B3A", EBF="#33A14A", DNF="#7BC87E", DBF="#B2DF8A",
              MF="#FDBF6F", CSH="#E5820B", OSH="#D4A017", WSA="#C4A85C",
              SAV="#E8D44D", WET="#4DAECC")
MIN_PAIRS <- 8; MIN_SITES <- 3

igbp_meta <- unique(fread("derived_tables/outputs_afterEGU_results/EFP_mortality_trait_hydro_combined_with_meteo_dist_lags.csv",
                           select = c("SITE_ID","IGBP")))

diff_dt <- fread(file.path(OUT, "yearly_diff_pairs_highdist.csv"))
diff_dt <- merge(diff_dt, igbp_meta, by = "SITE_ID")
diff_dt[, response := factor(response, EFP_ORDER)]
diff_dt[, IGBP := factor(IGBP, IGBP_ORDER)]

igbp_n <- diff_dt[response == "GPPsat", .(n_pairs = .N, n_sites = uniqueN(SITE_ID)), by = IGBP]
fit_igbp <- igbp_n[n_pairs >= MIN_PAIRS & n_sites >= MIN_SITES, as.character(IGBP)]
cat("IGBP groups fit a line (n_pairs>=8 & n_sites>=3):", paste(fit_igbp, collapse=", "), "\n")
cat("IGBP groups points-only (too few pairs/sites): ",
    paste(setdiff(as.character(igbp_n$IGBP), fit_igbp), collapse=", "), "\n\n")
diff_fit <- diff_dt[IGBP %in% fit_igbp]

stat <- diff_dt[, {
  ct <- cor.test(d_dist, d_val, method = "spearman", exact = FALSE)
  list(rho = unname(ct$estimate), p_rho = ct$p.value, n_pairs = .N, n_sites = uniqueN(SITE_ID))
}, by = response]
stat[, q_rho := p.adjust(p_rho, method = "BH")]

ols <- diff_dt[, {
  m <- lm(d_val ~ d_dist)
  s <- summary(m)
  cf <- coef(s)["d_dist", ]
  list(slope = cf["Estimate"], se = cf["Std. Error"], t = cf["t value"], p_slope = cf["Pr(>|t|)"],
       r2 = s$r.squared, intercept = coef(m)[["(Intercept)"]])
}, by = response]
ols[, q_slope := p.adjust(p_slope, method = "BH")]

stat <- merge(stat, ols, by = "response")
stars <- function(q) fifelse(is.na(q), "", fifelse(q<0.001,"***", fifelse(q<0.01,"**", fifelse(q<0.05,"*"," ns"))))
stat[, lab := sprintf("rho=%.2f%s | slope=%.3f%s (R2=%.3f)\nn=%d pairs, %d sites",
                       rho, stars(q_rho), slope, stars(q_slope), r2, n_pairs, n_sites)]
fwrite(stat[order(response)], file.path(OUT, "yearly_diff_stats_highdist_by_igbp.csv"))
cat("=== pooled Spearman + OLS regression (d_val ~ d_dist), per EFP, High-disturbance subset ===\n")
print(stat[order(response), .(response, rho = round(rho,3), q_rho = signif(q_rho,3),
                                slope = round(slope,4), q_slope = signif(q_slope,3), r2 = round(r2,4))])

BG <- "white"; GRID <- "#D9D9D9"; TXT <- "#111111"; AX <- "#444444"
th <- theme_bw(base_size = 11) + theme(
  plot.background = element_rect(fill = BG, colour = NA),
  panel.background = element_rect(fill = BG, colour = NA),
  panel.border = element_rect(colour = GRID, fill = NA, linewidth = 0.4),
  panel.grid.major = element_line(colour = GRID, linewidth = 0.2),
  panel.grid.minor = element_blank(),
  strip.background = element_rect(fill = "#EFEFEF", colour = GRID),
  strip.text = element_text(colour = TXT, size = 10, face = "bold"),
  axis.text = element_text(colour = AX, size = 9),
  axis.title = element_text(colour = AX, size = 10, face = "bold"),
  plot.title = element_text(colour = TXT, size = 14, face = "bold"),
  plot.subtitle = element_text(colour = AX, size = 9),
  plot.caption = element_text(colour = AX, size = 8, hjust = 0),
  legend.position = "right")

p <- ggplot(diff_dt, aes(d_dist, d_val)) +
  geom_hline(yintercept = 0, colour = "#888888", linewidth = 0.3, linetype = "dashed") +
  geom_vline(xintercept = 0, colour = "#888888", linewidth = 0.3, linetype = "dashed") +
  geom_point(aes(colour = IGBP), size = 1.6, alpha = 0.7) +
  geom_smooth(aes(group = IGBP, colour = IGBP), data = diff_fit, method = "lm", formula = y ~ x,
              se = FALSE, linewidth = 0.8) +
  geom_smooth(method = "lm", formula = y ~ x, colour = "black", linetype = "22",
              linewidth = 0.9, se = FALSE) +
  geom_text(data = stat, aes(x = -Inf, y = Inf, label = lab), inherit.aes = FALSE,
            hjust = -0.03, vjust = 1.3, size = 2.5, colour = TXT, fontface = "bold") +
  scale_colour_manual(values = IGBP_COL, breaks = IGBP_ORDER, name = "IGBP",
                       labels = function(x) ifelse(x %in% fit_igbp, x, paste0(x, " (points only)"))) +
  facet_wrap(~response, scales = "free", nrow = 1, labeller = labeller(response = EFP_LAB)) +
  labs(title = "Year-to-year disturbance change vs EFP change, by IGBP - High-disturbance sites only",
       subtitle = "tc>=30, High-disturbance subset (23/93 sites, same NB threshold as plot_disturbance_vs_clustering.R) | coloured lines = per-IGBP lm fit (>=8 pairs & >=3 sites only) | dashed black = pooled lm fit\nannotation: pooled Spearman rho and pooled OLS slope (Delta-EFP ~ Delta-disturbance), BH-FDR across the 4 EFPs",
       x = "Delta relative disturbance (500m buffer), year t vs year t-1", y = NULL,
       caption = "Same underlying pairs as mortality_change_vs_efp_change_highdist.png; IGBP colouring/per-group fits and the OLS slope/R2 are additions, not a replacement.") +
  th

ggsave(file.path(OUT, "mortality_change_vs_efp_change_highdist_by_igbp.png"), p, width = 15, height = 5, dpi = 300, bg = BG)
ggsave(file.path(OUT, "mortality_change_vs_efp_change_highdist_by_igbp.pdf"), p, width = 15, height = 5, bg = BG)
cat("\nSaved:", file.path(OUT, "mortality_change_vs_efp_change_highdist_by_igbp.png"), "\n")
