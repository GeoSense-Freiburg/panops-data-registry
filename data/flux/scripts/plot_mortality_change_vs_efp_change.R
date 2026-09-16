#!/usr/bin/env Rscript
# ============================================================================
# Simple complementary test suggested by Miguel at the 2026-09-14 FORTRACK
# meeting: is there a simple relationship between YEAR-OVER-YEAR CHANGES in
# disturbance and YEAR-OVER-YEAR CHANGES in EFPs, independent of the full
# ML modelling approach?
#
# Design: for each site, sort its available years and take first differences
# between CONSECUTIVE available years (YEAR_t - YEAR_{t-1} == 1 only - a
# pair spanning a gap, e.g. 2019 -> 2021, is dropped so every diff reflects
# one calendar year of change, not an ambiguous multi-year one). This
# first-differencing cancels out anything time-invariant per site (traits,
# soil, climate normals, location) - a much simpler, non-ML complement to
# the main results, testing the raw pairwise relationship Miguel described.
#
# Scope (agreed with the user): tc>=30 (93 sites / 395 site-years, the same
# harmonized B1 files used everywhere else this session), relative_disturbance
# (500m) as the disturbance/mortality-change metric, raw (non-anomaly) EFP
# values for GPPsat/NEPmax/ETmax/WUE.
#
# Caveat (Teja, same meeting): a same-year climate swing could drive both
# Delta-disturbance and Delta-EFP at once: this raw test does not control for
# that. It is deliberately the SIMPLE version Miguel asked for, not a
# replacement for the main SHAP-based results.
# ============================================================================
suppressMessages({library(data.table); library(ggplot2)})
setwd("/mnt/gsdata/projects/panops/panops-data-registry/data/flux")
B <- "derived_tables/outputs_afterEGU_results"
OUT <- "plots/V10/mortality_change_vs_efp_change"; dir.create(OUT, showWarnings = FALSE, recursive = TRUE)

EFP_ORDER <- c("GPPsat","NEPmax","ETmax","WUE")
EFP_LAB <- c(GPPsat="Delta GPPsat  (µmol m⁻² s⁻¹)", NEPmax="Delta NEPmax  (µmol m⁻² s⁻¹)",
             ETmax="Delta ETmax  (mm d⁻¹)", WUE="Delta WUE  (g C mm⁻¹)")
DIST_VAR <- "relative_disturbance_500m"

diff_rows <- list()
for (resp in EFP_ORDER) {
  df <- fread(sprintf("%s/v10/v10_B1_%s_harmonized.csv", B, resp))
  dt <- df[, .(SITE_ID, YEAR, val = get(resp), dist = get(DIST_VAR))]
  dt <- dt[complete.cases(dt)]
  setorder(dt, SITE_ID, YEAR)

  dd <- dt[, {
    n <- .N
    if (n < 2) list() else {
      gap <- diff(YEAR)
      keep <- gap == 1L
      list(YEAR_to = YEAR[-1][keep], YEAR_from = YEAR[-n][keep],
           d_val = diff(val)[keep], d_dist = diff(dist)[keep])
    }
  }, by = SITE_ID]

  dd[, response := resp]
  diff_rows[[resp]] <- dd
  cat(sprintf("%-7s: %3d site-years -> %3d sites -> %3d consecutive-year pairs (from %d sites)\n",
              resp, nrow(dt), uniqueN(dt$SITE_ID), nrow(dd), uniqueN(dd$SITE_ID)))
}
diff_dt <- rbindlist(diff_rows)
diff_dt[, response := factor(response, EFP_ORDER)]
fwrite(diff_dt, file.path(OUT, "yearly_diff_pairs.csv"))

stat <- diff_dt[, {
  ct <- cor.test(d_dist, d_val, method = "spearman", exact = FALSE)
  list(rho = unname(ct$estimate), p = ct$p.value, n_pairs = .N, n_sites = uniqueN(SITE_ID))
}, by = response]
stat[, q := p.adjust(p, method = "BH")]
stars <- function(q) fifelse(is.na(q), "", fifelse(q<0.001,"***", fifelse(q<0.01,"**", fifelse(q<0.05,"*"," ns"))))
stat[, lab := sprintf("rho=%.2f%s\nn=%d pairs, %d sites", rho, stars(q), n_pairs, n_sites)]
fwrite(stat[order(response)], file.path(OUT, "yearly_diff_stats.csv"))
cat("\n")
print(stat[order(response), .(response, rho = round(rho,3), q = signif(q,3), n_pairs, n_sites)])

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
  plot.caption = element_text(colour = AX, size = 8, hjust = 0))

p <- ggplot(diff_dt, aes(d_dist, d_val)) +
  geom_hline(yintercept = 0, colour = "#888888", linewidth = 0.35, linetype = "dashed") +
  geom_vline(xintercept = 0, colour = "#888888", linewidth = 0.35, linetype = "dashed") +
  geom_point(size = 1.3, alpha = 0.6, colour = "#4C7C3A") +
  geom_smooth(method = "lm", formula = y ~ x, colour = "#222222", fill = "#444444",
              linewidth = 0.7, se = TRUE) +
  geom_text(data = stat, aes(x = -Inf, y = Inf, label = lab), inherit.aes = FALSE,
            hjust = -0.08, vjust = 1.3, size = 2.8, colour = TXT, fontface = "bold") +
  facet_wrap(~response, scales = "free", nrow = 1, labeller = labeller(response = EFP_LAB)) +
  labs(title = "Do year-to-year changes in disturbance track year-to-year changes in EFPs?",
       subtitle = "tc>=30 (93 sites), consecutive-year pairs only (YEAR_t - YEAR_(t-1) = 1) | Delta = value(t) - value(t-1), pooled across sites\nlinear fit with 95% CI | Spearman rho, BH-FDR across the 4 EFPs",
       x = "Delta relative disturbance (500m buffer), year t vs year t-1",
       y = NULL,
       caption = "Simple pairwise complement to the SHAP-based results (Miguel, FORTRACK 2026-09-14), not a replacement.\nCaveat (Teja, same meeting): a same-year climate swing could drive both axes at once - this raw test does not control for that.") +
  th

ggsave(file.path(OUT, "mortality_change_vs_efp_change.png"), p, width = 13, height = 4.5, dpi = 300, bg = BG)
ggsave(file.path(OUT, "mortality_change_vs_efp_change.pdf"), p, width = 13, height = 4.5, bg = BG)
cat("\nSaved:", file.path(OUT, "mortality_change_vs_efp_change.png"), "\n")
