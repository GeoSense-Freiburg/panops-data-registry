#!/usr/bin/env Rscript
# ============================================================================
# Same test as plot_mortality_change_vs_efp_change.R, restricted to sites
# classified "High disturbance" - directly testing Rene's meeting point #5:
# the relationship may be diluted by sites with little/no disturbance, where
# mortality has little influence; it might show up once restricted to the
# smaller set of strongly disturbed sites.
#
# "High disturbance" reuses the EXACT natural-break definition already
# established in plot_disturbance_vs_clustering.R (NOT reinvented here):
# site-level rel_dist_max = max(relative_disturbance_500m) across a site's
# years, threshold = mean(rel_dist_max) + 0.5*sd(rel_dist_max) across the 93
# tc>=30 sites. That gives 23/93 sites.
# ============================================================================
suppressMessages({library(data.table); library(ggplot2)})
setwd("/mnt/gsdata/projects/panops/panops-data-registry/data/flux")
B <- "derived_tables/outputs_afterEGU_results"
OUT <- "plots/V10/mortality_change_vs_efp_change"

EFP_ORDER <- c("GPPsat","NEPmax","ETmax","WUE")
EFP_LAB <- c(GPPsat="Delta GPPsat  (µmol m⁻² s⁻¹)", NEPmax="Delta NEPmax  (µmol m⁻² s⁻¹)",
             ETmax="Delta ETmax  (mm d⁻¹)", WUE="Delta WUE  (g C mm⁻¹)")
DIST_VAR <- "relative_disturbance_500m"

cov <- fread(file.path(OUT, "..", "disturbance_split", "disturbance_vs_clustering_by_site.csv"))
nb_dist <- mean(cov$rel_dist_max, na.rm = TRUE) + 0.5 * sd(cov$rel_dist_max, na.rm = TRUE)
high_sites <- cov[rel_dist_max > nb_dist, SITE_ID]
cat(sprintf("NB High threshold (max relative_disturbance_500m across years) = %.2f%%\n", nb_dist))
cat(sprintf("High-disturbance sites: %d / %d\n\n", length(high_sites), nrow(cov)))

diff_rows <- list()
for (resp in EFP_ORDER) {
  df <- fread(sprintf("%s/v10/v10_B1_%s_harmonized.csv", B, resp))
  dt <- df[SITE_ID %in% high_sites, .(SITE_ID, YEAR, val = get(resp), dist = get(DIST_VAR))]
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
fwrite(diff_dt, file.path(OUT, "yearly_diff_pairs_highdist.csv"))

stat <- diff_dt[, {
  ct <- cor.test(d_dist, d_val, method = "spearman", exact = FALSE)
  list(rho = unname(ct$estimate), p = ct$p.value, n_pairs = .N, n_sites = uniqueN(SITE_ID))
}, by = response]
stat[, q := p.adjust(p, method = "BH")]
stars <- function(q) fifelse(is.na(q), "", fifelse(q<0.001,"***", fifelse(q<0.01,"**", fifelse(q<0.05,"*"," ns"))))
stat[, lab := sprintf("rho=%.2f%s\nn=%d pairs, %d sites", rho, stars(q), n_pairs, n_sites)]
fwrite(stat[order(response)], file.path(OUT, "yearly_diff_stats_highdist.csv"))
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
  geom_point(size = 1.5, alpha = 0.7, colour = "#B0413E") +
  geom_smooth(method = "lm", formula = y ~ x, colour = "#222222", fill = "#444444",
              linewidth = 0.7, se = TRUE) +
  geom_text(data = stat, aes(x = -Inf, y = Inf, label = lab), inherit.aes = FALSE,
            hjust = -0.08, vjust = 1.3, size = 2.8, colour = TXT, fontface = "bold") +
  facet_wrap(~response, scales = "free", nrow = 1, labeller = labeller(response = EFP_LAB)) +
  labs(title = "Year-to-year disturbance change vs EFP change - High-disturbance sites only",
       subtitle = sprintf("tc>=30, High-disturbance subset only (%d/93 sites, max relative_disturbance_500m > %.1f%% NB threshold) | consecutive-year pairs only\nlinear fit with 95%% CI | Spearman rho, BH-FDR across the 4 EFPs",
                           length(high_sites), nb_dist),
       x = "Delta relative disturbance (500m buffer), year t vs year t-1",
       y = NULL,
       caption = "Same design as mortality_change_vs_efp_change.png, restricted to the 23 sites classified High disturbance (same NB threshold as plot_disturbance_vs_clustering.R).\nTests Rene's meeting point #5: does the signal appear once diluting low/no-disturbance sites are excluded?") +
  th

ggsave(file.path(OUT, "mortality_change_vs_efp_change_highdist.png"), p, width = 13, height = 4.5, dpi = 300, bg = BG)
ggsave(file.path(OUT, "mortality_change_vs_efp_change_highdist.pdf"), p, width = 13, height = 4.5, bg = BG)
cat("\nSaved:", file.path(OUT, "mortality_change_vs_efp_change_highdist.png"), "\n")
