#!/usr/bin/env Rscript
# ============================================================================
# Replicates the central test of Musavi et al. 2017 (Nat. Ecol. Evol.,
# "Stand age and species richness dampen interannual variation of
# ecosystem-level photosynthetic capacity"), substituting MORTALITY for
# their predictors (stand age, species richness).
#
# Their design (Table 1/2, Fig 1): compute cvGPPsat = SD(GPPsat across
# years) / mean(GPPsat across years) per site (the interannual variability,
# IAV, of the EFP), then fit a linear model cvGPPsat ~ predictor across
# sites, weighting each site by 1/sqrt(n_years) (their Eq. in Methods, to
# down-weight sites with fewer years of data), reporting R^2, Adj R^2,
# coefficient +/- SE, P.
#
# Here: cv is computed for all 4 EFPs (not just GPPsat), using the FULL raw
# annual record per site (from the combined EFP table, not the model-ready
# B1 files, which drop rows for climate-predictor completeness reasons
# unrelated to whether the EFP itself was measured - exactly mirroring
# Musavi's own use of the full available flux record). The predictor is
# relative_disturbance_500m, summarised per site two ways (mirroring their
# two separate single-predictor models before any combination): rel_dist_mean
# and rel_dist_max (site's mean / max across its years) - both already
# computed in disturbance_vs_clustering_by_site.csv, not recomputed here.
# rel_dist_mean and rel_dist_max are correlated at r=0.94 across sites, so
# (as in their own stepwise approach) each is modelled separately rather
# than combined in one collinear model.
#
# Scope: tc>=30, 93 sites (all have >=4 years raw EFP data, matching
# Musavi's own >=4-year data requirement).
# ============================================================================
suppressMessages({library(data.table); library(ggplot2)})
setwd("/mnt/gsdata/projects/panops/panops-data-registry/data/flux")
OUT <- "plots/V10/mortality_change_vs_efp_change"

EFP_ORDER <- c("GPPsat","NEPmax","ETmax","WUE")
EFP_LAB <- c(GPPsat="cv(GPPsat)", NEPmax="cv(NEPmax)", ETmax="cv(ETmax)", WUE="cv(WUE)")
PRED_LAB <- c(rel_dist_mean="Mean relative disturbance (500m), across years",
              rel_dist_max="Max relative disturbance (500m), across years")

sites93 <- unique(fread("derived_tables/outputs_afterEGU_results/v10/v10_B1_GPPsat_harmonized.csv", select = "SITE_ID")$SITE_ID)
main <- fread("derived_tables/outputs_afterEGU_results/EFP_mortality_trait_hydro_combined_with_meteo_dist_lags.csv",
              select = c("SITE_ID","YEAR", EFP_ORDER))
main <- main[SITE_ID %in% sites93]

cv_dt <- rbindlist(lapply(EFP_ORDER, function(r) {
  s <- main[!is.na(get(r)), .(mu = mean(get(r)), sdv = sd(get(r)), n_years = .N), by = SITE_ID]
  s <- s[n_years >= 3]  # matches this project's own site-selection floor (>=3 continuous years)
  s[, cv := sdv / mu]
  s[, response := r]
  s
}))
cat(sprintf("cv computed for %d sites per EFP (>=3 years, full raw record)\n", uniqueN(cv_dt$SITE_ID)))

cov <- fread("plots/V10/disturbance_split/disturbance_vs_clustering_by_site.csv",
             select = c("SITE_ID","rel_dist_mean","rel_dist_max"))
dt <- merge(cv_dt, cov, by = "SITE_ID")
dt[, response := factor(response, EFP_ORDER)]
fwrite(dt, file.path(OUT, "musavi_cv_data.csv"))

# --- weighted linear models, mirroring Table 1/2 exactly (weight = 1/sqrt(n_years)) --
model_results <- list()
for (r in EFP_ORDER) for (pred in c("rel_dist_mean","rel_dist_max")) {
  sub <- dt[response == r]
  m <- lm(as.formula(sprintf("cv ~ %s", pred)), data = sub, weights = 1/sqrt(sub$n_years))
  s <- summary(m)
  cf <- coef(s)[pred, ]
  model_results[[paste(r, pred)]] <- data.table(
    response = r, predictor = pred, n = nrow(sub),
    R2 = s$r.squared, Adj_R2 = s$adj.r.squared,
    coefficient = cf["Estimate"], se = cf["Std. Error"], P = cf["Pr(>|t|)"])
}
model_dt <- rbindlist(model_results)
model_dt[, response := factor(response, EFP_ORDER)]  # match dt's factor exactly, or facet_wrap falls back to alphabetical across layers
model_dt[, q := p.adjust(P, method = "BH"), by = predictor]
fwrite(model_dt, file.path(OUT, "musavi_cv_model_table.csv"))
cat("\n=== weighted lm: cv(EFP) ~ mortality predictor, weights=1/sqrt(n_years) (mirrors Musavi et al. 2017 Table 1/2) ===\n")
print(model_dt[, .(response, predictor, n, R2 = round(R2,3), coefficient = signif(coefficient,3),
                    se = signif(se,3), P = signif(P,3), q = signif(q,3))])

stars <- function(q) fifelse(is.na(q), "", fifelse(q<0.001,"***", fifelse(q<0.01,"**", fifelse(q<0.05,"*"," ns"))))
model_dt[, lab := sprintf("R2=%.3f%s\ncoef=%.4f, n=%d", R2, stars(q), coefficient, n)]

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
  legend.position = "bottom")

plot_one <- function(pred) {
  sub <- dt
  stat_sub <- model_dt[predictor == pred]
  ggplot(sub, aes(x = get(pred), y = cv)) +
    geom_point(aes(size = n_years), colour = "#4C7C3A", alpha = 0.65) +
    geom_smooth(aes(weight = 1/sqrt(n_years)), method = "lm", formula = y ~ x,
                colour = "#222222", fill = "#444444", linewidth = 0.7, se = TRUE) +
    geom_text(data = stat_sub, aes(x = -Inf, y = Inf, label = lab), inherit.aes = FALSE,
              hjust = -0.08, vjust = 1.3, size = 2.7, colour = TXT, fontface = "bold") +
    scale_size_continuous(name = "years of data", range = c(1, 4), breaks = c(3,4,5,6,7)) +
    facet_wrap(~response, scales = "free", nrow = 1, labeller = labeller(response = EFP_LAB)) +
    labs(x = PRED_LAB[pred], y = NULL) + th
}

library(patchwork)
p <- plot_one("rel_dist_mean") / plot_one("rel_dist_max") +
  plot_annotation(
    title = "Does mortality explain interannual variability (CV) of EFPs? (Musavi et al. 2017 design, predictor swapped for mortality)",
    subtitle = "tc>=30, 93 sites (>=3 years raw EFP record) | cv(EFP) = SD(EFP across years) / mean(EFP across years) | weighted lm, weight = 1/sqrt(n_years) (Musavi et al. 2017 Methods)\ntop row: mean relative disturbance across years | bottom row: max relative disturbance across years | point size = years of data (= the regression weight)",
    caption = "Musavi et al. 2017, Nat. Ecol. Evol. 1:0048 tested cvGPPsat ~ ln(stand age) + species richness (R2=0.55) with this exact weighted-lm design; predictor substituted here for site-level mortality exposure.",
    theme = theme(plot.title = element_text(size = 13, face = "bold"),
                  plot.subtitle = element_text(size = 9, colour = AX),
                  plot.caption = element_text(size = 8, colour = AX, hjust = 0)))

ggsave(file.path(OUT, "musavi_cv_vs_mortality.png"), p, width = 14, height = 8, dpi = 300, bg = BG)
ggsave(file.path(OUT, "musavi_cv_vs_mortality.pdf"), p, width = 14, height = 8, bg = BG)
cat("\nSaved:", file.path(OUT, "musavi_cv_vs_mortality.png"), "\n")
