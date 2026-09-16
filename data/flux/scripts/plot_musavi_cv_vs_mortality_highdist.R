#!/usr/bin/env Rscript
# ============================================================================
# Same test as plot_musavi_cv_vs_mortality.R (Musavi et al. 2017 design:
# cv(EFP) ~ mortality predictor, weighted lm, weight=1/sqrt(n_years)),
# restricted to the High-disturbance sites only (same NB threshold as
# plot_disturbance_vs_clustering.R / the highdist pairwise-diff scripts:
# max relative_disturbance_500m per site > mean+0.5sd -> 23/93 sites).
# ============================================================================
suppressMessages({library(data.table); library(ggplot2); library(patchwork)})
setwd("/mnt/gsdata/projects/panops/panops-data-registry/data/flux")
OUT <- "plots/V10/mortality_change_vs_efp_change"

EFP_ORDER <- c("GPPsat","NEPmax","ETmax","WUE")
EFP_LAB <- c(GPPsat="cv(GPPsat)", NEPmax="cv(NEPmax)", ETmax="cv(ETmax)", WUE="cv(WUE)")
PRED_LAB <- c(rel_dist_mean="Mean relative disturbance (500m), across years",
              rel_dist_max="Max relative disturbance (500m), across years")

cov <- fread("plots/V10/disturbance_split/disturbance_vs_clustering_by_site.csv",
             select = c("SITE_ID","rel_dist_mean","rel_dist_max"))
nb_dist <- mean(cov$rel_dist_max, na.rm = TRUE) + 0.5 * sd(cov$rel_dist_max, na.rm = TRUE)
high_sites <- cov[rel_dist_max > nb_dist, SITE_ID]
cat(sprintf("NB High threshold (max relative_disturbance_500m across years) = %.2f%%\n", nb_dist))
cat(sprintf("High-disturbance sites: %d / %d\n\n", length(high_sites), nrow(cov)))

sites93 <- unique(fread("derived_tables/outputs_afterEGU_results/v10/v10_B1_GPPsat_harmonized.csv", select = "SITE_ID")$SITE_ID)
main <- fread("derived_tables/outputs_afterEGU_results/EFP_mortality_trait_hydro_combined_with_meteo_dist_lags.csv",
              select = c("SITE_ID","YEAR", EFP_ORDER))
main <- main[SITE_ID %in% sites93 & SITE_ID %in% high_sites]

cv_dt <- rbindlist(lapply(EFP_ORDER, function(r) {
  s <- main[!is.na(get(r)), .(mu = mean(get(r)), sdv = sd(get(r)), n_years = .N), by = SITE_ID]
  s <- s[n_years >= 3]
  s[, cv := sdv / mu]
  s[, response := r]
  s
}))
cat(sprintf("cv computed for %d High-disturbance sites per EFP (>=3 years, full raw record)\n", uniqueN(cv_dt$SITE_ID)))

dt <- merge(cv_dt, cov, by = "SITE_ID")
dt[, response := factor(response, EFP_ORDER)]
fwrite(dt, file.path(OUT, "musavi_cv_data_highdist.csv"))

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
model_dt[, response := factor(response, EFP_ORDER)]
model_dt[, q := p.adjust(P, method = "BH"), by = predictor]
fwrite(model_dt, file.path(OUT, "musavi_cv_model_table_highdist.csv"))
cat("\n=== weighted lm: cv(EFP) ~ mortality predictor, weights=1/sqrt(n_years), High-disturbance subset ===\n")
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
  stat_sub <- model_dt[predictor == pred]
  ggplot(dt, aes(x = get(pred), y = cv)) +
    geom_point(aes(size = n_years), colour = "#B0413E", alpha = 0.7) +
    geom_smooth(aes(weight = 1/sqrt(n_years)), method = "lm", formula = y ~ x,
                colour = "#222222", fill = "#444444", linewidth = 0.7, se = TRUE) +
    geom_text(data = stat_sub, aes(x = -Inf, y = Inf, label = lab), inherit.aes = FALSE,
              hjust = -0.08, vjust = 1.3, size = 2.7, colour = TXT, fontface = "bold") +
    scale_size_continuous(name = "years of data", range = c(1.5, 4.5), breaks = c(3,4,5,6,7)) +
    facet_wrap(~response, scales = "free", nrow = 1, labeller = labeller(response = EFP_LAB)) +
    labs(x = PRED_LAB[pred], y = NULL) + th
}

p <- plot_one("rel_dist_mean") / plot_one("rel_dist_max") +
  plot_annotation(
    title = "Does mortality explain interannual variability (CV) of EFPs? High-disturbance sites only (Musavi et al. 2017 design)",
    subtitle = sprintf("tc>=30, High-disturbance subset only (%d/93 sites, max relative_disturbance_500m > %.1f%% NB threshold), >=3 years raw EFP record | weighted lm, weight = 1/sqrt(n_years)\ntop row: mean relative disturbance across years | bottom row: max relative disturbance across years | point size = years of data",
                        length(high_sites), nb_dist),
    caption = "Same design as musavi_cv_vs_mortality.png (full 93-site sample), restricted to the 23 High-disturbance sites.",
    theme = theme(plot.title = element_text(size = 13, face = "bold"),
                  plot.subtitle = element_text(size = 9, colour = AX),
                  plot.caption = element_text(size = 8, colour = AX, hjust = 0)))

ggsave(file.path(OUT, "musavi_cv_vs_mortality_highdist.png"), p, width = 14, height = 8, dpi = 300, bg = BG)
ggsave(file.path(OUT, "musavi_cv_vs_mortality_highdist.pdf"), p, width = 14, height = 8, bg = BG)
cat("\nSaved:", file.path(OUT, "musavi_cv_vs_mortality_highdist.png"), "\n")
