#!/usr/bin/env Rscript
# ============================================================================
# Source data for the main display items (Nature Ecology & Evolution asks for one Excel file per figure).
# Reads only files inside the package, writes source_data/<item>.xlsx. Run from anywhere:
#   Rscript paper_code_data/make_source_data.R
# Main display items (coauthor draft numbering): Fig. 1 sites and EFPs, Table 1 skill, Table 2 site-year MAE
# bootstrap, Fig. 2 net mortality SHAP per site, Fig. 3 High vs Low+Mid, Fig. 4 relative mortality vs direction,
# Fig. 5 residual by mortality class. Learner: AutoGluon; mortality class from relative mortality (mean of the 100-500 m
# buffers, site maximum across years; Low < mean - 0.5 SD, High > mean + 0.5 SD).
# ============================================================================
suppressMessages({library(data.table); library(writexl)})
args <- commandArgs(trailingOnly = FALSE)
here <- dirname(normalizePath(sub("^--file=", "", args[grep("^--file=", args)])))
O <- file.path(here, "source_data"); dir.create(O, showWarnings = FALSE)
EFP <- c("GPPsat", "NEPmax", "ETmax", "WUE")

ds <- fread(file.path(here, "derived_tables/paper_v5/paper_dataset_full_lags_monthly_tc30_minrun2_buf100-200-300-400-500.csv"))
meta <- fread(file.path(here, "site_metadata.csv"))
tm_cols <- paste0("relative_disturbance_", c(100, 200, 300, 400, 500), "m")
ds[, relative_mortality_pct := rowMeans(.SD), .SDcols = tm_cols]
site <- ds[, .(relative_mortality_site_max_pct = max(relative_mortality_pct)), by = SITE_ID]
mu <- mean(site$relative_mortality_site_max_pct); s <- sd(site$relative_mortality_site_max_pct)
b_lo <- max(mu - 0.5 * s, 0); b_hi <- mu + 0.5 * s
site[, mortality_class := fifelse(relative_mortality_site_max_pct < b_lo, "Low", fifelse(relative_mortality_site_max_pct > b_hi, "High", "Mid"))]
readme <- function(txt) data.table(note = txt)

# ---- Fig. 1 -------------------------------------------------------------------------------------------
f1 <- merge(meta[, .(SITE_ID, site_name, latitude = location_lat, longitude = location_long, igbp, network = product_source_network,
                     n_site_years, first_year, last_year)], site, by = "SITE_ID")
write_xlsx(list(README = readme(c("Fig. 1: sites (map) and distributions of the four ecosystem functional properties.",
                                  "sites: one row per site; site_years: one row per site-year with the four properties.")),
                sites = f1, site_years = ds[, c("SITE_ID", "YEAR", EFP), with = FALSE]), file.path(O, "Fig1_sites_and_EFPs.xlsx"))

# ---- Table 1 ------------------------------------------------------------------------------------------
m <- fread(file.path(here, "AutoGluon/outputs/efp_v5_nothresh/AG_metrics_repCV.csv"))[response %in% EFP]
m[, c("model_set", "window") := tstrsplit(model, "_")][, model_set := fifelse(model_set == "M3", "C+T", "C+T+M")]
write_xlsx(list(README = readme("Table 1: AutoGluon skill under repeated leave-one-site-out cross-validation."),
                skill = m[, .(response, window, model_set, n_sites, n_site_years = n_pairs, R2, RMSE, MAE)]),
           file.path(O, "Table1_skill.xlsx"))

# ---- Table 2 ------------------------------------------------------------------------------------------
b <- fread(file.path(here, "plots/paper_v5_nothresh/paired_bootstrap_statistics.csv"))[learner == "AG" & metric == "MAE" & level == "siteyear" &
                                                                         subset %in% c("all", "high", "yr_midhigh", "yr_high")]
write_xlsx(list(README = readme(c("Table 2: change of the pooled site-year MAE when mortality is added (AutoGluon).",
                                  "pct = % change; pct_lo/pct_hi = 95 % site-block bootstrap interval; p_perm/q = paired permutation (sites swapped), BH-corrected across the three learners.",
                                  "subset: all site-years; high = High sites; yr_midhigh / yr_high = site-years whose own relative mortality exceeds the Mid / High break.")),
                statistics = b[, .(response, window, subset, n_sites = n_units, n_site_years = n_obs, diff, pct, pct_lo, pct_hi, p_perm, q)]),
           file.path(O, "Table2_siteyear_MAE_bootstrap.xlsx"))

# ---- Figs 2-4: net signed mortality SHAP per site -------------------------------------------------------
sh <- fread(file.path(here, "AutoGluon/outputs/efp_v5_nothresh/AG_site_signed_shap_M4.csv"))[group == "Disturbance" & response %in% EFP]
sh[, window := sub("M4_", "", model)]
sh <- merge(sh[, .(SITE_ID = test_site, response, window, net_mortality_shap = mean_signed_shap)], site, by = "SITE_ID")
write_xlsx(list(README = readme(c("Fig. 2: net signed SHAP of the mortality block per site (C+T+M, AutoGluon, 24 months).",
                                  "Below zero = mortality lowers the predicted property. Sites ordered by relative_mortality_site_max_pct.")),
                per_site = sh[window == "24m"][order(-relative_mortality_site_max_pct)]), file.path(O, "Fig2_net_mortality_shap_per_site.xlsx"))

sh[, group2 := fifelse(mortality_class == "High", "High", "Low+Mid")]
t3 <- sh[, {w <- wilcox.test(net_mortality_shap[group2 == "High"], net_mortality_shap[group2 == "Low+Mid"], exact = FALSE)
           .(n_high = sum(group2 == "High"), n_lowmid = sum(group2 == "Low+Mid"), median_high = median(net_mortality_shap[group2 == "High"]),
             median_lowmid = median(net_mortality_shap[group2 == "Low+Mid"]), p = w$p.value)}, by = .(response, window)]
t3[, q := p.adjust(p, "BH")]
write_xlsx(list(README = readme("Fig. 3: net mortality SHAP of High vs Low+Mid sites (AutoGluon); Mann-Whitney U, BH-corrected across the figure."),
                per_site = sh[, .(SITE_ID, response, window, net_mortality_shap, mortality_class, group = group2)], statistics = t3),
           file.path(O, "Fig3_high_vs_lowmid.xlsx"))

t4 <- sh[window == "24m", {ct <- suppressWarnings(cor.test(relative_mortality_site_max_pct, net_mortality_shap, method = "spearman", exact = FALSE))
                          .(n_sites = .N, spearman_rho = unname(ct$estimate), p = ct$p.value)}, by = response]
t4[, q := p.adjust(p, "BH")]
write_xlsx(list(README = readme("Fig. 4: site maximum of relative mortality vs net mortality SHAP (AutoGluon, 24 months); Spearman correlation."),
                per_site = sh[window == "24m", .(SITE_ID, response, relative_mortality_site_max_pct, net_mortality_shap)], statistics = t4),
           file.path(O, "Fig4_mortality_vs_direction.xlsx"))

# ---- Fig. 5 -------------------------------------------------------------------------------------------
r5 <- fread(file.path(here, "plots/paper_v5_nothresh/dose_response/residual_by_mortality_AG_24m.csv"))
c5 <- fread(file.path(here, "plots/paper_v5_nothresh/dose_response/residual_correction_by_mortality_AG_24m.csv"))
tr <- fread(file.path(here, "plots/paper_v5_nothresh/dose_response/residual_correction_trend_AG_24m.csv"))
write_xlsx(list(README = readme(c("Fig. 5: mean residual (observed - predicted) per class of the site-year's relative mortality, without (C+T) and with mortality (C+T+M), AutoGluon, 24 months.",
                                  "residual_by_class: mean and 95 % site-block bootstrap interval; correction_by_class: residual(C+T+M) - residual(C+T) with one-sided paired permutation (BH);",
                                  "correction_trend: mixed-model slope of the correction per +10 % relative mortality with site-block bootstrap.")),
                residual_by_class = r5, correction_by_class = c5, correction_trend = tr), file.path(O, "Fig5_residual_by_mortality_class.xlsx"))

cat(sprintf("source data written to %s (%d files); class breaks %.1f / %.1f %%\n", O, length(list.files(O)), b_lo, b_hi))
