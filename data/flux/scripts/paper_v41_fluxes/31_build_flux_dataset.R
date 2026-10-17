#!/usr/bin/env Rscript
# ============================================================================
# paper_v41 FLUXES, step 31 - modelling dataset = the paper_v41 rows (106 sites /
# 388 site-years, identical predictors: pre-season climate lags, Lusk traits, v4.1
# mortality) with the EFP responses replaced by growing-season flux totals from step 30.
#
# Source: HALF-HOURLY totals (HH_*). They agree with the ONEFlux daily files at
# r >= 0.998 on these site-years (daily GPP/NEP ~3.8 % higher, ET equal).
# Completeness gate: a site-year is kept only if >= MIN_COV of the growing-season
# half-hours have GPP, NEE and LE. A seasonal SUM is linearly deflated by missing
# data (a percentile-based EFP is not), so partial seasons are dropped, not rescaled.
#
# Responses (units per growing season):
#   GPP_gs  g C m-2   (GPP_NT_VUT_REF)       NEP_gs  g C m-2   (-NEE_VUT_REF)
#   ET_gs   mm        (LE_F_MDS -> ET)       WUE_gs  g C mm-1  (GPP_gs / ET_gs)
# Output: derived_tables/paper_v41_fluxes/paper_dataset_fluxes.csv (+ _counts.txt, _dropped.csv)
# ============================================================================
suppressMessages(library(data.table))
setwd("/mnt/gsdata/projects/panops/panops-data-registry/data/flux")
Sys.setenv(PAPER_RESP_SET = "efp")          # read the EFP paper dataset as the row/predictor source
source("scripts/paper_v41/paper_common.R")
OUT <- "derived_tables/paper_v41_fluxes"
MIN_COV <- as.numeric(Sys.getenv("FLUX_MIN_COV", "0.8"))

base <- fread(PAPER_DATA)
tot <- fread(file.path(OUT, "flux_totals_hh_dd_by_site_year.csv"))
tot[, cov := pmin(HH_cov_GPP, HH_cov_NEE, HH_cov_LE)]
stopifnot(nrow(tot) == nrow(base), !anyDuplicated(tot[, .(SITE_ID, YEAR)]))

d <- merge(base, tot[, .(SITE_ID, YEAR, GPP_gs = HH_GPP, NEP_gs = HH_NEP, ET_gs = HH_ET, WUE_gs = HH_WUE,
                         gs_n_days_flux = n_gs_days, flux_cov = cov)], by = c("SITE_ID", "YEAR"))
drop <- d[!(flux_cov >= MIN_COV) | !is.finite(GPP_gs + NEP_gs + ET_gs + WUE_gs)]
d <- d[flux_cov >= MIN_COV & is.finite(GPP_gs + NEP_gs + ET_gs + WUE_gs)]
setorder(d, SITE_ID, YEAR)

fwrite(d, file.path(OUT, "paper_dataset_fluxes.csv"))
fwrite(drop[, .(SITE_ID, YEAR, IGBP, flux_cov = round(flux_cov, 3), gs_n_days_flux,
                relative_disturbance_500m, relative_mortality_500m)], file.path(OUT, "paper_dataset_fluxes_dropped.csv"))
msg <- c(sprintf("paper_v41 fluxes dataset (built %s)", format(Sys.time(), "%Y-%m-%d %H:%M")),
         sprintf("source rows (paper_v41 EFP dataset): %d site-years, %d sites", nrow(base), uniqueN(base$SITE_ID)),
         sprintf("completeness gate: >= %.0f %% of growing-season half-hours with GPP, NEE and LE", 100 * MIN_COV),
         sprintf("dropped: %d site-years (%d sites lose all their years: %s)", nrow(drop),
                 length(setdiff(unique(drop$SITE_ID), d$SITE_ID)), paste(setdiff(unique(drop$SITE_ID), d$SITE_ID), collapse = ", ")),
         sprintf("kept: %d site-years, %d sites, years %d-%d", nrow(d), uniqueN(d$SITE_ID), min(d$YEAR), max(d$YEAR)),
         "responses (median [IQR]):",
         sapply(c("GPP_gs", "NEP_gs", "ET_gs", "WUE_gs"), function(v) {
           q <- quantile(d[[v]], c(.25, .5, .75)); sprintf("  %-7s %.1f [%.1f-%.1f]", v, q[2], q[1], q[3]) }))
writeLines(msg, file.path(OUT, "paper_dataset_fluxes_counts.txt")); cat(msg, sep = "\n")
