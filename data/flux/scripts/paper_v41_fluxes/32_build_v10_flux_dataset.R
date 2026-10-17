#!/usr/bin/env Rscript
# ============================================================================
# V10 FLUXES - the V10 tc>=30 modelling rows (93 sites / 395 site-years, deadtrees v2-2 mortality,
# V10 climate lags and traits) with growing-season flux totals as the response, built exactly as
# for paper_v41 (step 30: half-hourly GPP_NT_VUT_REF / -NEE_VUT_REF / LE_F_MDS, EFP growing-season
# rule, sums over the flagged days; step 31: >= 80 % completeness gate), so that the V10 and v4.1
# flux runs differ only in their predictors and site-years. Replaces, for this comparison, the
# earlier predict_fluxes/ run (daily files, >= 70 % gate, GPPsat tuning reused), which is left as is.
#
# Predictors: v10_lag2_B2_GPPsat_harmonized.csv (as predict_fluxes/): climate lag1 + lag2, mortality
# current / lag1 / lag2; the window is selected later by paper_cols() (PAPER_RESP_SET=v10flux).
# Site metadata (IGBP, coordinates) and tree cover (site mean of forest_mean_pct_500m, the V10 Fig. 5
# tree cover) from the V10 main table.
# Output: derived_tables/paper_v10_fluxes/paper_dataset_v10_fluxes.csv (+ _counts.txt, _dropped.csv)
#   Rscript 32_build_v10_flux_dataset.R     (runs step 30 for the V10 site-years first if needed)
# ============================================================================
suppressMessages(library(data.table))
ROOT <- "/mnt/gsdata/projects/panops/panops-data-registry/data/flux"; setwd(ROOT)
OUT <- "derived_tables/paper_v10_fluxes"; dir.create(OUT, showWarnings = FALSE, recursive = TRUE)
MIN_COV <- as.numeric(Sys.getenv("FLUX_MIN_COV", "0.8"))
RSCRIPT <- "/home/nk1125/miniconda3/envs/clean_r_env/bin/Rscript"

pred <- fread("derived_tables/outputs_afterEGU_results/v10_lag2/v10_lag2_B2_GPPsat_harmonized.csv")
drop_cols <- c("GPPsat", "NEPmax", "ETmax", "WUE", "uWUE", grep("_anom|^(GPPsat|NEPmax|ETmax|WUE|uWUE)_lag", names(pred), value = TRUE))
pred <- pred[, setdiff(names(pred), drop_cols), with = FALSE]
main <- fread("derived_tables/outputs_afterEGU_results/EFP_mortality_trait_hydro_combined_with_meteo_dist_lags.csv",
              select = c("SITE_ID", "IGBP", "LOCATION_LAT", "LOCATION_LONG", "forest_mean_pct_500m"))
meta <- main[, .(IGBP = IGBP[1], LOCATION_LAT = LOCATION_LAT[1], LOCATION_LONG = LOCATION_LONG[1],
                 tc_site_mean_500m = mean(forest_mean_pct_500m, na.rm = TRUE)), by = SITE_ID]
pred <- merge(pred, meta, by = "SITE_ID", all.x = TRUE)
stopifnot(!anyNA(pred$IGBP), !anyNA(pred$LOCATION_LAT))

# flux totals for these site-years (step 30, same code as for paper_v41)
tot_file <- file.path(OUT, "flux_totals_hh_dd_by_site_year.csv")
if (!file.exists(tot_file)) {
  sy <- file.path(OUT, "v10_site_years.csv"); fwrite(unique(pred[, .(SITE_ID, YEAR)]), sy)
  st <- system2(RSCRIPT, "scripts/paper_v41_fluxes/30_flux_totals_hh_dd.R",
                env = c(sprintf("FLUX_SITEYEARS=%s", sy), sprintf("FLUX_OUT_DIR=%s", OUT), "PAPER_CORES=24"))
  stopifnot(st == 0, file.exists(tot_file))
}
tot <- fread(tot_file)
tot[, cov := pmin(HH_cov_GPP, HH_cov_NEE, HH_cov_LE)]

d <- merge(pred, tot[, .(SITE_ID, YEAR, GPP_gs = HH_GPP, NEP_gs = HH_NEP, ET_gs = HH_ET, WUE_gs = HH_WUE,
                         gs_n_days_flux = n_gs_days, flux_cov = cov)], by = c("SITE_ID", "YEAR"), all.x = TRUE)
ok <- d$flux_cov >= MIN_COV & is.finite(d$GPP_gs + d$NEP_gs + d$ET_gs + d$WUE_gs)
ok[is.na(ok)] <- FALSE
drop <- d[!ok]; d <- d[ok]
setorder(d, SITE_ID, YEAR)
fwrite(d, file.path(OUT, "paper_dataset_v10_fluxes.csv"))
fwrite(drop[, .(SITE_ID, YEAR, IGBP, flux_cov = round(flux_cov, 3), gs_n_days_flux,
                relative_disturbance_500m, relative_mortality_500m)], file.path(OUT, "paper_dataset_v10_fluxes_dropped.csv"))
lost <- setdiff(unique(drop$SITE_ID), d$SITE_ID)
msg <- c(sprintf("V10 fluxes dataset (built %s)", format(Sys.time(), "%Y-%m-%d %H:%M")),
         sprintf("source rows (V10 tc>=30, v10_lag2 B2): %d site-years, %d sites", nrow(pred), uniqueN(pred$SITE_ID)),
         sprintf("completeness gate: >= %.0f %% of growing-season half-hours with GPP, NEE and LE", 100 * MIN_COV),
         sprintf("dropped: %d site-years (%d sites lose all their years: %s)", nrow(drop), length(lost), paste(lost, collapse = ", ")),
         sprintf("kept: %d site-years, %d sites, years %d-%d", nrow(d), uniqueN(d$SITE_ID), min(d$YEAR), max(d$YEAR)),
         "responses (median [IQR]):",
         sapply(c("GPP_gs", "NEP_gs", "ET_gs", "WUE_gs"), function(v) {
           q <- quantile(d[[v]], c(.25, .5, .75)); sprintf("  %-7s %.1f [%.1f-%.1f]", v, q[2], q[1], q[3]) }))
writeLines(msg, file.path(OUT, "paper_dataset_v10_fluxes_counts.txt")); cat(msg, sep = "\n")
