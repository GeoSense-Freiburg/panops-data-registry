#!/usr/bin/env Rscript
# ============================================================================
# Yearly accumulated flux totals, growing season only, from FLUXNET DAILY
# (FLUXMET_DD) files in fluxnet_2017_2025_V02/. Self-contained: does not read
# or write anything under the main project's derived_tables/ or plots/ -
# only the established 93-site tc>=30 SITE_ID list is read from there, to
# keep the site scope identical without duplicating the sites themselves.
#
# WHY THIS EXISTS: GPPsat/NEPmax/ETmax/WUE (the EFPs used everywhere else in
# this project) each bake in some of the same climate quantities (SW_IN, TA,
# VPD) used as predictors - see daily_reports/2026-09-17-efp-calculation-and-
# circularity.md. A straight yearly SUM of the raw fluxes has no such
# dependency (GPP/RECO/NEE are already-partitioned mass fluxes; only ET
# needs TA, and only for the LE->ET unit conversion, not as a percentile/
# curve-fit driver) - a genuinely circularity-free response.
#
# WHY DAILY, NOT HALF-HOURLY: a first version summed the raw HH file
# (GPP_NT_VUT_REF etc.), gated on >=80% half-hourly coverage within the
# growing season. On a 15-site smoke test, median coverage was ~44% and only
# 13% of site-years cleared 80% - NEE_VUT_REF/GPP_NT_VUT_REF are NOT fully
# gap-filled at half-hourly resolution the way TA_F/SW_IN_F are (u-star
# filtering removes a large, systematic share, mostly at night). The FLUXMET
# DD file ONEFlux ships alongside HH is ~86% complete for the same site/
# variable/period - daily values are a MEAN RATE (not pre-integrated, verified
# against known GPP magnitudes: ~3 umol/m2/s in June/July, ~0.2 in January),
# so the same unit-conversion logic applies, just with an 86400s/day
# multiplier instead of 1800s/half-hour, and no need for a strict half-hourly
# coverage gate.
#
# Growing season: SAME method and threshold as the existing EFP pipeline
# (notebooks/14_calc_EFPs.ipynb, EFPcalc_year(), GSfilt=0.3): GPP-threshold
# growing-season filter, threshold = 0.3 x 95th percentile of annual GPP.
# Called directly as bigleaf::filter.growing.season(GPPd, tGPP), NOT through
# the filter.data() wrapper - that wrapper defaults records.per.hour=2 (i.e.
# assumes half-hourly input) and re-aggregates every 48 rows into one "day"
# before smoothing; fed daily rows directly, that silently corrupts the
# smoothing window and the filter excludes ~0 days even at boreal sites with
# obvious winter dormancy (caught via a 15-site smoke test - every single
# site-year showed n_days_growing_season == n_days_total). filter.growing.
# season() takes a plain daily-GPP vector (any unit) and has no such issue.
#
# Units: GPP/RECO/NEE daily means are umol CO2 m-2 s-1 -> g C m-2/day via
# (86400 s/day) x (12.011 g C / mol) x (1e-6 mol/umol), summed over growing-
# season days. NEP = -NEE. ET via bigleaf::LE.to.ET(LE, TA) * 86400 (mm/day),
# summed to mm. WUE_annual = GPP_sum / ET_sum (ratio of sums, not a mean of
# daily ratios - the standard ecosystem-scale WUE definition).
#
#   Rscript extract_yearly_flux_totals.R [n_sites_for_smoke_test]
#   (omit, or 0, for the full 93-site run)
# ============================================================================
suppressMessages({library(dplyr); library(purrr); library(stringr); library(data.table)})
PROJECT_ROOT <- "/mnt/gsdata/projects/panops/panops-data-registry/data/flux"
setwd(PROJECT_ROOT)
OUT_DIR <- "predict_fluxes/derived_tables"; dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)
LOG_DIR <- "predict_fluxes/logs"; dir.create(LOG_DIR, showWarnings = FALSE, recursive = TRUE)
BASE_DIR <- "fluxnet_2017_2025_V02"
START_YEAR <- 2017; END_YEAR <- 2025
GSfilt <- 0.3          # identical threshold to EFPcalc_year()
MIN_COVERAGE <- 0.7    # daily coverage is much better than half-hourly (~86% typical);
                       # still gate on it so a SUM (linearly sensitive to gaps, unlike a
                       # percentile) isn't silently deflated for unusually gappy years.

args <- commandArgs(trailingOnly = TRUE)
N_SMOKE <- if (length(args) >= 1) as.integer(args[1]) else 0

if (!requireNamespace("bigleaf", quietly = TRUE)) stop("bigleaf package required.")

sites93 <- unique(fread("derived_tables/outputs_afterEGU_results/v10/v10_B1_GPPsat_harmonized.csv",
                         select = "SITE_ID")$SITE_ID)
cat("Site scope: tc>=30, ", length(sites93), " sites (unchanged from the main project)\n", sep = "")

clean_col <- function(x) { x[x == -9999] <- NA_real_; x }

read_fluxmet_dd <- function(zip_path, dd_file, site_id, start_year = START_YEAR, end_year = END_YEAR) {
  dat <- read.csv(unz(zip_path, dd_file), stringsAsFactors = FALSE)
  ts <- as.character(dat$TIMESTAMP)
  tibble(
    SITE_ID  = site_id,
    YEAR     = as.integer(substr(ts, 1, 4)),
    DOY      = as.integer(format(as.Date(ts, format = "%Y%m%d"), "%j")),
    GPP_NT   = clean_col(dat$GPP_NT_VUT_REF),
    RECO_NT  = clean_col(dat$RECO_NT_VUT_REF),
    NEE      = clean_col(dat$NEE_VUT_REF),
    LE       = clean_col(dat$LE_F_MDS),
    TA       = clean_col(dat$TA_F)
  ) %>%
    filter(YEAR >= start_year, YEAR <= end_year)
}

GC_PER_DAY <- 86400 * 12.011 * 1e-6  # umol CO2 m-2 s-1 -> g C m-2, per day

accumulate_year <- function(dat_year, site, yr) {
  empty <- tibble(SITE_ID = site, YEAR = yr, GPP_sum_gC = NA_real_, RECO_sum_gC = NA_real_,
                   NEP_sum_gC = NA_real_, ET_sum_mm = NA_real_, WUE_annual = NA_real_,
                   n_days_total = nrow(dat_year), n_days_growing_season = NA_integer_,
                   frac_valid_GPP = NA_real_, frac_valid_NEE = NA_real_, frac_valid_LE = NA_real_,
                   status = "failed")
  if (nrow(dat_year) < 90) { empty$status <- "insufficient_data"; return(empty) }  # <3 months

  dat_year <- dat_year[order(dat_year$DOY), ]
  gs_flag <- tryCatch(
    bigleaf::filter.growing.season(dat_year$GPP_NT, tGPP = GSfilt),
    error = function(e) { message("  filter.growseas error ", site, "-", yr, ": ", e$message); NULL }
  )
  if (is.null(gs_flag)) { empty$status <- "no_growing_season"; return(empty) }
  gs <- dat_year[gs_flag == 1, ]
  if (nrow(gs) == 0) { empty$status <- "no_growing_season"; return(empty) }

  frac_gpp <- mean(!is.na(gs$GPP_NT)); frac_nee <- mean(!is.na(gs$NEE)); frac_le <- mean(!is.na(gs$LE))
  coverage_ok <- min(frac_gpp, frac_nee, frac_le) >= MIN_COVERAGE

  gpp_sum  <- sum(gs$GPP_NT, na.rm = TRUE) * GC_PER_DAY
  reco_sum <- sum(gs$RECO_NT, na.rm = TRUE) * GC_PER_DAY
  nep_sum  <- -sum(gs$NEE, na.rm = TRUE) * GC_PER_DAY

  et_day <- tryCatch(bigleaf::LE.to.ET(gs$LE, gs$TA) * 86400, error = function(e) rep(NA_real_, nrow(gs)))
  et_sum <- sum(et_day, na.rm = TRUE)
  wue <- if (is.finite(et_sum) && et_sum > 0) gpp_sum / et_sum else NA_real_

  if (!coverage_ok) {
    return(tibble(SITE_ID = site, YEAR = yr, GPP_sum_gC = NA_real_, RECO_sum_gC = NA_real_,
                  NEP_sum_gC = NA_real_, ET_sum_mm = NA_real_, WUE_annual = NA_real_,
                  n_days_total = nrow(dat_year), n_days_growing_season = nrow(gs),
                  frac_valid_GPP = frac_gpp, frac_valid_NEE = frac_nee, frac_valid_LE = frac_le,
                  status = "low_coverage"))
  }

  tibble(SITE_ID = site, YEAR = yr, GPP_sum_gC = gpp_sum, RECO_sum_gC = reco_sum,
         NEP_sum_gC = nep_sum, ET_sum_mm = et_sum, WUE_annual = wue,
         n_days_total = nrow(dat_year), n_days_growing_season = nrow(gs),
         frac_valid_GPP = frac_gpp, frac_valid_NEE = frac_nee, frac_valid_LE = frac_le, status = "ok")
}

all_zips <- list.files(BASE_DIR, pattern = "\\.zip$", full.names = TRUE)
zip_site <- str_extract(basename(all_zips), "[A-Z]{2}-[A-Za-z0-9]{3}")
zips_93 <- all_zips[zip_site %in% sites93]
site_of_zip <- str_extract(basename(zips_93), "[A-Z]{2}-[A-Za-z0-9]{3}")
cat("Zip files matching the 93-site scope:", length(zips_93), "\n")
stopifnot(length(zips_93) == length(sites93))

if (N_SMOKE > 0) {
  keep <- seq_len(min(N_SMOKE, length(zips_93)))
  zips_93 <- zips_93[keep]; site_of_zip <- site_of_zip[keep]
  cat("SMOKE TEST: running on", length(zips_93), "site(s) only\n")
}

results <- list(); failed <- list()
for (i in seq_along(zips_93)) {
  site_i <- site_of_zip[i]; t0 <- Sys.time()
  dd_file <- tryCatch({
    inside <- unzip(zips_93[i], list = TRUE)$Name
    inside[str_detect(inside, "FLUXNET_FLUXMET_DD") & str_detect(inside, "\\.csv$")][1]
  }, error = function(e) NA_character_)
  if (is.na(dd_file)) { message("[", i, "/", length(zips_93), "] ", site_i, ": no DD file in zip, skipped"); next }

  out_i <- tryCatch({
    dat_i <- read_fluxmet_dd(zips_93[i], dd_file, site_i)
    if (nrow(dat_i) == 0) stop("no rows after year filter")
    yrs <- sort(unique(dat_i$YEAR))
    map_dfr(yrs, function(yr) accumulate_year(dat_i %>% filter(YEAR == yr), site_i, yr))
  }, error = function(e) {
    failed[[site_i]] <<- e$message
    message("[", i, "/", length(zips_93), "] ", site_i, ": FAILED - ", e$message)
    NULL
  })
  if (!is.null(out_i)) {
    results[[site_i]] <- out_i
    el <- round(as.numeric(difftime(Sys.time(), t0, units = "secs")), 1)
    cat(sprintf("[%d/%d] %s: %d site-years (%.1fs)\n", i, length(zips_93), site_i, nrow(out_i), el))
  }
}

out <- bind_rows(results)
fn <- if (N_SMOKE > 0) sprintf("yearly_flux_totals_raw_SMOKE%d.csv", N_SMOKE) else "yearly_flux_totals_raw.csv"
fwrite(out, file.path(OUT_DIR, fn))
if (length(failed) > 0) fwrite(data.table(SITE_ID = names(failed), error = unlist(failed)),
                                file.path(LOG_DIR, "extract_yearly_flux_totals_failed_sites.csv"))

cat("\nSites processed:", length(results), " / ", length(zips_93), " | failed:", length(failed), "\n")
cat("Rows (site-years):", nrow(out), " | status breakdown:\n")
print(table(out$status))
cat("\nSaved:", file.path(OUT_DIR, fn), "\n")
cat("EXTRACT_YEARLY_FLUX_TOTALS_DONE\n")
