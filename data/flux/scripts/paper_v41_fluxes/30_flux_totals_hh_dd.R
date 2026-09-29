#!/usr/bin/env Rscript
# ============================================================================
# paper_v41 FLUXES, step 30 - growing-season flux totals for the paper_v41
# site-years (106 sites / 388 site-years), as the response instead of the EFPs.
#
# Responses: GPP (GPP_NT_VUT_REF), NEP (= -NEE_VUT_REF), ET (LE_F_MDS -> mm via
# bigleaf::LE.to.ET with TA_F), WUE = GPP_sum / ET_sum (ratio of sums).
# Growing season: the SAME rule as the EFP pipeline and paper_v41 step 02 -
# daily mean of half-hourly GPP_NT_VUT_REF, bigleaf::filter.growing.season(tGPP = 0.3);
# totals are summed over the flagged days (not the start-end window).
#
# Computed TWICE from the same zip, so the source can be chosen on data:
#   HH: half-hourly values x 1800 s, summed over growing-season half-hours
#   DD: ONEFlux daily means (FLUXMET_DD) x 86400 s, summed over growing-season days
# For each: completeness (share of non-missing values within the season; HH also
# split into day / night by SW_IN_F > 0 and the share of measured NEE, QC = 0).
# The old predict_fluxes/ project found HH NEE/GPP only ~44 % complete (u*-filtered
# nights not gap-filled) and used DD (~86 %); this re-checks that on these site-years.
#
# Output: derived_tables/paper_v41_fluxes/flux_totals_hh_dd_by_site_year.csv
#   Rscript 30_flux_totals_hh_dd.R [n_sites_smoke]
# ============================================================================
Sys.setenv(OMP_NUM_THREADS = "1", OPENBLAS_NUM_THREADS = "1")
suppressMessages({library(dplyr); library(readr); library(stringr); library(data.table); library(parallel)})
setDTthreads(1)
setwd("/mnt/gsdata/projects/panops/panops-data-registry/data/flux")
source("scripts/paper_v41/paper_common.R")

args <- commandArgs(trailingOnly = TRUE)
N_SMOKE <- if (length(args) >= 1) as.integer(args[1]) else 0
N_CORES <- as.integer(Sys.getenv("PAPER_CORES", "24"))
BASE <- "fluxnet_2017_2025_V02"
# env FLUX_SITEYEARS = csv with SITE_ID, YEAR (default: the paper_v41 dataset); FLUX_OUT_DIR = output folder
OUT <- Sys.getenv("FLUX_OUT_DIR", "derived_tables/paper_v41_fluxes"); dir.create(OUT, showWarnings = FALSE, recursive = TRUE)
GSfilt <- 0.3
GC_HH <- 1800 * 12.011e-6      # umol CO2 m-2 s-1 over one half-hour -> g C m-2
GC_DD <- 86400 * 12.011e-6     # daily mean rate -> g C m-2 per day

sy_keep <- unique(fread(Sys.getenv("FLUX_SITEYEARS", PAPER_DATA), select = c("SITE_ID", "YEAR")))
sites <- sort(unique(sy_keep$SITE_ID))
if (N_SMOKE > 0) sites <- head(sites, N_SMOKE)

zips <- list.files(BASE, pattern = "\\.zip$", full.names = TRUE)
zsite <- str_extract(basename(zips), "(?<=_)[A-Z]{2}-[A-Za-z0-9]{3}(?=_FLUXNET)")
na9 <- function(x) { x <- suppressWarnings(as.numeric(x)); x[x == -9999] <- NA_real_; x }
col <- function(d, v) if (v %in% names(d)) na9(d[[v]]) else rep(NA_real_, nrow(d))
fin_sum <- function(x) sum(x[is.finite(x)])

one_site <- function(site) {
  zp <- zips[zsite == site]
  years <- sy_keep[SITE_ID == site, YEAR]
  if (!length(zp)) return(data.table(SITE_ID = site, status = "no_zip"))
  if (length(zp) > 1) {                         # same rule as step 02: zip covering most of the years
    yrs <- str_match(basename(zp), "_(\\d{4})-(\\d{4})_")
    cover <- sapply(seq_along(zp), function(i) sum(years >= as.integer(yrs[i, 2]) & years <= as.integer(yrs[i, 3])))
    zp <- zp[which.max(cover)]
  }
  tryCatch({
    inside <- unzip(zp, list = TRUE)$Name
    hhf <- inside[str_detect(inside, "FLUXNET_FLUXMET_HH") & str_detect(inside, "\\.csv$")][1]
    ddf <- inside[str_detect(inside, "FLUXNET_FLUXMET_DD") & str_detect(inside, "\\.csv$")][1]
    hh <- as.data.table(readr::read_csv(unz(zp, hhf), show_col_types = FALSE, progress = FALSE,
                                        col_types = readr::cols(.default = "c")))
    dd <- as.data.table(readr::read_csv(unz(zp, ddf), show_col_types = FALSE, progress = FALSE,
                                        col_types = readr::cols(.default = "c")))
    ts <- as.character(hh$TIMESTAMP_START)
    H <- data.table(YEAR = as.integer(substr(ts, 1, 4)),
                    DOY = as.integer(format(as.Date(substr(ts, 1, 8), "%Y%m%d"), "%j")),
                    GPP = col(hh, "GPP_NT_VUT_REF"), NEE = col(hh, "NEE_VUT_REF"),
                    NEE_QC = col(hh, "NEE_VUT_REF_QC"), LE = col(hh, "LE_F_MDS"),
                    TA = col(hh, "TA_F"), SW = col(hh, "SW_IN_F"))
    tsd <- as.character(dd$TIMESTAMP)
    D <- data.table(YEAR = as.integer(substr(tsd, 1, 4)),
                    DOY = as.integer(format(as.Date(tsd, "%Y%m%d"), "%j")),
                    GPP = col(dd, "GPP_NT_VUT_REF"), NEE = col(dd, "NEE_VUT_REF"),
                    LE = col(dd, "LE_F_MDS"), TA = col(dd, "TA_F"))
    rbindlist(lapply(years, function(yr) {
      h <- H[YEAR == yr]; d <- D[YEAR == yr]
      out <- data.table(SITE_ID = site, YEAR = yr, status = "ok")
      if (nrow(h) < 48 * 90) { out$status <- "insufficient_hh"; return(out) }
      daily <- h[, .(g = mean(GPP, na.rm = TRUE)), by = DOY][order(DOY)]
      daily[!is.finite(g), g := NA_real_]
      gsf <- tryCatch(bigleaf::filter.growing.season(daily$g, tGPP = GSfilt), error = function(e) NULL)
      if (is.null(gsf) || !any(gsf == 1, na.rm = TRUE)) { out$status <- "no_growing_season"; return(out) }
      gdoy <- daily$DOY[which(gsf == 1)]
      hs <- h[DOY %in% gdoy]; ds <- d[DOY %in% gdoy]
      et_hh <- bigleaf::LE.to.ET(hs$LE, hs$TA) * 1800
      et_dd <- bigleaf::LE.to.ET(ds$LE, ds$TA) * 86400
      day <- is.finite(hs$SW) & hs$SW > 0
      out[, `:=`(
        n_gs_days = length(gdoy), gs_start_doy = min(gdoy), gs_end_doy = max(gdoy),
        # half-hourly
        HH_GPP = fin_sum(hs$GPP) * GC_HH, HH_NEP = -fin_sum(hs$NEE) * GC_HH, HH_ET = fin_sum(et_hh),
        HH_cov_GPP = mean(is.finite(hs$GPP)), HH_cov_NEE = mean(is.finite(hs$NEE)), HH_cov_LE = mean(is.finite(hs$LE)),
        HH_cov_NEE_day = mean(is.finite(hs$NEE[day])), HH_cov_NEE_night = mean(is.finite(hs$NEE[!day])),
        HH_frac_NEE_measured = mean(hs$NEE_QC == 0, na.rm = TRUE),
        # daily
        DD_GPP = fin_sum(ds$GPP) * GC_DD, DD_NEP = -fin_sum(ds$NEE) * GC_DD, DD_ET = fin_sum(et_dd),
        DD_n_days = nrow(ds),
        DD_cov_GPP = sum(is.finite(ds$GPP)) / length(gdoy), DD_cov_NEE = sum(is.finite(ds$NEE)) / length(gdoy),
        DD_cov_LE = sum(is.finite(ds$LE)) / length(gdoy))]
      out[, `:=`(HH_WUE = HH_GPP / HH_ET, DD_WUE = DD_GPP / DD_ET)]
      out
    }), fill = TRUE)
  }, error = function(e) data.table(SITE_ID = site, status = paste("error:", conditionMessage(e))))
}

t0 <- Sys.time()
res <- rbindlist(mclapply(sites, one_site, mc.cores = N_CORES), fill = TRUE)
tag <- if (N_SMOKE > 0) sprintf("_SMOKE%d", N_SMOKE) else ""
fwrite(res, file.path(OUT, sprintf("flux_totals_hh_dd_by_site_year%s.csv", tag)))
cat(sprintf("done in %.1f min | site-years %d | sites %d\n",
            as.numeric(difftime(Sys.time(), t0, units = "mins")), nrow(res), uniqueN(res$SITE_ID)))
print(table(res$status, useNA = "ifany"))
q <- function(x) round(quantile(x, c(0, .1, .25, .5, .75, 1), na.rm = TRUE), 3)
for (v in grep("_cov_|_frac_", names(res), value = TRUE)) { cat(sprintf("%-22s", v)); print(q(res[[v]])) }
cat("STEP30_DONE\n")
