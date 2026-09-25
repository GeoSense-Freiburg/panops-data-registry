#!/usr/bin/env Rscript
# ============================================================================
# Paper dataset (deadtrees v4.1), step 02 - from raw FLUXNET half-hourly data:
#   (a) NEPmax recomputed as in Migliavacca et al. 2021 (Nature 598:468):
#       90th percentile of ALL half-hourly NEP (= -NEE, day and night) in the
#       growing season, good-quality data only (QC 0/1). Replaces the old
#       definition in notebooks/14_calc_EFPs.ipynb (99th percentile of daytime
#       NEP, SW_IN > 200). The old value is recomputed alongside as a check:
#       it should reproduce NEPmax in yearly_EFP_per_site/.
#   (b) growing-season start/end per site-year (daily GPP > 30 % of its 95th
#       percentile, bigleaf::filter.growing.season on daily means - the same
#       rule and threshold as the EFP pipeline), needed for the pre-season
#       climate lag windows.
#   (c) monthly climate 2015-2025 with the same function as the EFP pipeline
#       (calc_monthly_meteo): mean/p05/p95 of TA, VPD, SW_IN, P sum/mean.
#       Starts in 2015 (not 2017 as the existing file) so that lag windows for
#       the early EFP years exist.
# Reading and QC follow read_fluxmet_hh() / EFPcalc_year() in 14_calc_EFPs.ipynb.
#
#   Rscript 02_nepmax90_gs_monthly_meteo.R [n_sites_smoke]
# ============================================================================
Sys.setenv(OMP_NUM_THREADS = "1", OPENBLAS_NUM_THREADS = "1")
suppressMessages({library(dplyr); library(readr); library(stringr); library(data.table); library(parallel)})
setDTthreads(1)
setwd("/mnt/gsdata/projects/panops/panops-data-registry/data/flux")
if (!requireNamespace("bigleaf", quietly = TRUE)) stop("bigleaf required")

args <- commandArgs(trailingOnly = TRUE)
N_SMOKE <- if (length(args) >= 1) as.integer(args[1]) else 0
N_CORES <- as.integer(Sys.getenv("PAPER_CORES", "24"))
BASE <- "fluxnet_2017_2025_V02"
EFP_DIR <- file.path(BASE, "EFP_outputs_corrected", "yearly_EFP_per_site")
OUT <- "derived_tables/paper_v41"
MET_START <- 2015; EFP_START <- 2017; END <- 2025; GSfilt <- 0.3

clean_col <- function(df, col) if (col %in% names(df)) dplyr::na_if(df[[col]], -9999) else NA_real_
q_safe <- function(x, p) { x <- x[is.finite(x)]; if (!length(x)) NA_real_ else as.numeric(quantile(x, p, names = FALSE)) }
mean_safe <- function(x) { x <- x[is.finite(x)]; if (!length(x)) NA_real_ else mean(x) }
sum_safe <- function(x) { x <- x[is.finite(x)]; if (!length(x)) NA_real_ else sum(x) }

read_hh <- function(zip_path, hh_file, site) {
  d <- readr::read_csv(unz(zip_path, hh_file), show_col_types = FALSE, progress = FALSE)
  d %>% mutate(
    SITE_ID = site,
    DT = lubridate::ymd_hm(as.character(TIMESTAMP_START), tz = "UTC"),
    YEAR = lubridate::year(DT), year = YEAR, MONTH = lubridate::month(DT),
    DOY = lubridate::yday(DT), doy = DOY,
    GPP_NT = clean_col(pick(everything()), "GPP_NT_VUT_REF"),
    NEE = clean_col(pick(everything()), "NEE_VUT_REF"),
    LE = clean_col(pick(everything()), "LE_F_MDS"), H = clean_col(pick(everything()), "H_F_MDS"),
    TA = clean_col(pick(everything()), "TA_F"), VPD = clean_col(pick(everything()), "VPD_F"),
    SW_IN = clean_col(pick(everything()), "SW_IN_F"), P = clean_col(pick(everything()), "P_F"),
    NEE_QC = if ("NEE_VUT_REF_QC" %in% names(d)) dplyr::na_if(NEE_VUT_REF_QC, -9999) else 0L,
    LE_QC = if ("LE_F_MDS_QC" %in% names(d)) dplyr::na_if(LE_F_MDS_QC, -9999) else 0L,
    H_QC = if ("H_F_MDS_QC" %in% names(d)) dplyr::na_if(H_F_MDS_QC, -9999) else 0L,
    TA_QC = if ("TA_F_QC" %in% names(d)) dplyr::na_if(TA_F_QC, -9999) else 0L
  ) %>% select(SITE_ID, YEAR, year, MONTH, DOY, doy, GPP_NT, NEE, LE, H, TA, VPD, SW_IN, P,
               NEE_QC, LE_QC, H_QC, TA_QC) %>%
    filter(YEAR >= MET_START, YEAR <= END)
}

monthly_meteo <- function(d) {
  d %>% group_by(SITE_ID, YEAR, MONTH) %>% summarise(
    TA_mean = mean_safe(TA), TA_p05 = q_safe(TA, .05), TA_p95 = q_safe(TA, .95),
    VPD_mean = mean_safe(VPD), VPD_p05 = q_safe(VPD, .05), VPD_p95 = q_safe(VPD, .95),
    SW_IN_mean = mean_safe(SW_IN), SW_IN_p05 = q_safe(SW_IN, .05), SW_IN_p95 = q_safe(SW_IN, .95),
    P_sum = sum_safe(P), P_mean = mean_safe(P), n_obs = n(), .groups = "drop")
}

site_year <- function(dy, site, yr) {
  out <- data.table(SITE_ID = site, YEAR = yr, n_hh = nrow(dy), gs_start_doy = NA_integer_, gs_end_doy = NA_integer_,
                    n_gs_days = NA_integer_, NEPmax90_gs_all = NA_real_, n_nep_hh = NA_integer_,
                    NEPmax99_day_old = NA_real_, status = "ok")
  if (nrow(dy) < 48 * 30 * 3) { out$status <- "insufficient_data"; return(out) }
  daily <- dy %>% group_by(DOY) %>% summarise(g = mean_safe(GPP_NT), .groups = "drop") %>% arrange(DOY)
  gsf <- tryCatch(bigleaf::filter.growing.season(daily$g, tGPP = GSfilt), error = function(e) NULL)
  if (!is.null(gsf) && any(gsf == 1, na.rm = TRUE)) {
    gd <- daily$DOY[which(gsf == 1)]
    out$gs_start_doy <- as.integer(min(gd)); out$gs_end_doy <- as.integer(max(gd)); out$n_gs_days <- length(gd)
  }
  qcv <- intersect(c("TA", "H", "LE", "NEE"), c("TA", "H", "LE", "NEE")[c("TA_QC", "H_QC", "LE_QC", "NEE_QC") %in% names(dy)])
  lrc <- tryCatch(suppressWarnings(bigleaf::filter.data(
    data.frame(dy), quality.control = length(qcv) > 0, filter.growseas = TRUE, filter.precip = FALSE,
    GPP = "GPP_NT", doy = "doy", year = "year", tGPP = GSfilt, precip = "P", tprecip = 0.1, precip.hours = 24,
    records.per.hour = 2, vars.qc = qcv, quality.ext = "_QC", good.quality = 1)),
    error = function(e) NULL)
  if (is.null(lrc) || nrow(lrc) <= 48 * 5) { out$status <- "no_gs_data"; return(out) }
  nep <- -lrc$NEE
  out$NEPmax90_gs_all <- q_safe(nep, 0.90); out$n_nep_hh <- sum(is.finite(nep))
  out$NEPmax99_day_old <- q_safe(nep[is.finite(lrc$SW_IN) & lrc$SW_IN * 2.11 > 200 * 2.11], 0.99)
  out
}

# ---- site list: every site with a yearly EFP file ------------------------------
sites <- sub("_yearly_EFP\\.csv$", "", list.files(EFP_DIR, pattern = "_yearly_EFP\\.csv$"))
zips <- list.files(BASE, pattern = "\\.zip$", full.names = TRUE)
zsite <- str_extract(basename(zips), "(?<=_)[A-Z]{2}-[A-Za-z0-9]{3}(?=_FLUXNET)")
cat(sprintf("sites with EFP files: %d | zips: %d | sites without a zip: %d | sites with >1 zip: %d\n",
            length(sites), length(zips), sum(!sites %in% zsite), sum(table(zsite[zsite %in% sites]) > 1)))
if (N_SMOKE > 0) sites <- head(sites, N_SMOKE)

one_site <- function(site) {
  zp <- zips[zsite == site]
  if (!length(zp)) return(list(sy = data.table(SITE_ID = site, status = "no_zip"), met = NULL))
  if (length(zp) > 1) {                                   # keep the zip whose years cover most of 2015-2025
    yrs <- str_match(basename(zp), "_(\\d{4})-(\\d{4})_")
    cover <- pmax(0, pmin(as.integer(yrs[, 3]), END) - pmax(as.integer(yrs[, 2]), MET_START) + 1)
    zp <- zp[which.max(cover)]
  }
  res <- tryCatch({
    inside <- unzip(zp, list = TRUE)$Name
    hh <- inside[str_detect(inside, "FLUXNET_FLUXMET_HH") & str_detect(inside, "\\.csv$")][1]
    if (is.na(hh)) stop("no HH file")
    d <- read_hh(zp, hh, site)
    met <- as.data.table(monthly_meteo(d))
    sy <- rbindlist(lapply(sort(unique(d$YEAR[d$YEAR >= EFP_START])), function(yr) site_year(d[d$YEAR == yr, ], site, yr)))
    list(sy = sy, met = met)
  }, error = function(e) list(sy = data.table(SITE_ID = site, status = paste("error:", conditionMessage(e))), met = NULL))
  res
}

t0 <- Sys.time()
res <- mclapply(sites, one_site, mc.cores = N_CORES)
sy <- rbindlist(lapply(res, `[[`, "sy"), fill = TRUE)
met <- rbindlist(lapply(res, `[[`, "met"), fill = TRUE)
tag <- if (N_SMOKE > 0) sprintf("_SMOKE%d", N_SMOKE) else ""
fwrite(sy, file.path(OUT, sprintf("nepmax90_gs_by_site_year%s.csv", tag)))
fwrite(met, file.path(OUT, sprintf("monthly_meteo_2015_2025%s.csv", tag)))
cat(sprintf("done in %.1f min | site-years %d | sites %d | monthly rows %d\n",
            as.numeric(difftime(Sys.time(), t0, units = "mins")), nrow(sy[!is.na(YEAR)]), uniqueN(sy$SITE_ID), nrow(met)))
print(table(sy$status, useNA = "ifany"))
cat("STEP02_DONE\n")
