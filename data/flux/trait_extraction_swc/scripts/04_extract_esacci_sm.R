# 04_extract_esacci_sm.R
# Extract daily ESA CCI Soil Moisture (COMBINED v09.2, 0.25 deg, surface ~2-5 cm)
# at each flux tower: the tower cell plus the mean of the 3 x 3 block around it
# (~75 x 75 km), which fills coastal / masked tower cells.
#
# Source: data/soil_moisture_cci/COMBINED/v09.2/<year>/*.nc in the PANOPS registry
# (downloaded 2026-10-27, 1978-11-01 .. 2024-12-31).
#
# Flag bits (flag column): 1 snow / frozen, 2 dense vegetation, 4 no convergence,
# 8 outside physical bounds, 16 weight below threshold, 32 all datasets unreliable,
# 64 barren ground advisory. sm is NA whenever no valid estimate exists.
#
# Outputs (outputs/):
#   esacci_sm_daily.csv.gz      one row per site x day
#   esacci_sm_site_year.csv     one row per site x year: valid days, flag fractions, mean sm
#   esacci_sm_site_summary.csv  one row per site: grid cell, record coverage

suppressMessages({
  library(terra)
  library(data.table)
  library(parallel)
})
Sys.setenv(PROJ_DATA = "/home/nk1125/miniconda3/envs/clean_r_env/share/proj")

base_dir <- "/mnt/gsdata/projects/panops/panops-data-registry/data/flux"
proj_dir <- file.path(base_dir, "trait_extraction_swc")
sm_dir   <- "/mnt/gsdata/projects/panops/panops-data-registry/data/soil_moisture_cci/COMBINED/v09.2"
out_dir  <- file.path(proj_dir, "outputs")
n_cores  <- 32
vars     <- c("sm", "sm_uncertainty", "flag", "freqbandID", "sensor")

sites <- read.csv(file.path(proj_dir, "data", "site_coordinates.csv"), stringsAsFactors = FALSE)
cat("Sites with coordinates:", nrow(sites), "\n")

files <- list.files(sm_dir, pattern = "^ESACCI-SOILMOISTURE.*[.]nc$", recursive = TRUE, full.names = TRUE)
dates <- as.Date(sub(".*-(\\d{8})000000-fv.*", "\\1", basename(files)), format = "%Y%m%d")
cat("Daily files:", length(files), "(", format(min(dates)), "..", format(max(dates)), ")\n")

# Tower cell and 3 x 3 neighbourhood on the regular 0.25 deg grid (longitude wraps at +/-180)
ref <- rast(files[1], subds = "sm")
tower_cell <- cellFromXY(ref, as.matrix(sites[, c("lon", "lat")]))
tower_rc <- rowColFromCell(ref, tower_cell)
offs <- expand.grid(dcol = -1:1, drow = -1:1)
nb_cells <- sapply(seq_len(nrow(sites)), function(i) {
  rr <- pmin(pmax(tower_rc[i, 1] + offs$drow, 1), nrow(ref))
  cc <- (tower_rc[i, 2] + offs$dcol - 1) %% ncol(ref) + 1
  cellFromRowCol(ref, rr, cc)
})  # 9 x n_sites

read_day <- function(k) {
  v <- values(rast(files[k], subds = vars))
  sm_nb <- matrix(v[nb_cells, "sm"], nrow = 9)
  sm_nb[is.nan(sm_nb)] <- NA
  data.table(
    SITE_ID = sites$SITE_ID, date = dates[k],
    sm = v[tower_cell, "sm"], sm_uncertainty = v[tower_cell, "sm_uncertainty"],
    flag = as.integer(v[tower_cell, "flag"]), freqbandID = as.integer(v[tower_cell, "freqbandID"]),
    sensor = as.integer(v[tower_cell, "sensor"]),
    sm_3x3_mean = colMeans(sm_nb, na.rm = TRUE), n_valid_3x3 = colSums(!is.na(sm_nb))
  )
}

t0 <- Sys.time()
daily <- rbindlist(mclapply(seq_along(files), read_day, mc.cores = n_cores))
cat("Read in", round(as.numeric(difftime(Sys.time(), t0, units = "mins")), 1), "min\n")
for (col in c("sm", "sm_uncertainty", "sm_3x3_mean")) set(daily, which(is.nan(daily[[col]])), col, NA_real_)
setorder(daily, SITE_ID, date)
stopifnot(nrow(daily) == nrow(sites) * length(files))

# Site x year coverage
daily[, year := as.integer(format(date, "%Y"))]
site_year <- daily[, .(
  n_days = .N,
  n_valid = sum(!is.na(sm)),
  n_valid_3x3 = sum(!is.na(sm_3x3_mean)),
  frac_valid = round(mean(!is.na(sm)), 3),
  frac_frozen = round(mean(bitwAnd(flag, 1L) > 0, na.rm = TRUE), 3),
  frac_dense_veg = round(mean(bitwAnd(flag, 2L) > 0, na.rm = TRUE), 3),
  sm_mean = round(mean(sm, na.rm = TRUE), 4),
  sm_3x3_mean = round(mean(sm_3x3_mean, na.rm = TRUE), 4)
), by = .(SITE_ID, year)]
site_year[is.nan(sm_mean), sm_mean := NA]
site_year[is.nan(sm_3x3_mean), sm_3x3_mean := NA]
daily[, year := NULL]

# Site summary: grid cell and record coverage (2017-2024 = flux study years in the record)
cell_xy <- xyFromCell(ref, tower_cell)
site_summary <- data.table(sites[, c("SITE_ID", "lat", "lon", "IGBP")],
                           cell_lon = cell_xy[, 1], cell_lat = cell_xy[, 2])
site_summary <- merge(site_summary, daily[, .(
  first_valid = if (any(!is.na(sm))) min(date[!is.na(sm)]) else as.Date(NA),
  last_valid = if (any(!is.na(sm))) max(date[!is.na(sm)]) else as.Date(NA),
  n_valid = sum(!is.na(sm)),
  frac_valid_2017_2024 = round(mean(!is.na(sm[date >= as.Date("2017-01-01")])), 3),
  frac_valid_3x3_2017_2024 = round(mean(!is.na(sm_3x3_mean[date >= as.Date("2017-01-01")])), 3),
  frac_dense_veg_2017_2024 = round(mean(bitwAnd(flag[date >= as.Date("2017-01-01")], 2L) > 0, na.rm = TRUE), 3)
), by = SITE_ID], by = "SITE_ID")

fwrite(daily, file.path(out_dir, "esacci_sm_daily.csv.gz"))
fwrite(site_year, file.path(out_dir, "esacci_sm_site_year.csv"))
fwrite(site_summary, file.path(out_dir, "esacci_sm_site_summary.csv"))
cat("\nWrote", nrow(daily), "site-day rows,", nrow(site_year), "site-year rows to", out_dir, "\n")
cat("Sites with no valid tower-cell value 2017-2024:", sum(site_summary$frac_valid_2017_2024 == 0), "\n")
cat("Median valid-day fraction 2017-2024: tower", median(site_summary$frac_valid_2017_2024),
    "| 3x3", median(site_summary$frac_valid_3x3_2017_2024), "\n")
