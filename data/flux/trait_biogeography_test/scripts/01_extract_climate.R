# 01_extract_climate.R
# Extract site-level long-term climate normals from the raw FLUXNET zips
# (Flux4Daniel/flux_raw_zip), using the yearly-resolution FLUXMET_YY table
# bundled in each zip (already QC'd/gap-filled annual sums by FLUXNET's own
# pipeline; we are not touching the HH data, just reading the YY rollup).

base_dir <- "/mnt/gsdata/projects/panops/panops-data-registry/data/flux"
proj_dir <- file.path(base_dir, "trait_biogeography_test")
zip_dir  <- file.path(base_dir, "Flux4Daniel/flux_raw_zip")

sites <- read.csv(file.path(proj_dir, "data", "site_table.csv"), stringsAsFactors = FALSE)
cat("Extracting climate for", nrow(sites), "sites\n")

vars <- c("TA_F", "VPD_F", "P_F", "SW_IN_F", "PA_F", "WS_F")

extract_one <- function(site_id, zip_file) {
  zpath <- file.path(zip_dir, zip_file)
  entries <- tryCatch(utils::unzip(zpath, list = TRUE)$Name, error = function(e) NULL)
  if (is.null(entries)) return(NULL)
  yy_entry <- entries[grepl("FLUXMET_YY_", entries)]
  if (length(yy_entry) != 1) return(NULL)

  con <- unz(zpath, yy_entry)
  yy <- tryCatch(read.csv(con), error = function(e) NULL)
  if (is.null(yy) || nrow(yy) == 0) return(NULL)

  present <- intersect(vars, names(yy))
  if (length(present) == 0) return(NULL)

  yy[present][yy[present] == -9999] <- NA

  means <- sapply(present, function(v) mean(yy[[v]], na.rm = TRUE))
  n_years <- sum(!is.na(yy$TA_F))

  res <- as.list(means)
  res$n_years_climate <- n_years
  res
}

results <- vector("list", nrow(sites))
for (i in seq_len(nrow(sites))) {
  r <- tryCatch(extract_one(sites$SITE_ID[i], sites$zip_file[i]),
                error = function(e) NULL)
  if (!is.null(r)) {
    r$SITE_ID <- sites$SITE_ID[i]
    results[[i]] <- r
  }
  if (i %% 50 == 0) cat("  ", i, "/", nrow(sites), "\n")
}

ok <- !vapply(results, is.null, logical(1))
cat("Successful extractions:", sum(ok), "/", nrow(sites), "\n")

clim <- do.call(rbind, lapply(results[ok], function(r) as.data.frame(r, stringsAsFactors = FALSE)))
names(clim) <- sub("_F$", "", names(clim))
names(clim)[names(clim) == "TA"] <- "TA_mean"
names(clim)[names(clim) == "VPD"] <- "VPD_mean"
names(clim)[names(clim) == "P"] <- "P_mean_annual"
names(clim)[names(clim) == "SW_IN"] <- "SW_IN_mean"
names(clim)[names(clim) == "PA"] <- "PA_mean"
names(clim)[names(clim) == "WS"] <- "WS_mean"

# De Martonne aridity index: higher = wetter
clim$aridity_index <- clim$P_mean_annual / (clim$TA_mean + 10)

clim <- clim[, c("SITE_ID", "TA_mean", "VPD_mean", "P_mean_annual", "SW_IN_mean",
                  "PA_mean", "WS_mean", "aridity_index", "n_years_climate")]

write.csv(clim, file.path(proj_dir, "data", "climate_by_site.csv"), row.names = FALSE)
cat("\nWrote", nrow(clim), "rows to data/climate_by_site.csv\n")
cat("\nMissingness per column:\n")
print(sapply(clim, function(x) sum(is.na(x))))
