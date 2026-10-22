# 02_extract_traits.R
# Extract CWM plant traits (analysis-ready + hydraulic, Lusk et al. 2025,
# Shrub_Tree_Grass 1km) at each flux tower as a 500 m buffer mean.
# Rasters are in EASE-Grid 2.0 (equal-area, metres) -> project tower points first.
# analysis-ready rasters are 3-band: [1] CWM mean, [2] coefficient of variation,
# [3] area-of-applicability (AOA) mask -> we keep the mean and the AOA fraction
# (share of the buffer inside the model's applicability domain) as a QC column.
# hydraulic rasters are single-band (predicted CWM value only, no AOA layer).

suppressMessages(library(terra))

base_dir  <- "/mnt/gsdata/projects/panops/panops-data-registry/data/flux"
proj_dir  <- file.path(base_dir, "trait_biogeography_test")
trait_dir <- "/mnt/gsdata/projects/panops/panops-data-registry/data/trait_maps/lusk_et-al_2025/cwms/Shrub_Tree_Grass/1km"
buffer_m  <- 500

sites <- read.csv(file.path(proj_dir, "data", "site_table.csv"), stringsAsFactors = FALSE)
cat("Extracting traits for", nrow(sites), "sites, buffer =", buffer_m, "m\n")

ar_files <- list.files(file.path(trait_dir, "analysis-ready"), pattern = "[.]tif$", full.names = TRUE)
hy_dirs  <- list.dirs(file.path(trait_dir, "hydraulic"), recursive = FALSE)
hy_files <- unlist(lapply(hy_dirs, function(d) list.files(file.path(d, "splot_gbif"), pattern = "[.]tif$", full.names = TRUE)))

clean_name <- function(nm) {
  nm <- sub(" \\(mean\\)$", "", nm)
  nm <- gsub("[ /()]+", "_", nm)
  gsub("_+", "_", nm)
}

# Project tower points once, to the CRS of the first raster (all trait rasters share it)
ref_r <- rast(ar_files[1])
pts_ll <- vect(sites, geom = c("lon", "lat"), crs = "EPSG:4326")
pts_proj <- project(pts_ll, crs(ref_r))
buf <- buffer(pts_proj, width = buffer_m)

trait_df <- data.frame(SITE_ID = sites$SITE_ID)

cat("\n-- analysis-ready traits --\n")
for (f in ar_files) {
  r <- rast(f)
  nm <- clean_name(names(r)[1])
  val <- terra::extract(r[[1]], buf, fun = mean, na.rm = TRUE, ID = FALSE)[, 1]
  aoa <- terra::extract(r[[3]], buf, fun = mean, na.rm = TRUE, ID = FALSE)[, 1]
  trait_df[[nm]] <- val
  trait_df[[paste0("aoa_", nm)]] <- aoa
  cat(" ", basename(f), "->", nm, "| n missing:", sum(is.na(val)), "\n")
}

cat("\n-- hydraulic traits --\n")
for (f in hy_files) {
  r <- rast(f)
  nm <- clean_name(names(r)[1])
  val <- terra::extract(r[[1]], buf, fun = mean, na.rm = TRUE, ID = FALSE)[, 1]
  trait_df[[nm]] <- val
  cat(" ", basename(f), "->", nm, "| n missing:", sum(is.na(val)), "\n")
}

write.csv(trait_df, file.path(proj_dir, "data", "traits_by_site.csv"), row.names = FALSE)
cat("\nWrote", nrow(trait_df), "rows x", ncol(trait_df), "cols to data/traits_by_site.csv\n")
