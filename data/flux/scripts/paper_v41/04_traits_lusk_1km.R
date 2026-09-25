#!/usr/bin/env Rscript
# ============================================================================
# Paper dataset (deadtrees v4.1), step 04 - plant traits per site.
# Source: Lusk et al. 2025 community-weighted means, Shrub_Tree_Grass, 1 km
#   data/trait_maps/lusk_et-al_2025/cwms/Shrub_Tree_Grass/1km  (analysis-ready + hydraulic/splot_gbif)
# Same source and 500 m scale as trait_biogeography_test/scripts/02_extract_traits.R, but
# AREA-WEIGHTED: every 1 km pixel the 500 m circle touches counts by its overlap fraction.
# The earlier centre-in-polygon extraction left 18 sites all-NA, because a 500 m circle often
# contains no 1 km pixel centre (same issue as the forest-age extraction).
# Columns: <trait> (weighted mean), plus aoa_<trait> for analysis-ready traits (share of the buffer
# inside the model's area of applicability). Hydraulic WUE is renamed WUE_trait (EFP clash).
# ============================================================================
# pin PROJ to this env's own database; an inherited PROJ_DATA/PROJ_LIB from another conda install breaks EPSG lookups
Sys.setenv(PROJ_DATA = "/home/nk1125/miniconda3/envs/clean_r_env/share/proj", PROJ_LIB = "/home/nk1125/miniconda3/envs/clean_r_env/share/proj")
suppressMessages({library(terra); library(data.table)})
setwd("/mnt/gsdata/projects/panops/panops-data-registry/data/flux")
TD <- "/mnt/gsdata/projects/panops/panops-data-registry/data/trait_maps/lusk_et-al_2025/cwms/Shrub_Tree_Grass/1km"
BUF <- 500
meta <- fread("fluxnet_2017_2025_V02/fluxnet_site_metadata_clean_with_elevation.csv")
efp_sites <- sub("_yearly_EFP\\.csv$", "", list.files("fluxnet_2017_2025_V02/EFP_outputs_corrected/yearly_EFP_per_site"))
sites <- meta[SITE_ID %in% efp_sites & is.finite(LOCATION_LAT) & is.finite(LOCATION_LONG)]
cat(sprintf("EFP sites: %d | with coordinates: %d\n", length(efp_sites), nrow(sites)))

names_tab <- fread("trait_biogeography_test/data/trait_metadata.csv")
ar_files <- list.files(file.path(TD, "analysis-ready"), pattern = "[.]tif$", full.names = TRUE)
hy_files <- unlist(lapply(list.dirs(file.path(TD, "hydraulic"), recursive = FALSE),
  function(d) list.files(file.path(d, "splot_gbif"), pattern = "[.]tif$", full.names = TRUE)))

ref <- rast(ar_files[1])
buf <- buffer(project(vect(as.data.frame(sites), geom = c("LOCATION_LONG", "LOCATION_LAT"), crs = "EPSG:4326"), crs(ref)), width = BUF)

wmean <- function(r) {
  e <- terra::extract(r, buf, exact = TRUE)
  names(e)[2] <- "v"
  e <- as.data.table(e)[is.finite(v) & fraction > 0]
  w <- e[, .(val = sum(v * fraction) / sum(fraction)), by = ID]
  out <- rep(NA_real_, nrow(sites)); out[w$ID] <- w$val; out
}

res <- data.table(SITE_ID = sites$SITE_ID)
for (f in ar_files) {
  id <- as.integer(sub("^X(\\d+)_.*", "\\1", basename(f)))
  nm <- names_tab[try_id == id, short]; if (!length(nm)) nm <- sprintf("trait_X%d", id)
  r <- rast(f)
  res[[nm]] <- wmean(r[[1]]); res[[paste0("aoa_", nm)]] <- wmean(r[[3]])
  cat(sprintf("  %-22s NA: %d\n", nm, sum(is.na(res[[nm]]))))
}
for (f in hy_files) {
  nm <- basename(dirname(dirname(f))); if (nm == "WUE") nm <- "WUE_trait"
  res[[nm]] <- wmean(rast(f)[[1]])
  cat(sprintf("  %-22s NA: %d\n", nm, sum(is.na(res[[nm]]))))
}
fwrite(res, "derived_tables/paper_v41/traits_lusk1km_500m_by_site.csv")
tr <- setdiff(names(res), c("SITE_ID", grep("^aoa_", names(res), value = TRUE)))
cat(sprintf("sites %d | traits %d | sites with any trait NA: %d | all NA: %d\nSTEP04_DONE\n", nrow(res), length(tr),
            sum(rowSums(is.na(res[, ..tr])) > 0), sum(rowSums(is.na(res[, ..tr])) == length(tr))))
