# 00_build_site_table.R
# Merge site-level EFPs with site metadata (lat/lon/IGBP/elevation), keep sites
# that also have a raw FLUXNET zip available (needed for climate extraction).

base_dir <- "/mnt/gsdata/projects/panops/panops-data-registry/data/flux"
proj_dir <- file.path(base_dir, "trait_biogeography_test")

efp <- read.csv(file.path(base_dir, "Flux4Daniel/EFP_outputs/ALL_SITES_EFP.csv"),
                 stringsAsFactors = FALSE)
meta <- read.csv(file.path(base_dir, "Flux4Daniel/fluxnet_site_metadata_clean_with_elevation.csv"),
                  stringsAsFactors = FALSE)

cat("EFP rows:", nrow(efp), " | metadata rows:", nrow(meta), "\n")

# meta can have duplicate SITE_ID rows (multiple team members); collapse to one row/site
meta_site <- unique(meta[, c("SITE_ID", "LOCATION_LAT", "LOCATION_LONG", "IGBP", "LOCATION_ELEV",
                              "first_year", "last_year")])
meta_site <- meta_site[!duplicated(meta_site$SITE_ID), ]
cat("Unique metadata sites:", nrow(meta_site), "\n")

# Which sites have a raw zip available (needed for climate extraction)
zip_files <- list.files(file.path(base_dir, "Flux4Daniel/flux_raw_zip"), pattern = "\\.zip$")
zip_site_id <- sub("^[A-Za-z]+_([A-Za-z]{2}-[A-Za-z0-9]+)_.*", "\\1", zip_files)
zip_lookup <- data.frame(SITE_ID = zip_site_id, zip_file = zip_files, stringsAsFactors = FALSE)
zip_lookup <- zip_lookup[!duplicated(zip_lookup$SITE_ID), ]
cat("Sites with a raw zip:", nrow(zip_lookup), "\n")

df <- merge(efp, meta_site, by = "SITE_ID")
cat("After joining EFP + metadata:", nrow(df), "\n")

df <- merge(df, zip_lookup, by = "SITE_ID")
cat("After requiring a raw zip:", nrow(df), "\n")

# Quality gates:
#  - status == "ok" (EFP fit converged, per calc_EFPs_per_site_v2.ipynb)
#  - nyears >= 2 (EFPs are already a multi-year composite; require at least 2
#    years so the site-level value isn't a single-year snapshot)
#  - complete coordinates and IGBP
before <- nrow(df)
df <- df[df$status == "ok" & !is.na(df$nyears) & df$nyears >= 2 &
         !is.na(df$LOCATION_LAT) & !is.na(df$LOCATION_LONG) &
         df$IGBP != "" & !is.na(df$IGBP), ]
cat("After quality gates (status==ok, nyears>=2, coords/IGBP present):", nrow(df),
    " (dropped", before - nrow(df), ")\n")

# Drop land-cover classes excluded by the project convention (water/urban/barren/snow
# already excluded upstream per Flux4Daniel/readme.txt); also drop CRO (cropland) and
# CVM (mosaic) here since CWM trait maps are Shrub/Tree/Grass only and croplands are
# not represented in that trait space.
excl_igbp <- c("CRO", "CVM", "WAT", "URB", "BSV", "SNO")
before <- nrow(df)
df <- df[!(df$IGBP %in% excl_igbp), ]
cat("After excluding non-natural-vegetation IGBP classes (", paste(excl_igbp, collapse=","),
    "):", nrow(df), " (dropped", before - nrow(df), ")\n")

out_cols <- c("SITE_ID", "zip_file", "LOCATION_LAT", "LOCATION_LONG", "LOCATION_ELEV", "IGBP",
              "nyears", "first_year", "last_year",
              "GPPsat", "NEPmax", "ETmax", "uWUE", "WUE", "CUEeco90", "E0", "EF", "EFampl",
              "Gsmax", "Rb", "Rbmax", "TZ")
df <- df[, out_cols]
names(df)[names(df) == "LOCATION_LAT"] <- "lat"
names(df)[names(df) == "LOCATION_LONG"] <- "lon"
names(df)[names(df) == "LOCATION_ELEV"] <- "elev"

write.csv(df, file.path(proj_dir, "data", "site_table.csv"), row.names = FALSE)
cat("\nWrote", nrow(df), "sites to data/site_table.csv\n")
print(table(df$IGBP))
