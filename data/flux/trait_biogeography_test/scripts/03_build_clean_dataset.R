# 03_build_clean_dataset.R
# Merge site_table (EFPs + IGBP + coords) + climate_by_site + traits_by_site
# into one clean, site-level (cross-sectional) dataset, and report completeness.

proj_dir <- "/mnt/gsdata/projects/panops/panops-data-registry/data/flux/trait_biogeography_test"

sites <- read.csv(file.path(proj_dir, "data", "site_table.csv"), stringsAsFactors = FALSE)
clim  <- read.csv(file.path(proj_dir, "data", "climate_by_site.csv"), stringsAsFactors = FALSE)
trait <- read.csv(file.path(proj_dir, "data", "traits_by_site.csv"), stringsAsFactors = FALSE)

df <- merge(sites, clim, by = "SITE_ID")
df <- merge(df, trait, by = "SITE_ID")
cat("Merged dataset:", nrow(df), "sites x", ncol(df), "cols\n")

trait_cols <- setdiff(names(trait), c("SITE_ID", grep("^aoa_", names(trait), value = TRUE)))
clim_cols  <- setdiff(names(clim), c("SITE_ID", "n_years_climate"))
efp_cols   <- c("GPPsat", "NEPmax", "ETmax", "uWUE", "WUE")

cat("\n-- Missingness in trait columns --\n")
print(sapply(df[trait_cols], function(x) sum(is.na(x))))

cat("\n-- Area-of-applicability (AOA) coverage (mean fraction of 500m buffer inside AOA) --\n")
aoa_cols <- grep("^aoa_", names(df), value = TRUE)
aoa_summary <- sapply(df[aoa_cols], function(x) mean(x, na.rm = TRUE))
print(round(aoa_summary, 3))
low_aoa_sites <- sum(rowMeans(df[aoa_cols], na.rm = TRUE) < 0.5)
cat("Sites with <50% avg buffer coverage inside AOA across analysis-ready traits:", low_aoa_sites, "\n")

# Drop sites missing any of the 5 canonical EFPs or any trait/climate predictor
before <- nrow(df)
complete_rows <- complete.cases(df[, c(efp_cols, clim_cols, trait_cols)])
df_clean <- df[complete_rows, ]
cat("\nComplete-case sites (EFP + climate + traits all present):", nrow(df_clean),
    "(dropped", before - nrow(df_clean), ")\n")

write.csv(df, file.path(proj_dir, "data", "merged_dataset_full.csv"), row.names = FALSE)
write.csv(df_clean, file.path(proj_dir, "data", "clean_dataset.csv"), row.names = FALSE)
cat("\nWrote data/merged_dataset_full.csv (", nrow(df), "rows) and data/clean_dataset.csv (",
    nrow(df_clean), "rows)\n")

cat("\nIGBP distribution in clean_dataset:\n")
print(table(df_clean$IGBP))
