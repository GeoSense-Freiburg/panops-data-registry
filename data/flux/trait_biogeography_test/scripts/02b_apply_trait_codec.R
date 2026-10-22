# 02b_apply_trait_codec.R
# Rename analysis-ready trait columns from raw TRY-ID-derived names to the
# short names given by the user's codec, and write a trait_metadata.csv
# reference table (id, short, long, unit, source) for reporting.

suppressMessages(library(jsonlite))

proj_dir <- "/mnt/gsdata/projects/panops/panops-data-registry/data/flux/trait_biogeography_test"
codec <- fromJSON("/tmp/trait_codec.json")

traits <- read.csv(file.path(proj_dir, "data", "traits_by_site.csv"), stringsAsFactors = FALSE)

# map from the names 02_extract_traits.R produced -> TRY id
old_to_id <- c(
  Leaf_C = "13", Leaf_N_mass_ = "14", Leaf_width = "145", Leaf_C_N_ratio = "146",
  Leaf_P = "15", Stem_conduit_density = "169", Stem_conduit_diameter = "281",
  Leaf_area_3112_ = "3112", Leaf_area_3113_ = "3113", Leaf_area_3114_ = "3114",
  SLA = "3117", SSD = "4", Leaf_thickness = "46", Leaf_N_area_ = "50",
  Leaf_dry_mass = "55", RRD = "6", Leaf_delta_15N = "78"
)

rename_map <- c(old_to_id)
for (old in names(old_to_id)) {
  id <- old_to_id[[old]]
  new <- codec[[id]]$short
  names(traits)[names(traits) == old] <- new
  names(traits)[names(traits) == paste0("aoa_", old)] <- paste0("aoa_", new)
}

hydraulic_cols <- c("gsmax", "P12", "P50", "P88", "rdmax", "WUE")

write.csv(traits, file.path(proj_dir, "data", "traits_by_site.csv"), row.names = FALSE)
cat("Renamed columns. New trait columns:\n")
print(setdiff(names(traits), c("SITE_ID", grep("^aoa_", names(traits), value = TRUE))))

# metadata reference table
meta_rows <- lapply(names(old_to_id), function(old) {
  id <- old_to_id[[old]]
  data.frame(short = codec[[id]]$short, try_id = id, long = codec[[id]]$long,
             unit = codec[[id]]$unit, source = "analysis-ready (CWM, Lusk et al. 2025)")
})
meta_ar <- do.call(rbind, meta_rows)

meta_hy <- data.frame(
  short = hydraulic_cols,
  try_id = NA,
  long = c("Maximum stomatal conductance", "Water potential at 12% loss of hydraulic conductivity",
           "Water potential at 50% loss of hydraulic conductivity", "Water potential at 88% loss of hydraulic conductivity",
           "Maximum rooting depth (hydraulic)", "Water-use efficiency (trait-based)"),
  unit = c("mol m-2 s-1", "MPa", "MPa", "MPa", "m", "umol CO2 / mol H2O"),
  source = "hydraulic (CWM, Lusk et al. 2025)"
)

trait_metadata <- rbind(meta_ar, meta_hy)
write.csv(trait_metadata, file.path(proj_dir, "data", "trait_metadata.csv"), row.names = FALSE)
cat("\nWrote data/trait_metadata.csv with", nrow(trait_metadata), "trait definitions\n")
