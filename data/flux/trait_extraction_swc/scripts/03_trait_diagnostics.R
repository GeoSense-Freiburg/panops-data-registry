# 03_trait_diagnostics.R
# Per-trait reliability diagnostics across all sites, plus wide site x trait
# tables of the tower-cell value, its COV and its AOA flag.
#
# Outputs (outputs/):
#   trait_diagnostics.csv      one row per trait
#   traits_wide_tower_mean.csv site x trait, tower-cell CWM
#   traits_wide_tower_cov.csv  site x trait, tower-cell coefficient of variation
#   traits_wide_tower_aoa.csv  site x trait, tower-cell AOA mask (1 = outside AOA)

base_dir <- "/mnt/gsdata/projects/panops/panops-data-registry/data/flux"
out_dir  <- file.path(base_dir, "trait_extraction_swc", "outputs")

s <- read.csv(file.path(out_dir, "traits_site_summary.csv"), stringsAsFactors = FALSE)

diag <- do.call(rbind, lapply(split(s, s$trait_id), function(d) {
  v <- d$tower_mean[!is.na(d$tower_mean)]
  top_share <- if (length(v)) max(table(signif(v, 6))) / length(v) else NA_real_
  data.frame(
    trait_id = d$trait_id[1], trait = d$trait[1], group = d$group[1],
    n_sites = nrow(d), n_sites_valid = length(v),
    n_distinct_values = length(unique(signif(v, 6))),
    most_common_value_share = round(top_share, 3),
    n_negative = sum(v < 0),
    tower_min = min(v), tower_median = median(v), tower_max = max(v),
    median_tower_cov = median(d$tower_cov, na.rm = TRUE),
    p95_tower_cov = unname(quantile(d$tower_cov, 0.95, na.rm = TRUE)),
    n_sites_cov_gt_0.1 = sum(d$tower_cov > 0.1, na.rm = TRUE),
    n_sites_outside_aoa = sum(d$tower_aoa == 1, na.rm = TRUE),
    median_spatial_cv_3x3 = median(d$spatial_cv_3x3, na.rm = TRUE),
    # collapsed map: a few values repeated over many sites (e.g. a fill/default value)
    flag_collapsed = top_share > 0.2
  )
}))
diag$flag_negative <- diag$n_negative > 0 & !diag$trait_id %in% c("X78", "P12", "P50", "P88")  # d15N and P-values can be negative
diag <- diag[order(diag$group, diag$trait_id), ]
write.csv(diag, file.path(out_dir, "trait_diagnostics.csv"), row.names = FALSE)

to_wide <- function(col) {
  w <- reshape(s[, c("SITE_ID", "trait_id", col)], idvar = "SITE_ID", timevar = "trait_id", direction = "wide")
  names(w) <- sub(paste0("^", col, "[.]"), "", names(w))
  w[order(w$SITE_ID), ]
}
write.csv(to_wide("tower_mean"), file.path(out_dir, "traits_wide_tower_mean.csv"), row.names = FALSE)
write.csv(to_wide("tower_cov"), file.path(out_dir, "traits_wide_tower_cov.csv"), row.names = FALSE)
write.csv(to_wide("tower_aoa"), file.path(out_dir, "traits_wide_tower_aoa.csv"), row.names = FALSE)

print(diag[, c("trait_id", "trait", "group", "n_distinct_values", "most_common_value_share", "n_negative",
               "median_tower_cov", "n_sites_cov_gt_0.1", "n_sites_outside_aoa", "flag_collapsed", "flag_negative")],
      row.names = FALSE)
