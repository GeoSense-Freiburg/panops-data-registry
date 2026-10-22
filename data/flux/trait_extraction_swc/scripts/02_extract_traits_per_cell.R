# 02_extract_traits_per_cell.R
# Extract every Lusk et al. 2025 CWM trait map (Shrub_Tree_Grass, 1 km;
# analysis-ready + experimental + hydraulic) per 1 km cell around each flux
# tower, so trait reliability can be judged per site and trait.
#
# Cells: the 3 x 3 block centred on the tower cell (tower cell + 8 neighbours,
# ~3 x 3 km), i.e. every cell a 500 m buffer can touch.
# analysis-ready / experimental rasters are 3-band:
#   [1] CWM mean, [2] coefficient of variation, [3] area-of-applicability mask
# hydraulic rasters are single-band (prediction only, no COV / AOA).
#
# Outputs (outputs/):
#   traits_per_cell_long.csv  one row per site x trait x cell
#   traits_site_summary.csv   one row per site x trait (tower cell + 3x3 stats)
#   trait_list.csv            trait id, name, group, bands available

suppressMessages(library(terra))
Sys.setenv(PROJ_DATA = "/home/nk1125/miniconda3/envs/clean_r_env/share/proj")

base_dir  <- "/mnt/gsdata/projects/panops/panops-data-registry/data/flux"
proj_dir  <- file.path(base_dir, "trait_extraction_swc")
trait_dir <- "/mnt/gsdata/projects/panops/panops-data-registry/data/trait_maps/lusk_et-al_2025/cwms/Shrub_Tree_Grass/1km"
out_dir   <- file.path(proj_dir, "outputs")
dir.create(out_dir, showWarnings = FALSE)

sites <- read.csv(file.path(proj_dir, "data", "site_coordinates.csv"), stringsAsFactors = FALSE)
cat("Sites with coordinates:", nrow(sites), "\n")

list_tifs <- function(d) list.files(d, pattern = "^X.*[.]tif$", full.names = TRUE)
files <- rbind(
  data.frame(file = list_tifs(file.path(trait_dir, "analysis-ready")), group = "analysis-ready"),
  data.frame(file = list_tifs(file.path(trait_dir, "experimental")), group = "experimental"),
  data.frame(file = list.files(file.path(trait_dir, "hydraulic"), pattern = "_predict[.]tif$",
                               recursive = TRUE, full.names = TRUE), group = "hydraulic")
)
files$trait_id <- ifelse(files$group == "hydraulic",
                         sub("_splot_gbif_predict[.]tif$", "", basename(files$file)),
                         sub("^(X[0-9]+)_.*", "\\1", basename(files$file)))

# Tower cell and its 3 x 3 neighbourhood (all rasters share one EASE-Grid 2.0 grid)
ref <- rast(files$file[1])
pts <- project(vect(sites, geom = c("lon", "lat"), crs = "EPSG:4326"), crs(ref))
xy_tower <- crds(pts)
tower_cell <- cellFromXY(ref, xy_tower)
tower_rc <- rowColFromCell(ref, tower_cell)

offs <- expand.grid(dcol = -1:1, drow = -1:1)
cells_df <- do.call(rbind, lapply(seq_len(nrow(sites)), function(i) {
  data.frame(SITE_ID = sites$SITE_ID[i], drow = offs$drow, dcol = offs$dcol,
             cell = cellFromRowCol(ref, tower_rc[i, 1] + offs$drow, tower_rc[i, 2] + offs$dcol),
             tower_x = xy_tower[i, 1], tower_y = xy_tower[i, 2])
}))
cxy <- xyFromCell(ref, cells_df$cell)
cells_df$is_tower_cell <- cells_df$drow == 0 & cells_df$dcol == 0
cells_df$dist_cell_centre_m <- round(sqrt((cxy[, 1] - cells_df$tower_x)^2 + (cxy[, 2] - cells_df$tower_y)^2))
cells_df$tower_x <- cells_df$tower_y <- NULL

# 3 x 3 window extent per site (cell centre of the tower cell +/- 1.5 cells).
# Values are read by cropping each window: terra 1.7.83 returns wrong values
# when many cells of these scaled Int16 multi-band rasters are read at once
# (r[cells] and extract() mix bands / scaling), single windows are correct.
tower_xy <- xyFromCell(ref, tower_cell)
win_ext <- lapply(seq_len(nrow(sites)), function(i) {
  ext(tower_xy[i, 1] - 1.5 * res(ref)[1], tower_xy[i, 1] + 1.5 * res(ref)[1],
      tower_xy[i, 2] - 1.5 * res(ref)[2], tower_xy[i, 2] + 1.5 * res(ref)[2])
})
# crop() returns cells row by row from the top: drow -1..1, dcol -1..1 within a row,
# which is the row order of cells_df (expand.grid varies dcol fastest)
read_windows <- function(r) {
  do.call(rbind, lapply(win_ext, function(e) {
    v <- values(crop(r, e, snap = "near"), mat = TRUE)
    stopifnot(nrow(v) == 9)
    v
  }))
}

long_list <- list()
trait_rows <- list()
for (i in seq_len(nrow(files))) {
  r <- rast(files$file[i])
  stopifnot(compareGeom(r, ref, stopOnError = FALSE))
  v <- read_windows(r)
  name <- sub(" \\(mean\\)$", "", names(r)[1])
  has_unc <- nlyr(r) >= 3
  long_list[[i]] <- data.frame(
    cells_df[, c("SITE_ID", "cell", "drow", "dcol", "is_tower_cell", "dist_cell_centre_m")],
    trait_id = files$trait_id[i], trait = name, group = files$group[i],
    mean = v[, 1],
    cov = if (has_unc) v[, 2] else NA_real_,
    aoa = if (has_unc) v[, 3] else NA_real_
  )
  trait_rows[[i]] <- data.frame(trait_id = files$trait_id[i], trait = name, group = files$group[i],
                                has_cov_aoa = has_unc, file = sub(paste0(trait_dir, "/"), "", files$file[i]))
  cat(sprintf("  %-15s %-14s %-45s NA tower cells: %d\n", files$trait_id[i], files$group[i], name,
              sum(is.na(v[cells_df$is_tower_cell, 1]))))
}
long <- do.call(rbind, long_list)
long <- merge(sites[, c("SITE_ID", "lat", "lon", "IGBP")], long, by = "SITE_ID")
long <- long[order(long$SITE_ID, long$group, long$trait_id, long$drow, long$dcol), ]

# Site x trait summary: tower-cell values + spatial statistics over the 3 x 3 block
summ <- do.call(rbind, lapply(split(long, list(long$SITE_ID, long$trait_id), drop = TRUE), function(d) {
  tw <- d[d$is_tower_cell, ]
  ok <- !is.na(d$mean)
  m3 <- if (any(ok)) mean(d$mean[ok]) else NA_real_
  data.frame(
    SITE_ID = d$SITE_ID[1], IGBP = d$IGBP[1], trait_id = d$trait_id[1], trait = d$trait[1], group = d$group[1],
    tower_mean = tw$mean, tower_cov = tw$cov, tower_aoa = tw$aoa,
    n_cells_valid = sum(ok),
    mean_3x3 = m3,
    sd_3x3 = if (sum(ok) > 1) sd(d$mean[ok]) else NA_real_,
    spatial_cv_3x3 = if (sum(ok) > 1 && m3 != 0) sd(d$mean[ok]) / abs(m3) else NA_real_,
    mean_cov_3x3 = if (all(is.na(d$cov))) NA_real_ else mean(d$cov[ok], na.rm = TRUE),
    max_cov_3x3 = if (all(is.na(d$cov))) NA_real_ else max(d$cov[ok], na.rm = TRUE),
    aoa_frac_3x3 = if (all(is.na(d$aoa))) NA_real_ else mean(d$aoa[ok], na.rm = TRUE)
  )
}))
summ <- summ[order(summ$SITE_ID, summ$group, summ$trait_id), ]

write.csv(long, file.path(out_dir, "traits_per_cell_long.csv"), row.names = FALSE)
write.csv(summ, file.path(out_dir, "traits_site_summary.csv"), row.names = FALSE)
write.csv(do.call(rbind, trait_rows), file.path(out_dir, "trait_list.csv"), row.names = FALSE)
cat("\nWrote", nrow(long), "cell rows and", nrow(summ), "site x trait rows to", out_dir, "\n")
