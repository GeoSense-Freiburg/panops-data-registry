# ============================================================================
# Shared definitions for the paper_v41 models, SHAP and figures (source this).
# Dataset: version A (lag 1 + lag 2 for every mortality metric and climate), 100-500 m,
# runs of >= 2 continuous EFP years, tree cover >= 30 %, traits complete -> 106 sites / 388
# site-years (memory: paper_dataset_v41). The same rows are used for both windows.
# Models (paper): M3 = climate + traits, M4 = climate + traits + mortality (disturbance).
# Windows: 12m = climate lag1 (12 pre-season months) + mortality current and lag1;
#          24m = climate lag1 + lag2 (24 pre-season months) + mortality current, lag1, lag2.
# ============================================================================
suppressMessages(library(data.table))
PAPER_ROOT <- "/mnt/gsdata/projects/panops/panops-data-registry/data/flux"
# Dataset / response set (env PAPER_RESP_SET):
#   "efp"      (default) paper_v41: deadtrees v4.1, 106 sites / 388 site-years, the four EFPs
#   "flux"     same rows, growing-season flux totals instead (scripts/paper_v41_fluxes/)
#   "v10flux"  V10 tc>=30 predictors (deadtrees v2-2, V10 traits and mortality block, 93 sites) with
#              the same half-hourly flux totals (scripts/paper_v41_fluxes/32_build_v10_flux_dataset.R)
#   "v10efp"   V10 tc>=30 EFP models (93 sites / 395 site-years), figures only: reads the V10 Optuna
#              true_24m signed SHAP (28_v10_efp_inputs.R copies it and builds the site table)
# EFP_ORDER keeps its name in every mode so each script reads the active responses from here;
# RESP_UNITS = text labels (Figs 2-4), RESP_EXPR = plotmath labels (Fig 1), RESP_WORD = noun in captions,
# PAPER_MAPS = mortality map product named in figure subtitles.
PAPER_RESP_SET <- Sys.getenv("PAPER_RESP_SET", "efp")
#   "efp_v5"   same EFP design built from deadtrees v5 maps (2026-10-06), outputs in paper_v5 - for the v4.1 vs v5 comparison
stopifnot(PAPER_RESP_SET %in% c("efp", "flux", "v10flux", "v10efp", "efp_v5"))
if (PAPER_RESP_SET %in% c("efp", "v10efp", "efp_v5")) {
  PAPER_DATA <- file.path(PAPER_ROOT, switch(PAPER_RESP_SET,
    efp    = "derived_tables/paper_v41/paper_dataset_full_lags_monthly_tc30_minrun2_buf100-200-300-400-500.csv",
    efp_v5 = "derived_tables/paper_v5/paper_dataset_full_lags_monthly_tc30_minrun2_buf100-200-300-400-500.csv",
    v10efp = "derived_tables/paper_v10_efp/paper_dataset_v10_efp.csv"))
  .sub <- switch(PAPER_RESP_SET, efp = "paper_v41", efp_v5 = "paper_v5", v10efp = "paper_v10_efp")
  EFP_ORDER  <- c("GPPsat", "NEPmax", "ETmax", "WUE")
  RESP_UNITS <- c(GPPsat = "GPPsat  (µmol m⁻² s⁻¹)", NEPmax = "NEPmax  (µmol m⁻² s⁻¹)",
                  ETmax  = "ETmax  (mm (30 min)⁻¹)",   WUE    = "WUE  (g C kg⁻¹ H₂O)")
  RESP_EXPR  <- c(GPPsat = expression(GPP[sat]~~(mu*mol~m^{-2}~s^{-1})),
                  NEPmax = expression(NEP[max]~~(mu*mol~m^{-2}~s^{-1})),
                  ETmax  = expression(ET[max]~~(mm~(30~min)^{-1})),
                  WUE    = expression(WUE~~(g~C~kg^{-1}~H[2]*O)))
  RESP_WORD  <- "EFP"
} else {
  PAPER_DATA <- file.path(PAPER_ROOT, if (PAPER_RESP_SET == "flux") "derived_tables/paper_v41_fluxes/paper_dataset_fluxes.csv"
                                      else "derived_tables/paper_v10_fluxes/paper_dataset_v10_fluxes.csv")
  .sub <- if (PAPER_RESP_SET == "flux") "paper_v41_fluxes" else "paper_v10_fluxes"
  EFP_ORDER  <- c("GPP_gs", "NEP_gs", "ET_gs", "WUE_gs")
  RESP_UNITS <- c(GPP_gs = "GPP  (g C m⁻² season⁻¹)", NEP_gs = "NEP  (g C m⁻² season⁻¹)",
                  ET_gs  = "ET  (mm season⁻¹)",       WUE_gs = "WUE  (g C mm⁻¹)")
  RESP_EXPR  <- c(GPP_gs = expression(GPP~~(g~C~m^{-2}~season^{-1})),
                  NEP_gs = expression(NEP~~(g~C~m^{-2}~season^{-1})),
                  ET_gs  = expression(ET~~(mm~season^{-1})),
                  WUE_gs = expression(WUE~~(g~C~mm^{-1})))
  RESP_WORD  <- "flux"
}
PAPER_MAPS <- if (PAPER_RESP_SET %in% c("v10flux", "v10efp")) "deadtrees v2-2" else if (PAPER_RESP_SET == "efp_v5") "deadtrees v5" else "deadtrees v4.1"
# Variant without the thresholded mortality metrics (env PAPER_NO_THRESH=1, user 2026-10-06): drops
# new_deadwood_gain_pp_thresh and relative_tree_loss_pct_thresh from the mortality block; outputs go to <sub>_nothresh.
PAPER_NO_THRESH <- Sys.getenv("PAPER_NO_THRESH", "0") == "1"
if (PAPER_NO_THRESH) .sub <- paste0(.sub, "_nothresh")
PAPER_OUT  <- Sys.getenv("PAPER_OUT_DIR", file.path(PAPER_ROOT, "derived_tables", .sub, "models"))
PAPER_FIG  <- Sys.getenv("PAPER_FIG_DIR", file.path(PAPER_ROOT, "plots", .sub))
PAPER_TUNE <- Sys.getenv("PAPER_TUNE_DIR", file.path(PAPER_ROOT, "plots", .sub, "Optuna"))
WINDOWS    <- c("12m", "24m")
if (PAPER_RESP_SET %in% c("v10flux", "v10efp")) {
  # V10 predictor definitions (V10 get_predictor_cols(), as in predict_fluxes/): 21 traits; mortality block =
  # 11 families (tree_cover_mean_pct, live_tree_cover_pct and tree_loss_pp are not predictors there);
  # mortality_loss_severity_pct exists only at lag1, lag2 only for absolute and relative mortality.
  TRAITS <- c("P12_mean", "P50_mean", "P88_mean", "gsmax_mean", "rdmax_mean", "SSD", "SLA", "Leaf C",
              "Leaf C/N ratio", "Leaf N (area)", "Leaf N (mass)", "Leaf P", "Leaf area", "Leaf area (3112)",
              "Leaf area (3114)", "Leaf delta 15N", "Leaf dry mass", "Leaf thickness", "Leaf width",
              "Stem conduit density", "Stem conduit diameter")
  MORT_RE <- "^(absolute_mortality|relative_mortality|new_deadwood_gain_pp(_thresh)?|new_mortality_rate_pct(_thresh)?|relative_tree_loss_pct(_thresh)?|relative_disturbance|mortality_loss_severity_pct(_thresh)?)_[0-9]+m"
} else {
  TRAITS <- c("LeafC", "LeafN_mass", "LeafWidth", "LeafCN_ratio", "LeafP", "StemConduitDensity",
              "StemConduitDiameter", "LeafArea_3112", "LeafArea_3113", "LeafArea_3114", "SLA", "SSD",
              "LeafThickness", "LeafN_area", "LeafDryMass", "RootingDepth", "LeafDelta15N",
              "gsmax", "P12", "P50", "P88", "rdmax", "WUE_trait")
  MORT_RE <- "^(absolute_mortality|relative_mortality|new_deadwood_gain_pp(_thresh)?|relative_tree_loss_pct(_thresh)?|relative_disturbance)_[0-9]+m"
  if (PAPER_NO_THRESH) MORT_RE <- "^(absolute_mortality|relative_mortality|new_deadwood_gain_pp|relative_tree_loss_pct|relative_disturbance)_[0-9]+m"
}

paper_load <- function() as.data.frame(fread(PAPER_DATA))

# Buffer that describes a SITE's disturbance in the figures (Low / Mid / High classes, high-disturbance
# subsets, colour scales) - NOT the model predictors, which always carry all buffers.
# env PAPER_CLASS_BUFFER: "avg" = per site-year mean over the five nested buffers 100-500 m, which weights
# the area nearest the tower most (it lies inside all five), like a flux footprint (user, 2026-10-05;
# default for the v4.1 sets) | "100" ... "500" = one buffer (default "500" for the V10 sets, as before).
# paper_class_data() returns SITE_ID, YEAR, `extra` and <metric>_cls for the three class metrics.
CLASS_BUF <- Sys.getenv("PAPER_CLASS_BUFFER", if (PAPER_RESP_SET %in% c("efp", "flux", "efp_v5")) "avg" else "500")
CLASS_LAB <- if (CLASS_BUF == "avg") "mean of 100-500 m buffers" else paste0(CLASS_BUF, " m")
CLASS_TAG <- if (CLASS_BUF == "avg") "" else paste0("_cls", CLASS_BUF, "m")   # file-name tag; none for the default
# Figure FILE names use the paper's metric names (user, 2026-10-05); the short keys inside the scripts stay:
#   absolute_mortality = absolute deadwood, relative_mortality = relative deadwood, relative_disturbance = relative mortality
FILE_KEY <- c(abs_mort = "abs_deadwood", rel_mort = "rel_deadwood", rel_dist = "total_mort",
              relmort = "rel_deadwood", reldist = "total_mort", high_relmort = "high_rel_deadwood", high_reldist = "high_total_mort")
CLASS_METRICS <- c("absolute_mortality", "relative_mortality", "relative_disturbance")
paper_class_data <- function(extra = character(0)) {
  bufs <- if (CLASS_BUF == "avg") c(100, 200, 300, 400, 500) else as.integer(CLASS_BUF)
  need <- as.vector(outer(CLASS_METRICS, paste0("_", bufs, "m"), paste0))
  d <- fread(PAPER_DATA, select = unique(c("SITE_ID", "YEAR", extra, intersect(need, names(fread(PAPER_DATA, nrows = 0))))))
  for (m in CLASS_METRICS) {
    cs <- paste0(m, "_", bufs, "m")
    if (all(cs %in% names(d))) d[, paste0(m, "_cls") := rowMeans(.SD), .SDcols = cs]
  }
  d
}
# sites and site-years of the active dataset, for figure labels
paper_n <- function() { d <- fread(PAPER_DATA, select = "SITE_ID"); c(sites = uniqueN(d$SITE_ID), sy = nrow(d)) }

paper_cols <- function(df, window, spec) {
  ac <- colnames(df)
  clim <- if (window == "12m") grep("^lag1_M[0-9]{2}_", ac, value = TRUE) else grep("^lag[12]_M[0-9]{2}_", ac, value = TRUE)
  mort <- grep(paste0(MORT_RE, "$"), ac, value = TRUE)                               # current year
  mort <- c(mort, grep(paste0(MORT_RE, "_lag1$"), ac, value = TRUE))
  if (window == "24m") mort <- c(mort, grep(paste0(MORT_RE, "_lag2$"), ac, value = TRUE))
  tr <- intersect(TRAITS, ac)
  stopifnot(length(tr) == length(TRAITS))
  out <- c(clim, tr)
  if (spec == "C+T+D") out <- c(out, mort)
  out
}

paper_group <- function(v) {
  ifelse(grepl("^lag[12]_M[0-9]{2}_", v), "Climate",
  ifelse(v %in% TRAITS, "Traits",
  ifelse(grepl(MORT_RE, v), "Disturbance", "Other")))
}
