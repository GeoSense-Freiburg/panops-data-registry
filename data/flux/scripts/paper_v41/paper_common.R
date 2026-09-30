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
stopifnot(PAPER_RESP_SET %in% c("efp", "flux", "v10flux", "v10efp"))
if (PAPER_RESP_SET %in% c("efp", "v10efp")) {
  PAPER_DATA <- file.path(PAPER_ROOT, if (PAPER_RESP_SET == "efp")
    "derived_tables/paper_v41/paper_dataset_full_lags_monthly_tc30_minrun2_buf100-200-300-400-500.csv" else
    "derived_tables/paper_v10_efp/paper_dataset_v10_efp.csv")
  .sub <- if (PAPER_RESP_SET == "efp") "paper_v41" else "paper_v10_efp"
  EFP_ORDER  <- c("GPPsat", "NEPmax", "ETmax", "WUE")
  RESP_UNITS <- c(GPPsat = "GPPsat  (µmol m⁻² s⁻¹)", NEPmax = "NEPmax  (µmol m⁻² s⁻¹)",
                  ETmax  = "ETmax  (mm d⁻¹)",         WUE    = "WUE  (g C mm⁻¹)")
  RESP_EXPR  <- c(GPPsat = expression(GPP[sat]~~(mu*mol~m^{-2}~s^{-1})),
                  NEPmax = expression(NEP[max]~~(mu*mol~m^{-2}~s^{-1})),
                  ETmax  = expression(ET[max]~~(mm~d^{-1})),
                  WUE    = expression(WUE~~(g~C~mm^{-1})))
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
PAPER_MAPS <- if (PAPER_RESP_SET %in% c("v10flux", "v10efp")) "deadtrees v2-2" else "deadtrees v4.1"
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
}

paper_load <- function() as.data.frame(fread(PAPER_DATA))
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
