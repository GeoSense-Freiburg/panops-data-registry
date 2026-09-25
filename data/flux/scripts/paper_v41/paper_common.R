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
PAPER_DATA <- file.path(PAPER_ROOT, "derived_tables/paper_v41/paper_dataset_full_lags_monthly_tc30_minrun2_buf100-200-300-400-500.csv")
PAPER_OUT  <- Sys.getenv("PAPER_OUT_DIR", file.path(PAPER_ROOT, "derived_tables/paper_v41/models"))
PAPER_FIG  <- Sys.getenv("PAPER_FIG_DIR", file.path(PAPER_ROOT, "plots/paper_v41"))
PAPER_TUNE <- Sys.getenv("PAPER_TUNE_DIR", file.path(PAPER_ROOT, "plots/paper_v41/Optuna"))
EFP_ORDER  <- c("GPPsat", "NEPmax", "ETmax", "WUE")
WINDOWS    <- c("12m", "24m")
TRAITS <- c("LeafC", "LeafN_mass", "LeafWidth", "LeafCN_ratio", "LeafP", "StemConduitDensity",
            "StemConduitDiameter", "LeafArea_3112", "LeafArea_3113", "LeafArea_3114", "SLA", "SSD",
            "LeafThickness", "LeafN_area", "LeafDryMass", "RootingDepth", "LeafDelta15N",
            "gsmax", "P12", "P50", "P88", "rdmax", "WUE_trait")
MORT_RE <- "^(absolute_mortality|relative_mortality|new_deadwood_gain_pp(_thresh)?|relative_tree_loss_pct(_thresh)?|relative_disturbance)_[0-9]+m"

paper_load <- function() as.data.frame(fread(PAPER_DATA))

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
