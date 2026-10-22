# 04_analysis.R
# Test: do static CWM plant traits explain cross-site variation in ecosystem
# functional properties (EFPs), in addition to and independently of climate?
#
# Data here are already cross-sectional (one EFP value per site, a multi-year
# composite from calc_EFPs_per_site_v2.ipynb) -> there is no site-year panel
# to decompose into between/within variance (the "ICC ceiling" step from the
# original recommendation doesn't apply to this dataset; noted in the report).
#
# Steps:
#  1. Drop near-duplicate trait columns (|r| > 0.95)
#  2. PCA-reduce the climate block and the trait block
#  3. vegan::varpart: partition R2(EFP) into unique-climate / unique-trait /
#     shared / residual, for each of the 5 canonical EFPs
#  4. Repeat adding IGBP (land-cover class) as a third block, to check the
#     trait signal isn't just recovering coarse biome identity
#  5. Bootstrap (999 resamples of sites) a 95% CI on the unique-trait fraction

suppressMessages({
  library(vegan)
})
set.seed(42)

proj_dir <- "/mnt/gsdata/projects/panops/panops-data-registry/data/flux/trait_biogeography_test"
df <- read.csv(file.path(proj_dir, "data", "clean_dataset.csv"), stringsAsFactors = FALSE)
cat("Loaded", nrow(df), "sites\n")

efp_vars   <- c("GPPsat", "NEPmax", "ETmax", "uWUE", "WUE")
clim_vars  <- c("TA_mean", "VPD_mean", "P_mean_annual", "SW_IN_mean", "PA_mean", "WS_mean", "aridity_index")
trait_vars <- c("LeafC", "LeafN_mass", "LeafWidth", "LeafCN_ratio", "LeafP", "StemConduitDensity",
                 "StemConduitDiameter", "LeafArea_3112", "LeafArea_3113", "LeafArea_3114", "SLA", "SSD",
                 "LeafThickness", "LeafN_area", "LeafDryMass", "RootingDepth", "LeafDelta15N",
                 "gsmax", "P12", "P50", "P88", "rdmax", "WUE_trait")

# --- 1. drop near-duplicate trait columns (|r| > 0.95) ---
tr_cor <- cor(df[trait_vars], use = "pairwise.complete.obs")
high <- which(abs(tr_cor) > 0.95 & upper.tri(tr_cor), arr.ind = TRUE)
dropped <- character(0)
if (nrow(high) > 0) {
  cat("\n-- Near-duplicate trait pairs (|r| > 0.95) --\n")
  for (i in seq_len(nrow(high))) {
    a <- rownames(tr_cor)[high[i, 1]]; b <- colnames(tr_cor)[high[i, 2]]
    cat(" ", a, "~", b, ": r =", round(tr_cor[high[i,1], high[i,2]], 3), "\n")
  }
  # keep the first of each correlated group, drop the rest
  to_drop <- unique(colnames(tr_cor)[high[, 2]])
  to_drop <- setdiff(to_drop, rownames(tr_cor)[high[,1]][!rownames(tr_cor)[high[,1]] %in% to_drop])
  dropped <- unique(colnames(tr_cor)[high[,2]])
  cat("Dropping (redundant):", paste(dropped, collapse = ", "), "\n")
}
trait_vars_kept <- setdiff(trait_vars, dropped)
cat("Traits retained for PCA:", length(trait_vars_kept), "/", length(trait_vars), "\n")

# --- 2. PCA reduce climate and trait blocks ---
pca_reduce <- function(mat, var_threshold = 0.80, max_axes = 6) {
  pca <- prcomp(mat, center = TRUE, scale. = TRUE)
  varexp <- cumsum(pca$sdev^2 / sum(pca$sdev^2))
  k <- min(which(varexp >= var_threshold), max_axes, na.rm = TRUE)
  k <- max(k, 2)
  list(scores = pca$x[, 1:k, drop = FALSE], varexp = varexp[1:k], pca = pca, k = k)
}

clim_pca  <- pca_reduce(scale(df[clim_vars]), var_threshold = 0.90, max_axes = 4)
trait_pca <- pca_reduce(scale(df[trait_vars_kept]), var_threshold = 0.80, max_axes = 6)

cat("\nClimate PCA: kept", clim_pca$k, "axes, cumulative var explained =",
    round(tail(clim_pca$varexp, 1), 3), "\n")
cat("Trait PCA: kept", trait_pca$k, "axes, cumulative var explained =",
    round(tail(trait_pca$varexp, 1), 3), "\n")

cat("\nTop climate loadings (PC1):\n")
print(round(sort(clim_pca$pca$rotation[, 1], decreasing = TRUE), 2))
cat("\nTop trait loadings (PC1):\n")
print(round(sort(trait_pca$pca$rotation[, 1], decreasing = TRUE), 2))

clim_df  <- as.data.frame(clim_pca$scores)
trait_df <- as.data.frame(trait_pca$scores)
igbp_dummy <- as.data.frame(model.matrix(~IGBP, data = df)[, -1, drop = FALSE])

# --- 3 & 4. variance partitioning per EFP: climate vs traits, then + IGBP ---
varpart_results <- list()
varpart_igbp_results <- list()

for (efp in efp_vars) {
  y <- df[[efp]]

  vp2 <- varpart(y, clim_df, trait_df)
  varpart_results[[efp]] <- vp2$part$indfract

  vp3 <- varpart(y, clim_df, trait_df, igbp_dummy)
  varpart_igbp_results[[efp]] <- vp3$part$indfract
}

cat("\n\n===== Variance partitioning: climate vs traits (adj. R2 fractions) =====\n")
for (efp in efp_vars) {
  cat("\n--", efp, "--\n")
  print(round(varpart_results[[efp]], 4))
}

cat("\n\n===== Variance partitioning: climate vs traits vs IGBP (adj. R2 fractions) =====\n")
for (efp in efp_vars) {
  cat("\n--", efp, "--\n")
  print(round(varpart_igbp_results[[efp]], 4))
}

# --- 5. bootstrap CI on the unique-trait fraction (2-block model) ---
# varpart 2-table indfract rows: [a] = X1 only (climate), [b] = X2 only (traits),
# [c] = X1+X2 shared, [d] = residual
boot_unique_trait <- function(efp, n_boot = 999) {
  y_all <- df[[efp]]
  n <- length(y_all)
  out <- numeric(n_boot)
  for (b in seq_len(n_boot)) {
    idx <- sample.int(n, n, replace = TRUE)
    vp <- tryCatch(varpart(y_all[idx], clim_df[idx, , drop = FALSE], trait_df[idx, , drop = FALSE]),
                    error = function(e) NULL)
    out[b] <- if (is.null(vp)) NA else vp$part$indfract$Adj.R.square[2]  # [b] unique to traits
  }
  out
}

cat("\n\n===== Bootstrap 95% CI: unique-to-traits adj. R2 (999 resamples) =====\n")
boot_summary <- data.frame(EFP = character(0), point_est = numeric(0),
                            ci_low = numeric(0), ci_high = numeric(0), n_valid_boot = integer(0))
for (efp in efp_vars) {
  bs <- boot_unique_trait(efp, n_boot = 999)
  bs_valid <- bs[!is.na(bs)]
  point <- varpart_results[[efp]]$Adj.R.square[2]
  ci <- quantile(bs_valid, c(0.025, 0.975))
  boot_summary <- rbind(boot_summary, data.frame(
    EFP = efp, point_est = round(point, 4),
    ci_low = round(ci[1], 4), ci_high = round(ci[2], 4), n_valid_boot = length(bs_valid)))
  cat(sprintf("  %-8s unique-trait R2 = %.4f  [95%% CI: %.4f, %.4f]  (n_valid=%d/999)\n",
              efp, point, ci[1], ci[2], length(bs_valid)))
}

# --- save everything ---
saveRDS(list(varpart_results = varpart_results, varpart_igbp_results = varpart_igbp_results,
             clim_pca = clim_pca, trait_pca = trait_pca, boot_summary = boot_summary,
             trait_vars_kept = trait_vars_kept, dropped_traits = dropped),
        file.path(proj_dir, "results", "analysis_results.rds"))

write.csv(boot_summary, file.path(proj_dir, "results", "bootstrap_unique_trait_R2.csv"), row.names = FALSE)

vp2_table <- do.call(rbind, lapply(names(varpart_results), function(e) {
  d <- varpart_results[[e]]; d$block <- rownames(d); d$EFP <- e; d
}))
write.csv(vp2_table, file.path(proj_dir, "results", "varpart_climate_vs_traits.csv"), row.names = FALSE)

vp3_table <- do.call(rbind, lapply(names(varpart_igbp_results), function(e) {
  d <- varpart_igbp_results[[e]]; d$block <- rownames(d); d$EFP <- e; d
}))
write.csv(vp3_table, file.path(proj_dir, "results", "varpart_climate_traits_igbp.csv"), row.names = FALSE)

cat("\n\nSaved results to results/ (analysis_results.rds, bootstrap_unique_trait_R2.csv,",
    "varpart_climate_vs_traits.csv, varpart_climate_traits_igbp.csv)\n")
