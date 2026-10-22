# 05_plots.R
# Diagnostic figures: global coverage, EFP x IGBP, trait x IGBP, PCA score
# plots, and a visual summary of the variance-partitioning results.

suppressMessages({
  library(ggplot2)
  library(rnaturalearth)
  library(sf)
  library(RColorBrewer)
})

proj_dir  <- "/mnt/gsdata/projects/panops/panops-data-registry/data/flux/trait_biogeography_test"
plots_dir <- file.path(proj_dir, "plots")

full  <- read.csv(file.path(proj_dir, "data", "merged_dataset_full.csv"), stringsAsFactors = FALSE)
clean <- read.csv(file.path(proj_dir, "data", "clean_dataset.csv"), stringsAsFactors = FALSE)
res   <- readRDS(file.path(proj_dir, "results", "analysis_results.rds"))

igbp_order <- c("ENF", "DBF", "DNF", "EBF", "MF", "CSH", "OSH", "WSA", "SAV", "GRA", "WET")
igbp_pal   <- setNames(colorRampPalette(brewer.pal(11, "Paired"))(length(igbp_order)), igbp_order)
full$IGBP  <- factor(full$IGBP, levels = igbp_order)
clean$IGBP <- factor(clean$IGBP, levels = igbp_order)

theme_set(theme_bw(base_size = 11) + theme(panel.grid.minor = element_blank()))

# ---------------------------------------------------------------------------
# 1. Global data coverage map
# ---------------------------------------------------------------------------
world <- ne_countries(scale = "medium", returnclass = "sf")

full$status <- ifelse(full$SITE_ID %in% clean$SITE_ID, "complete case (n=413)", "dropped (missing trait/climate, n=104)")

p1 <- ggplot() +
  geom_sf(data = world, fill = "grey95", color = "grey80", linewidth = 0.2) +
  geom_point(data = full, aes(x = lon, y = lat, color = IGBP, shape = status), size = 1.8, alpha = 0.85) +
  scale_color_manual(values = igbp_pal, drop = FALSE) +
  scale_shape_manual(values = c("complete case (n=413)" = 16, "dropped (missing trait/climate, n=104)" = 4)) +
  coord_sf(crs = st_crs(world)) +
  labs(title = "Global coverage: 517 candidate sites -> 413 complete-case",
       x = NULL, y = NULL, color = "IGBP", shape = NULL) +
  theme(legend.position = "right")

ggsave(file.path(plots_dir, "01_global_coverage_map.png"), p1, width = 11, height = 6, dpi = 200)
cat("Saved 01_global_coverage_map.png\n")

# ---------------------------------------------------------------------------
# 2. EFP distributions across IGBP
# ---------------------------------------------------------------------------
efp_vars <- c("GPPsat", "NEPmax", "ETmax", "uWUE", "WUE")
efp_long <- do.call(rbind, lapply(efp_vars, function(v) {
  data.frame(IGBP = clean$IGBP, EFP = v, value = clean[[v]])
}))
efp_long$EFP <- factor(efp_long$EFP, levels = efp_vars)

n_per_igbp <- table(clean$IGBP)
p2 <- ggplot(efp_long, aes(x = IGBP, y = value, fill = IGBP)) +
  geom_boxplot(outlier.size = 0.6, linewidth = 0.3) +
  facet_wrap(~EFP, scales = "free_y", ncol = 2) +
  scale_fill_manual(values = igbp_pal, drop = FALSE) +
  labs(title = "EFP distribution by IGBP class (n=413 sites)",
       subtitle = paste0("n per class: ", paste(names(n_per_igbp), n_per_igbp, sep = "=", collapse = ", ")),
       x = NULL, y = NULL) +
  theme(legend.position = "none", axis.text.x = element_text(angle = 45, hjust = 1))

ggsave(file.path(plots_dir, "02_EFP_by_IGBP.png"), p2, width = 10, height = 10, dpi = 200)
cat("Saved 02_EFP_by_IGBP.png\n")

# ---------------------------------------------------------------------------
# 3. Trait distributions across IGBP (all 23 traits, full grid)
# ---------------------------------------------------------------------------
trait_vars <- res$trait_vars_kept
trait_long <- do.call(rbind, lapply(trait_vars, function(v) {
  data.frame(IGBP = clean$IGBP, trait = v, value = clean[[v]])
}))
trait_long$trait <- factor(trait_long$trait, levels = trait_vars)

p3 <- ggplot(trait_long, aes(x = IGBP, y = value, fill = IGBP)) +
  geom_boxplot(outlier.size = 0.4, linewidth = 0.25) +
  facet_wrap(~trait, scales = "free_y", ncol = 4) +
  scale_fill_manual(values = igbp_pal, drop = FALSE) +
  labs(title = "All 23 CWM trait distributions by IGBP class (n=413 sites)", x = NULL, y = NULL) +
  theme(legend.position = "none", axis.text.x = element_text(angle = 45, hjust = 1, size = 6),
        strip.text = element_text(size = 8))

ggsave(file.path(plots_dir, "03_all_traits_by_IGBP.png"), p3, width = 16, height = 18, dpi = 200)
cat("Saved 03_all_traits_by_IGBP.png\n")

# Key subset (leaf-economics + hydraulic highlights), easier to read
key_traits <- c("SLA", "SSD", "LeafN_mass", "LeafThickness", "LeafCN_ratio", "P50", "gsmax", "RootingDepth")
p3b <- ggplot(subset(trait_long, trait %in% key_traits), aes(x = IGBP, y = value, fill = IGBP)) +
  geom_boxplot(outlier.size = 0.6, linewidth = 0.3) +
  facet_wrap(~trait, scales = "free_y", ncol = 2) +
  scale_fill_manual(values = igbp_pal, drop = FALSE) +
  labs(title = "Key trait distributions by IGBP class", x = NULL, y = NULL) +
  theme(legend.position = "none", axis.text.x = element_text(angle = 45, hjust = 1))

ggsave(file.path(plots_dir, "03b_key_traits_by_IGBP.png"), p3b, width = 10, height = 12, dpi = 200)
cat("Saved 03b_key_traits_by_IGBP.png\n")

# ---------------------------------------------------------------------------
# 4. PCA score plots (climate PC1/PC2, trait PC1/PC2), colored by IGBP
# ---------------------------------------------------------------------------
clim_scores  <- as.data.frame(res$clim_pca$scores[, 1:2])
trait_scores <- as.data.frame(res$trait_pca$scores[, 1:2])
names(clim_scores)  <- c("PC1", "PC2")
names(trait_scores) <- c("PC1", "PC2")
clim_scores$IGBP  <- clean$IGBP
trait_scores$IGBP <- clean$IGBP

clim_var  <- res$clim_pca$pca$sdev^2 / sum(res$clim_pca$pca$sdev^2)
trait_var <- res$trait_pca$pca$sdev^2 / sum(res$trait_pca$pca$sdev^2)

p4a <- ggplot(clim_scores, aes(PC1, PC2, color = IGBP)) +
  geom_point(size = 1.6, alpha = 0.8) +
  scale_color_manual(values = igbp_pal, drop = FALSE) +
  stat_ellipse(aes(group = IGBP), linewidth = 0.3, alpha = 0.5) +
  labs(title = "Climate PC space by IGBP",
       x = sprintf("PC1 (%.0f%%) [warm/dry/sunny <-> cool/wet]", 100 * clim_var[1]),
       y = sprintf("PC2 (%.0f%%)", 100 * clim_var[2]))

p4b <- ggplot(trait_scores, aes(PC1, PC2, color = IGBP)) +
  geom_point(size = 1.6, alpha = 0.8) +
  scale_color_manual(values = igbp_pal, drop = FALSE) +
  stat_ellipse(aes(group = IGBP), linewidth = 0.3, alpha = 0.5) +
  labs(title = "Trait PC space by IGBP",
       x = sprintf("PC1 (%.0f%%) [acquisitive <-> conservative LES]", 100 * trait_var[1]),
       y = sprintf("PC2 (%.0f%%)", 100 * trait_var[2]))

library(patchwork)
p4 <- p4a + p4b + plot_layout(guides = "collect")
ggsave(file.path(plots_dir, "04_PCA_scores_by_IGBP.png"), p4, width = 13, height = 6, dpi = 200)
cat("Saved 04_PCA_scores_by_IGBP.png\n")

# ---------------------------------------------------------------------------
# 5. Variance partitioning summary bar charts
# ---------------------------------------------------------------------------
vp2 <- read.csv(file.path(proj_dir, "results", "varpart_climate_vs_traits.csv"))
vp2_plot <- subset(vp2, block %in% c("[a] = X1|X2", "[b] = X2|X1", "[c]", "[d] = Residuals"))
vp2_plot$component <- factor(vp2_plot$block,
  levels = c("[a] = X1|X2", "[b] = X2|X1", "[c]", "[d] = Residuals"),
  labels = c("unique climate", "unique traits", "shared", "residual"))
vp2_plot$EFP <- factor(vp2_plot$EFP, levels = efp_vars)

p5a <- ggplot(vp2_plot, aes(x = EFP, y = Adj.R.squared, fill = component)) +
  geom_col(position = "stack") +
  scale_fill_manual(values = c("unique climate" = "#1b9e77", "unique traits" = "#d95f02",
                                "shared" = "#7570b3", "residual" = "grey85")) +
  labs(title = "Variance partitioning: climate vs. traits", x = NULL, y = "adjusted R2", fill = NULL) +
  geom_hline(yintercept = 0, linewidth = 0.3)

vp3 <- read.csv(file.path(proj_dir, "results", "varpart_climate_traits_igbp.csv"))
vp3_plot <- subset(vp3, block %in% c("[a] = X1 | X2+X3", "[b] = X2 | X1+X3", "[c] = X3 | X1+X2", "[h] = Residuals"))
vp3_plot$component <- factor(vp3_plot$block,
  levels = c("[a] = X1 | X2+X3", "[b] = X2 | X1+X3", "[c] = X3 | X1+X2", "[h] = Residuals"),
  labels = c("unique climate", "unique traits", "unique IGBP", "residual (+ overlaps)"))
vp3_plot$EFP <- factor(vp3_plot$EFP, levels = efp_vars)

p5b <- ggplot(vp3_plot, aes(x = EFP, y = Adj.R.square, fill = component)) +
  geom_col(position = "stack") +
  scale_fill_manual(values = c("unique climate" = "#1b9e77", "unique traits" = "#d95f02",
                                "unique IGBP" = "#e7298a", "residual (+ overlaps)" = "grey85")) +
  labs(title = "Variance partitioning: climate vs. traits vs. IGBP (unique fractions only)",
       x = NULL, y = "adjusted R2", fill = NULL) +
  geom_hline(yintercept = 0, linewidth = 0.3)

p5 <- p5a / p5b
ggsave(file.path(plots_dir, "05_variance_partitioning_summary.png"), p5, width = 9, height = 10, dpi = 200)
cat("Saved 05_variance_partitioning_summary.png\n")

cat("\nAll plots written to", plots_dir, "\n")
