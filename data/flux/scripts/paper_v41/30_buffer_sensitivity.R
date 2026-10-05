#!/usr/bin/env Rscript
# ============================================================================
# How much do tree cover and the mortality metrics change with the buffer size (100-500 m)?
# Site-years = the paper dataset (PAPER_RESP_SET, default efp: 104 sites / 381 site-years after the
# 2026-10-02 QC); values = current-year deadtrees v4.1 metrics from mortality_v41_multibuffer.csv.
#   buffer_values.png     per metric: one thin line per site (site mean over its years) across buffers,
#                         median and interquartile range of the sites on top
#   buffer_agreement.png  per metric: Spearman correlation between buffers over site-years (5 x 5)
#   buffer_summary.csv    per metric and buffer: median, IQR, share of zeros, correlation and
#                         median absolute difference to the 500 m value (the buffer the figures use)
# Output: plots/<set>/buffer_sensitivity/
#   Rscript 30_buffer_sensitivity.R
# ============================================================================
suppressMessages({library(data.table); library(ggplot2)})
setwd("/mnt/gsdata/projects/panops/panops-data-registry/data/flux")
source("scripts/paper_v41/paper_common.R")
OUT <- file.path(PAPER_FIG, "buffer_sensitivity"); dir.create(OUT, showWarnings = FALSE, recursive = TRUE)
PN <- paper_n()
BUFS <- c(100, 200, 300, 400, 500)
METRICS <- c(tree_cover_mean_pct = "Tree cover (%)",
             absolute_mortality = "Absolute deadwood (% of area)",
             relative_mortality = "Relative standing deadwood (% of tree cover)",
             relative_disturbance = "Total mortality (%)",
             new_deadwood_gain_pp = "New deadwood gain (pp)",
             relative_tree_loss_pct = "Relative tree loss (%)")
BUF_COLS <- setNames(c("#B2DFDB", "#66B2A9", "#2E8C83", "#0F6660", "#004D4D"), paste0(BUFS, " m"))  # one hue, light -> dark
INK <- "#111111"; MUTED <- "#444444"; GRID <- "#D9D9D9"; ACCENT <- "#004D4D"

keys <- fread(PAPER_DATA, select = c("SITE_ID", "YEAR"))
mort <- fread(file.path(PAPER_ROOT, "derived_tables/paper_v41/mortality_v41_multibuffer.csv"))
cols <- as.vector(outer(names(METRICS), paste0("_", BUFS, "m"), paste0))
m <- merge(keys, mort[, c("SITE_ID", "YEAR", cols), with = FALSE], by = c("SITE_ID", "YEAR"))
stopifnot(nrow(m) == nrow(keys))
long <- melt(m, id.vars = c("SITE_ID", "YEAR"), variable.name = "col", value.name = "value")
long[, buffer := as.integer(sub(".*_([0-9]+)m$", "\\1", col))]
long[, metric := factor(METRICS[sub("_[0-9]+m$", "", col)], levels = METRICS)]

th <- theme_bw(base_size = 11) + theme(
  panel.border = element_rect(colour = GRID, fill = NA, linewidth = 0.4),
  panel.grid.minor = element_blank(), panel.grid.major = element_line(colour = GRID, linewidth = 0.2),
  strip.background = element_rect(fill = "#EFEFEF", colour = GRID),
  strip.text = element_text(colour = INK, size = 10, face = "bold"),
  axis.text = element_text(colour = MUTED, size = 9), axis.title = element_text(colour = MUTED, size = 10),
  plot.title = element_text(colour = INK, size = 14, face = "bold"),
  plot.subtitle = element_text(colour = MUTED, size = 9))
sub_txt <- sprintf("%d sites / %d site-years of the paper dataset (tree cover >= 30 %%) · %s · current-year values",
                   PN[["sites"]], PN[["sy"]], PAPER_MAPS)

# ---- 1. values across buffers ---------------------------------------------------
site <- long[, .(value = mean(value, na.rm = TRUE)), by = .(SITE_ID, metric, buffer)]
band <- site[, .(med = median(value, na.rm = TRUE), q25 = quantile(value, 0.25, na.rm = TRUE),
                 q75 = quantile(value, 0.75, na.rm = TRUE)), by = .(metric, buffer)]
p1 <- ggplot() +
  geom_line(data = site, aes(buffer, value, group = SITE_ID), colour = "#9AA0A6", alpha = 0.35, linewidth = 0.3) +
  geom_ribbon(data = band, aes(buffer, ymin = q25, ymax = q75), fill = ACCENT, alpha = 0.18) +
  geom_line(data = band, aes(buffer, med), colour = ACCENT, linewidth = 1.1) +
  geom_point(data = band, aes(buffer, med), colour = ACCENT, size = 2.4) +
  facet_wrap(~ metric, scales = "free_y", ncol = 3) +
  scale_x_continuous(breaks = BUFS, labels = paste0(BUFS, " m")) +
  labs(x = "Buffer radius around the tower", y = NULL,
       title = "Tree cover and mortality metrics across buffer sizes",
       subtitle = paste0(sub_txt, "\nGrey = one site (mean over its years) · line + band = median and interquartile range over sites")) +
  th
ggsave(file.path(OUT, "buffer_values.png"), p1, width = 13, height = 7.5, dpi = 200, bg = "white")
ggsave(file.path(OUT, "buffer_values.pdf"), p1, width = 13, height = 7.5, bg = "white", device = cairo_pdf)

# ---- 2. agreement between buffers (Spearman, site-years) ------------------------
cor_dt <- rbindlist(lapply(names(METRICS), function(k) {
  X <- as.matrix(m[, paste0(k, "_", BUFS, "m"), with = FALSE]); colnames(X) <- paste0(BUFS, " m")
  C <- suppressWarnings(cor(X, method = "spearman", use = "pairwise.complete.obs"))
  d <- as.data.table(as.table(C)); setnames(d, c("b1", "b2", "rho")); d[, metric := METRICS[[k]]]; d
}))
cor_dt[, metric := factor(metric, levels = METRICS)]
cor_dt[, `:=`(b1 = factor(b1, levels = paste0(BUFS, " m")), b2 = factor(b2, levels = rev(paste0(BUFS, " m"))))]
lo <- floor(min(cor_dt$rho, na.rm = TRUE) * 10) / 10
p2 <- ggplot(cor_dt, aes(b1, b2, fill = rho)) +
  geom_tile(colour = "white", linewidth = 0.8) +
  geom_text(aes(label = sprintf("%.2f", rho), colour = rho > (lo + 1) / 2), size = 3) +
  scale_colour_manual(values = c(`TRUE` = "white", `FALSE` = INK), guide = "none") +
  scale_fill_gradient(low = "#E6F2F1", high = ACCENT, limits = c(lo, 1), name = "Spearman ρ") +
  facet_wrap(~ metric, ncol = 3) + coord_equal() +
  labs(x = NULL, y = NULL, title = "Agreement between buffer sizes",
       subtitle = paste0(sub_txt, "\nSpearman correlation over site-years; 1 = the buffers rank the site-years identically")) +
  th + theme(panel.grid = element_blank())
ggsave(file.path(OUT, "buffer_agreement.png"), p2, width = 12, height = 8.5, dpi = 200, bg = "white")
ggsave(file.path(OUT, "buffer_agreement.pdf"), p2, width = 12, height = 8.5, bg = "white", device = cairo_pdf)

# ---- 3. summary table -------------------------------------------------------------
ref <- long[buffer == 500, .(SITE_ID, YEAR, metric, v500 = value)]
s <- merge(long, ref, by = c("SITE_ID", "YEAR", "metric"))
summ <- s[, .(median = median(value, na.rm = TRUE), q25 = quantile(value, 0.25, na.rm = TRUE),
              q75 = quantile(value, 0.75, na.rm = TRUE), pct_zero = round(100 * mean(value == 0, na.rm = TRUE), 1),
              rho_vs_500m = round(suppressWarnings(cor(value, v500, method = "spearman", use = "complete.obs")), 3),
              median_abs_diff_vs_500m = median(abs(value - v500), na.rm = TRUE)), by = .(metric, buffer)]
setorder(summ, metric, buffer)
fwrite(summ, file.path(OUT, "buffer_summary.csv"))
print(summ)
cat("BUFFER_FIGS_DONE\n")
