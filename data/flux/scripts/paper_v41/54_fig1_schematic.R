#!/usr/bin/env Rscript
# ============================================================================
# Fig. 1 test version (2026-10-21, meeting with Teja): minimal conceptual / workflow schematic in Nature style instead
# of the world map. Left to right: data (FLUXNET towers + small site map, tree mortality and tree cover around the tower,
# plant traits, pre-season climate) -> ecosystem functional properties and two models (C+T, C+T+M; AutoGluon,
# leave-one-site-out) -> what is compared (error, bias, direction of the mortality effect).
# Icons are drawn with ggplot2 primitives (no external images), colours from fig_style.R.
# Output: plots/paper_v5_nothresh/nature/AG_24m/fig1_schematic.pdf/.png
# ============================================================================
suppressMessages({library(data.table); library(ggplot2)})
ROOT <- Sys.getenv("PAPER_ROOT", "/mnt/gsdata/projects/panops/panops-data-registry/data/flux"); setwd(ROOT)
source("scripts/paper_v41/fig_style.R")
OUT <- "plots/paper_v5_nothresh/nature/AG_24m"
TXT <- 6 / .pt; TXS <- 5.2 / .pt                                        # 6 pt labels, 5.2 pt small print
GREEN <- "#4D9221"; GREEN_L <- "#B8E186"; DEAD <- "#8C6D5A"; PINK <- "#C51B7D"; PINK_L <- "#F1B6DA"
GREY <- "#7F7F7F"; LINE <- "#4D4D4D"; SKY <- "#56B4E9"; SUN <- "#E69F00"
circle <- function(x0, y0, r, n = 60) { a <- seq(0, 2 * pi, length.out = n); data.table(x = x0 + r * cos(a), y = y0 + r * sin(a)) }

# ---- icon builders (coordinates in a 0-100 x 0-50 canvas) -----------------------------------------------------------
tower <- function(x0, y0, h = 9) {                                      # lattice tower with instrument arm
  w0 <- 1.1; w1 <- 0.45; z <- seq(0, h, length.out = 6)
  legs <- data.table(x = c(x0 - w0, x0 - w1, x0 + w0, x0 + w1), y = c(y0, y0 + h, y0, y0 + h), g = c(1, 1, 2, 2))
  wl <- function(t) w0 + (w1 - w0) * t / h
  br <- rbindlist(lapply(seq_len(length(z) - 1), function(i) data.table(
    x = c(x0 - wl(z[i]), x0 + wl(z[i + 1])), y = y0 + c(z[i], z[i + 1]), g = 10 + i)))
  arm <- data.table(x = c(x0 + w1, x0 + w1 + 2.2), y = y0 + h - 0.3, g = 99)
  list(lines = rbind(legs, br, arm), sonic = data.table(x = x0 + w1 + 2.2, y = y0 + h - 0.3))
}
conifer <- function(x0, y0, s = 1, fill = GREEN) {
  tri <- rbindlist(lapply(0:2, function(k) data.table(x = x0 + s * c(-1.6 + 0.35 * k, 0, 1.6 - 0.35 * k),
                                                      y = y0 + s * (c(1.2, 3.4, 1.2) + 1.1 * k), g = k)))
  list(trunk = data.table(x = c(x0, x0), y = c(y0, y0 + 1.3 * s)), crown = tri, fill = fill)
}
dead_tree <- function(x0, y0, s = 1) {                                  # snag with bare branches
  data.table(x = x0 + s * c(0, 0, 0, -1.1, 0, 1.0, 0, -0.7, 0, 0.6),
             y = y0 + s * c(0, 5.2, 2.4, 3.6, 3.2, 4.4, 4.2, 5.0, 4.8, 5.6),
             g = c(1, 1, 2, 2, 3, 3, 4, 4, 5, 5))
}
leaf <- function(x0, y0, s = 1) {
  t <- seq(0, pi, length.out = 40)
  up <- data.table(x = x0 + s * 3 * cos(t), y = y0 + s * 1.3 * sin(t)); lo <- copy(up)[, y := 2 * y0 - y]
  list(blade = rbind(up, lo[.N:1]), rib = data.table(x = x0 + s * c(-3.4, 3), y = y0))
}

# ---- canvas -----------------------------------------------------------------------------------------------------------
p <- ggplot() + coord_fixed(xlim = c(0, 100), ylim = c(0, 50), expand = FALSE) + theme_void() +
  theme(plot.background = element_rect(fill = "white", colour = NA))
lab <- function(x, y, t, size = TXT, ...) annotate("text", x = x, y = y, label = t, size = size, family = FONT, lineheight = 0.9, ...)
box <- function(x0, x1, y0, y1, col = "#BDBDBD", fill = NA, lw = 0.25)
  annotate("rect", xmin = x0, xmax = x1, ymin = y0, ymax = y1, colour = col, fill = fill, linewidth = lw)
arrow_ <- function(x, y, xend, yend, col = LINE)
  annotate("segment", x = x, y = y, xend = xend, yend = yend, colour = col, linewidth = 0.35,
           arrow = arrow(length = unit(1.2, "mm"), type = "closed"))

# column 1: data (FLUXNET: fluxes -> targets, meteorology -> climate predictors; traits feed both models; mortality only C+T+M)
X0 <- 1; X1 <- 35
p <- p + lab(18, 49, "Data", fontface = "bold", size = 7 / .pt)
# (i) flux towers + mini site map
p <- p + box(X0, X1, 25, 47) +
  annotate("segment", x = X0 + 1, xend = X1 - 1, y = 36, yend = 36, colour = "#D9D9D9", linewidth = 0.25, linetype = "22")
tw <- tower(4.6, 38.3, 7.6)
p <- p + geom_path(data = tw$lines, aes(x, y, group = g), colour = LINE, linewidth = 0.3) +
  geom_point(data = tw$sonic, aes(x, y), shape = 21, size = 0.9, fill = "white", colour = LINE, stroke = 0.3)
world <- as.data.table(map_data("world", wrap = c(-180, 180)))[region != "Antarctica"]
world[, c("x", "y") := robinson(long, lat)]
ds <- fread("derived_tables/paper_v5/paper_dataset_full_lags_monthly_tc30_minrun2_buf100-200-300-400-500.csv", select = "SITE_ID")
meta <- unique(fread("fluxnet_2017_2025_V02/fluxnet_shuttle_snapshot_20260512T115423.csv")[, .(SITE_ID = site_id, lat = location_lat, lon = location_long)], by = "SITE_ID")[SITE_ID %in% ds$SITE_ID]
sp <- cbind(meta, robinson(meta$lon, meta$lat))
sx <- function(x) 9.3 + (x + 2.7) / 5.4 * 11; sy <- function(y) 37.9 + (y + 1.4) / 2.9 * 5.7   # map into the panel
p <- p + geom_polygon(data = world, aes(sx(x), sy(y), group = group), fill = "#E3E3E3", colour = NA) +
  geom_point(data = sp, aes(sx(x), sy(y)), size = 0.22, colour = "black")
p <- p + lab(27.5, 45.2, "FLUXNET eddy covariance") + lab(27.5, 41.2, "fluxes, 101 sites\n361 site-years", size = TXS, colour = GREY)
# (ii) climate from the tower meteorology (same FLUXNET files)
sun <- circle(4.6, 31.4, 1.0); rays <- rbindlist(lapply(seq(0, 7, 1) * pi / 4, function(a) data.table(x = 4.6 + c(1.4, 2.0) * cos(a), y = 31.4 + c(1.4, 2.0) * sin(a), g = a)))
cloud <- rbindlist(list(circle(6.8, 28.6, 0.95)[, g := 1], circle(8.2, 29.0, 1.2)[, g := 2], circle(9.6, 28.6, 0.9)[, g := 3]))
p <- p + geom_polygon(data = sun, aes(x, y), fill = SUN, colour = NA) + geom_path(data = rays, aes(x, y, group = g), colour = SUN, linewidth = 0.3) +
  geom_polygon(data = cloud, aes(x, y, group = g), fill = "#D9D9D9", colour = NA) +
  annotate("segment", x = c(7.2, 8.4, 9.6), xend = c(6.9, 8.1, 9.3), y = 27.3, yend = 26.5, colour = SKY, linewidth = 0.35)
p <- p + lab(13, 31.3, "Climate (tower meteorology)", hjust = 0) + lab(13, 28.7, "pre-season, 12 or 24 months", size = TXS, colour = GREY, hjust = 0)
# (iii) plant traits
p <- p + box(X0, X1, 13, 23)
lf <- leaf(6.3, 18, 0.85)
p <- p + geom_polygon(data = lf$blade, aes(x, y), fill = GREEN, colour = NA) + geom_path(data = lf$rib, aes(x, y), colour = "white", linewidth = 0.3)
p <- p + lab(13, 19.3, "Plant traits", hjust = 0) + lab(13, 16.6, "community-weighted means\n(1 km maps)", size = TXS, colour = GREY, hjust = 0)
# (iv) tree mortality and tree cover around the tower (top view with the five buffers)
p <- p + box(X0, X1, 1, 11)
set.seed(3); g <- CJ(i = 1:14, j = 1:14)[, `:=`(x = 2.6 + i * 0.6, y = 1.5 + j * 0.6)]
g[, d := sqrt((i - 7.5)^2 + (j - 7.5)^2)][, cls := fifelse(runif(.N) < 0.16 | (i > 9 & j > 8 & runif(.N) < 0.6), "dead",
                                                       fifelse(runif(.N) < 0.25, "open", "tree"))]
p <- p + geom_tile(data = g[d <= 7.2], aes(x, y, fill = cls), width = 0.58, height = 0.58) +
  scale_fill_manual(values = c(tree = GREEN_L, dead = PINK, open = "#F2F2F2"), guide = "none")
for (r in c(0.85, 1.6, 2.35, 3.1, 3.85)) p <- p + geom_path(data = circle(7.1, 6.0, r), aes(x, y), colour = LINE, linewidth = 0.15, linetype = "22")
p <- p + annotate("point", x = 7.1, y = 6.0, shape = 24, size = 0.9, fill = "black", colour = "black")
for (k in 0:2) { ct <- conifer(14.8 + 2.4 * k, 1.8, 0.45)
  p <- p + geom_path(data = ct$trunk, aes(x, y), colour = DEAD, linewidth = 0.3) +
    geom_polygon(data = ct$crown, aes(x, y, group = g), fill = ct$fill, colour = NA) }
dt <- dead_tree(22.3, 1.8, 0.62); p <- p + geom_path(data = dt, aes(x, y, group = g), colour = PINK, linewidth = 0.4)
p <- p + lab(13, 9.4, "Tree mortality and tree cover", hjust = 0, colour = PINK) +
  lab(13, 7.0, "deadtrees.earth, 100–500 m buffers", size = TXS, colour = GREY, hjust = 0)

# column 2: targets and the two models ---------------------------------------------------------------------------------
p <- p + lab(55, 49, "Models", fontface = "bold", size = 7 / .pt)
p <- p + box(41, 69, 38.5, 47, col = LINE) +
  lab(55, 44.6, "Ecosystem functional properties") +
  annotate("text", x = 55, y = 41.2, label = "GPP[sat]*'   '*NEP[max]*'   '*ET[max]*'   WUE'", parse = TRUE, size = TXT, family = FONT)
p <- p + box(39.8, 70.2, 1, 35, col = "#BDBDBD") + lab(55, 33.3, "AutoGluon, leave-one-site-out", size = TXS, colour = GREY)
p <- p + box(43, 67, 19.5, 30, col = PAL$model[["C+T"]], lw = 0.45) +
  lab(55, 26.6, "Without mortality (C+T)", colour = PAL$model[["C+T"]], fontface = "bold") +
  lab(55, 22.9, "climate + traits", size = TXS, colour = GREY)
p <- p + box(43, 67, 3, 13.5, col = PAL$model[["C+T+M"]], lw = 0.45) +
  lab(55, 10.6, "With mortality (C+T+M)", colour = PAL$model[["C+T+M"]], fontface = "bold") +
  lab(55, 6.9, "climate + traits + mortality", size = TXS, colour = GREY)
# arrows: flux -> targets -> models; climate + traits -> shared line -> both models; mortality -> C+T+M only
p <- p + arrow_(X1 + 0.5, 42.7, 40.5, 42.7) + arrow_(55, 38.2, 55, 35.4)
p <- p + annotate("segment", x = X1 + 0.5, xend = 37.8, y = c(30, 18), yend = c(30, 18), colour = LINE, linewidth = 0.35) +
  annotate("segment", x = 37.8, xend = 37.8, y = 30, yend = 10.5, colour = LINE, linewidth = 0.35) +
  arrow_(37.8, 24.75, 42.6, 24.75) + arrow_(37.8, 10.5, 42.6, 10.5)
p <- p + arrow_(X1 + 0.5, 5.5, 42.6, 5.5, col = PINK)

# column 3: what is compared ------------------------------------------------------------------------------------------
p <- p + lab(87, 49, "Comparison", fontface = "bold", size = 7 / .pt)
items <- data.table(y = c(40, 26, 12), t = c("Prediction error", "Bias in years with mortality", "Direction of the mortality effect"),
                    s = c("R² and MAE (Fig. 2)", "residuals by relative mortality (Fig. 3)", "SHAP by site and mortality class (Figs 4–6)"))
for (i in seq_len(nrow(items))) p <- p + box(75, 99, items$y[i] - 4.8, items$y[i] + 4.8) +
  lab(87, items$y[i] + 1.4, items$t[i]) + lab(87, items$y[i] - 1.9, items$s[i], size = TXS, colour = GREY)
p <- p + annotate("segment", x = 70.2, xend = 72.6, y = 18, yend = 18, colour = LINE, linewidth = 0.35) +
  annotate("segment", x = 72.6, xend = 72.6, y = 12, yend = 40, colour = LINE, linewidth = 0.35)
for (yy in items$y) p <- p + arrow_(72.6, yy, 74.6, yy)

save_pub(p, file.path(OUT, "fig1_schematic"), W2, 94)
cat("schematic ->", file.path(OUT, "fig1_schematic.png"), "\n")
