# ============================================================================
# Figure style for the paper (Nature / Nature Ecology & Evolution house style), 2026-10-14.
# Nature research figure guide: sans-serif (Helvetica/Arial) 5-7 pt, panel labels 8 pt bold lowercase (a, b, c),
# axis lines with ticks, units in parentheses, no background gridlines, no coloured text (use keys), colour-blind-safe
# palette (Wong 2011, Nat. Methods 8:441), vector PDF with embedded fonts, RGB. Widths: 89 mm (single column),
# 120-136 mm (1.5 columns), 183 mm (double column); maximum height 170 mm.
# Font: Nimbus Sans (metric-identical free Helvetica) - the server has no Arial/Helvetica.
#   source("scripts/paper_v41/fig_style.R")
# ============================================================================
suppressMessages({library(ggplot2)})

FONT <- "Nimbus Sans"
MM <- 1 / 25.4
W1 <- 89; W15 <- 120; W2 <- 183; HMAX <- 170           # mm

# ---- colour codes (Okabe-Ito / Wong 2011 for categories; ColorBrewer OrRd for mortality, sequential) -------
OKABE <- c(black = "#000000", orange = "#E69F00", skyblue = "#56B4E9", green = "#009E73", yellow = "#F0E442",
           blue = "#0072B2", vermillion = "#D55E00", purple = "#CC79A7")
PAL <- list(
  model    = c("C+T" = "#0072B2", "C+T+M" = "#D55E00"),                       # without / with mortality
  model_long = c("Without mortality (C+T)" = "#0072B2", "With mortality (C+T+M)" = "#D55E00"),
  class    = c(Low = "#FDCC8A", Mid = "#FC8D59", High = "#B30000"),         # relative-mortality classes (OrRd)
  class2   = c("Low+Mid" = "#FDCC8A", High = "#B30000"),
  class2_piyg = c("Low+Mid" = "#7FBC41", High = "#C51B7D"),          # Fig 3 matched to the PiYG Fig 2 (2026-10-15)
  class2_piyg_pt = c("Low+Mid" = "#276419", High = "#5E0130"),
  learner  = c(AutoGluon = "#009E73", XGBoost = "#0072B2", LightGBM = "#E69F00"),
  igbp     = c(ENF = "#009E73", DBF = "#E69F00", MF = "#CC79A7", EBF = "#0072B2", WET = "#56B4E9",
               CSH = "#D55E00", OSH = "#F0E442", WSA = "#000000"),
  neutral  = "#7F7F7F", zero = "#4D4D4D")
MORT_LIMITS <- c(0, 90)
MORT_COLS <- c("#FDD49E", "#FDBB84", "#FC8D59", "#EF6548", "#D7301F", "#B30000", "#7F0000")   # OrRd 3-9: low end still visible on white
scale_fill_mortality <- function(name = "Relative mortality (%)", ...)
  scale_fill_gradientn(colours = MORT_COLS, limits = MORT_LIMITS, name = name, ...)
scale_colour_mortality <- function(name = "Relative mortality (%)", ...)
  scale_colour_gradientn(colours = MORT_COLS, limits = MORT_LIMITS, name = name, ...)
# alternative for Fig. 2 (user, 2026-10-14): green (low) -> pink/magenta (high), ColorBrewer PiYG without its
# near-white centre so mid-mortality bars stay visible; colour-blind safe (green vs magenta)
MORT_COLS_PIYG <- c("#4D9221", "#7FBC41", "#B8E186", "#F1B6DA", "#DE77AE", "#C51B7D", "#8E0152")
scale_fill_mortality_piyg <- function(name = "Relative mortality (%)", b_lo = NULL, b_hi = NULL, ...) {
  if (is.null(b_lo)) return(scale_fill_gradientn(colours = MORT_COLS_PIYG, limits = MORT_LIMITS, name = name, ...))
  # colours anchored to the class breaks: Low = greens, Mid = light green -> light pink, High = pink -> magenta
  v <- c(0, b_lo, b_hi, 50, MORT_LIMITS[2]) / MORT_LIMITS[2]
  scale_fill_gradientn(colours = c("#276419", "#7FBC41", "#F1B6DA", "#C51B7D", "#8E0152"), values = v,
                       limits = MORT_LIMITS, name = name, breaks = c(0, round(b_lo), round(b_hi), 50, 75), ...)
}

# EFP axis / strip labels as plotmath (subscripts, units in parentheses)
EFP_ORDER_FIG <- c("GPPsat", "NEPmax", "ETmax", "WUE")
# unit labels as used by the authors (user decision 2026-10-15): ETmax mm d-1, WUE g C mm-1
EFP_UNIT <- c(GPPsat = "(mu*mol~m^{-2}~s^{-1})", NEPmax = "(mu*mol~m^{-2}~s^{-1})",
              ETmax = "(mm~d^{-1})", WUE = "(g~C~mm^{-1})")
EFP_EXPR <- setNames(paste0(c(GPPsat = "GPP[sat]", NEPmax = "NEP[max]", ETmax = "ET[max]", WUE = "WUE"), "~", EFP_UNIT), names(EFP_UNIT))
EFP_SHORT <- c(GPPsat = "GPP[sat]", NEPmax = "NEP[max]", ETmax = "ET[max]", WUE = "WUE")

theme_pub <- function(base = 7) {
  theme_classic(base_size = base, base_family = FONT) %+replace% theme(
    line = element_line(linewidth = 0.25, colour = "black"),
    axis.line = element_line(linewidth = 0.25, colour = "black"),
    axis.ticks = element_line(linewidth = 0.25, colour = "black"),
    axis.ticks.length = unit(1.2, "pt"),
    axis.text = element_text(size = 6, colour = "black"),
    axis.title = element_text(size = 7, colour = "black"),
    strip.background = element_blank(),
    strip.text = element_text(size = 7, colour = "black", margin = margin(1, 1, 2, 1)),
    legend.text = element_text(size = 6), legend.title = element_text(size = 6),
    legend.key.size = unit(2.6, "mm"), legend.background = element_blank(), legend.key = element_blank(),
    legend.margin = margin(0, 0, 0, 0), legend.box.spacing = unit(1.5, "mm"),
    panel.grid = element_blank(), panel.background = element_blank(), plot.background = element_blank(),
    plot.title = element_text(size = 7, face = "bold", hjust = 0, margin = margin(0, 0, 2, 0)),
    plot.tag = element_text(size = 8, face = "bold", family = FONT),
    plot.margin = margin(1.5, 1.5, 1.5, 1.5, "mm"))
}
GEOM_TEXT_SIZE <- 6 / .pt                                   # 6 pt in ggplot's mm units
update_geom_defaults("text", list(family = FONT, size = GEOM_TEXT_SIZE, colour = "black"))
update_geom_defaults("label", list(family = FONT, size = GEOM_TEXT_SIZE))

stars <- function(q) ifelse(is.na(q), "", ifelse(q < 0.001, "***", ifelse(q < 0.01, "**", ifelse(q < 0.05, "*", ""))))

# vector PDF (embedded fonts, cairo) for the journal + 600-dpi PNG for Overleaf drafts
save_pub <- function(p, stem, width_mm, height_mm) {
  stopifnot(width_mm <= W2 + 0.5, height_mm <= HMAX + 0.5)
  dir.create(dirname(stem), showWarnings = FALSE, recursive = TRUE)
  ggsave(paste0(stem, ".pdf"), p, width = width_mm * MM, height = height_mm * MM, device = cairo_pdf, bg = "white")
  ggsave(paste0(stem, ".png"), p, width = width_mm * MM, height = height_mm * MM, dpi = 600, device = ragg::agg_png, bg = "white")
  invisible(stem)
}
