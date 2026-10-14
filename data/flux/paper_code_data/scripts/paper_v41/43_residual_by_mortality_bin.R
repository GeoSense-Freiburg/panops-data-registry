#!/usr/bin/env Rscript
# ============================================================================
# Simple view of test 2a (2026-10-09): mean residual (observed - predicted) of the models without (C+T) and with
# mortality (C+T+M) per class of the site-year's relative mortality. Below 0 = the model over-predicts the EFP.
# Interval: 95 % site-block bootstrap within each class (sites resampled with all their years in that class).
# Relative mortality = mean of the five buffers (paper_class_data()); classes 0-10, 10-20, 20-30, >= 30 %.
# Output: plots/<sub>/dose_response/residual_by_mortality_<L>_<win>.png (+ .csv)
#   PAPER_RESP_SET=efp_v5 PAPER_NO_THRESH=1 Rscript 43_residual_by_mortality_bin.R [AG|XGB|LGB] [24m|12m]
# ============================================================================
if (Sys.getenv("PAPER_RESP_SET") == "") Sys.setenv(PAPER_RESP_SET = "efp_v5", PAPER_NO_THRESH = "1")
suppressMessages({library(data.table); library(ggplot2)})
setwd(Sys.getenv("PAPER_ROOT", "."))
source("scripts/paper_v41/paper_common.R")
a <- commandArgs(trailingOnly = TRUE); L <- if (length(a) >= 1) a[1] else "AG"; WIN <- if (length(a) >= 2) a[2] else "24m"
LAB <- c(AG = "AutoGluon", XGB = "XGBoost", LGB = "LightGBM")[[L]]
# env AG_SET (2026-10-12): another AutoGluon run, e.g. efp_v5_nothresh_efpclim; files are tagged AG_<set suffix>
AG_SET <- Sys.getenv("AG_SET", paste0(PAPER_RESP_SET, if (PAPER_NO_THRESH) "_nothresh" else ""))
if (L == "AG" && AG_SET != paste0(PAPER_RESP_SET, if (PAPER_NO_THRESH) "_nothresh" else "")) {
  LAB <- paste0(LAB, ", EFP-specific climate"); L_TAG <- paste0("AG_", sub("^.*_", "", AG_SET)) } else L_TAG <- L
f <- if (L == "AG") file.path(PAPER_ROOT, "AutoGluon/outputs", AG_SET, "AG_predictions_repCV.csv") else file.path(PAPER_OUT, sprintf("%s_predictions_repCV.csv", L))
OUT <- file.path(PAPER_FIG, "dose_response"); dir.create(OUT, showWarnings = FALSE, recursive = TRUE)
BRK <- c(-Inf, 10, 20, 30, Inf); BLAB <- c("0-10 %", "10-20 %", "20-30 %", "> 30 %")
cd <- paper_class_data()[, .(SITE_ID, YEAR, tm = relative_disturbance_cls)]
p <- fread(f)[response %in% EFP_ORDER & model %in% paste0(c("M3_", "M4_"), WIN)]
p[, mod := sub("_.*", "", model)]
w <- merge(dcast(p, response + SITE_ID + YEAR + observed ~ mod, value.var = "predicted"), cd, by = c("SITE_ID", "YEAR"))
w <- w[is.finite(M3) & is.finite(M4) & is.finite(tm)]
w[, bin := factor(cut(tm, BRK, BLAB, right = FALSE), BLAB)]
long <- rbind(w[, .(response, SITE_ID, bin, res = observed - M3, model = "Without mortality (C+T)")],
              w[, .(response, SITE_ID, bin, res = observed - M4, model = "With mortality (C+T+M)")])
set.seed(42)
boot_ci <- function(res, site) {               # site-block bootstrap of the mean residual
  idx <- split(seq_along(res), site); k <- length(idx)
  b <- replicate(2000, { ii <- unlist(idx[sample(k, replace = TRUE)]); mean(res[ii]) })
  list(mean = mean(res), lo = quantile(b, 0.025), hi = quantile(b, 0.975), n_sy = length(res), n_sites = k)
}
S <- long[, boot_ci(res, SITE_ID), by = .(response, model, bin)]
S[, response := factor(response, EFP_ORDER)]
fwrite(S, file.path(OUT, sprintf("residual_by_mortality_%s_%s.csv", L_TAG, WIN)))
# ---- is the correction significant? corr = residual(C+T+M) - residual(C+T) = prediction lowered by adding mortality.
# Per class: mean correction, 95 % site-block bootstrap interval, one-sided paired permutation (100 000 draws; labels swapped per site,
# i.e. the site's corrections sign-flipped together; H1: correction > 0 = predictions lowered); BH across the 16 tests.
# Trend: mixed model corr ~ relative mortality (per 10 %) + (1 | site), slope with site-block bootstrap.
w[, corr := (observed - M4) - (observed - M3)]
perm_p <- function(v, site, n = 100000L) {
  ss <- rowsum(v, site)[, 1]; obs <- sum(ss); k <- length(ss)
  sg <- matrix(sample(c(-1, 1), n * k, replace = TRUE), n, k)
  (1 + sum(as.vector(sg %*% ss) >= obs)) / (n + 1)
}
C <- w[, c(boot_ci(corr, SITE_ID), p_perm = perm_p(corr, SITE_ID)), by = .(response, bin)]
C[, q_perm := p.adjust(p_perm, "BH")]
suppressMessages(library(nlme))
slope <- function(y, x, g) { m <- tryCatch(lme(y ~ x, random = ~ 1 | g, control = lmeControl(opt = "optim")), error = function(e) NULL)
  if (is.null(m)) NA_real_ else fixef(m)[["x"]] }
TR <- w[, {
  idx <- split(seq_len(.N), SITE_ID); k <- length(idx); x10 <- tm / 10
  b <- replicate(500, { dr <- sample(k, replace = TRUE); ii <- unlist(idx[dr]); slope(corr[ii], x10[ii], rep(seq_along(dr), lengths(idx[dr]))) })
  .(slope = slope(corr, x10, SITE_ID), lo = quantile(b, 0.025, na.rm = TRUE), hi = quantile(b, 0.975, na.rm = TRUE),
    p_boot = min(1, 2 * min(mean(b <= 0, na.rm = TRUE), mean(b >= 0, na.rm = TRUE))))
}, by = response]
TR[, q_boot := p.adjust(p_boot, "BH")]
fwrite(C, file.path(OUT, sprintf("residual_correction_by_mortality_%s_%s.csv", L_TAG, WIN)))
fwrite(TR, file.path(OUT, sprintf("residual_correction_trend_%s_%s.csv", L_TAG, WIN)))
cat("\n== correction per class (prediction lowered by adding mortality, EFP units) ==\n")
print(C[order(response, bin), .(response, bin, corr = round(mean, 3), lo = round(lo, 3), hi = round(hi, 3), q_perm = signif(q_perm, 2), n_sy, n_sites)])
cat("\n== trend: correction per +10 % relative mortality (mixed model, site-block bootstrap) ==\n")
print(TR[, .(response, slope = round(slope, 3), lo = round(lo, 3), hi = round(hi, 3), q_boot = signif(q_boot, 2))])

SIG <- C[, .(response = factor(response, EFP_ORDER), bin, lab = fifelse(q_perm < 0.001, "***", fifelse(q_perm < 0.01, "**", fifelse(q_perm < 0.05, "*", ""))))]
N <- S[model == "Without mortality (C+T)", .(response, bin, txt = sprintf("%d\n(%d)", n_sy, n_sites))]
MC <- c("Without mortality (C+T)" = "#22C4E0", "With mortality (C+T+M)" = "#E8257A")
pd <- position_dodge(width = 0.5)
g <- ggplot(S, aes(bin, mean, colour = model, group = model)) +
  geom_hline(yintercept = 0, colour = "grey40") +
  geom_line(position = pd, linewidth = 0.6, alpha = 0.6) +
  geom_errorbar(aes(ymin = lo, ymax = hi), width = 0, position = pd, linewidth = 0.8) +
  geom_point(position = pd, size = 2.6) +
  geom_text(data = SIG, aes(x = bin, y = Inf, label = lab), inherit.aes = FALSE, vjust = 1.3, size = 3.6, fontface = "bold", colour = "grey15") +
  geom_text(data = N, aes(x = bin, y = -Inf, label = txt), inherit.aes = FALSE, vjust = -0.3, size = 2.4, colour = "grey35", lineheight = 0.85) +
  scale_colour_manual(values = MC, breaks = names(MC), name = NULL) +
  scale_y_continuous(expand = expansion(mult = c(0.22, 0.16))) +
  facet_wrap(~ response, nrow = 1, scales = "free_y", labeller = labeller(response = RESP_UNITS)) +
  labs(x = "Relative mortality of the site-year", y = "Mean residual (observed - predicted)\nbelow 0 = model over-predicts",
       title = sprintf("Over-prediction in high-mortality years: without vs with mortality (%s, %s)", LAB, WIN),
       subtitle = paste0("Points = mean residual of all site-years in the class; bars = 95 % site-block bootstrap interval. Numbers at the bottom: site-years (sites).\n",
                         "Stars at the top: adding mortality lowers the prediction significantly in that class (one-sided paired permutation, models swapped per site, BH-FDR; * q<0.05, ** q<0.01, *** q<0.001)")) +
  theme_bw(base_size = 10) + theme(panel.grid.minor = element_blank(), panel.grid.major.x = element_blank(), legend.position = "bottom",
    strip.background = element_rect(fill = "#EFEFEF", colour = "grey80"), strip.text = element_text(face = "bold"),
    plot.title = element_text(face = "bold", size = 11), plot.subtitle = element_text(size = 8, colour = "grey30"))
ggsave(file.path(OUT, sprintf("residual_by_mortality_%s_%s.png", L_TAG, WIN)), g, width = 12, height = 4.6, dpi = 250, bg = "white")
print(S[, .(response, model, bin, mean = round(mean, 3), lo = round(lo, 3), hi = round(hi, 3), n_sy, n_sites)])
