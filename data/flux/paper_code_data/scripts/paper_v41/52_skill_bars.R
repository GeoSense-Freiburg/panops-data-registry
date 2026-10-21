#!/usr/bin/env Rscript
# ============================================================================
# Bar-plot version of Table 1 (R²) and Table 2 (MAE reduction), AutoGluon, Nature style (fig_style.R), 2026-10-20
# (meeting with Teja: tables to figures, first and last two columns of Table 2, with / without mortality, 12 and 24 months).
#   a  R² of the leave-one-site-out predictions, C+T vs C+T+M. Error bars: 95 % site-block bootstrap (10 000 draws, whole
#      sites resampled). Stars: one-sided paired permutation test of the R² gain, C+T / C+T+M predictions swapped per site
#      (10 000 draws), Benjamini-Hochberg across the 8 tests (4 properties x 2 windows).
#   b  relative MAE decrease (%) when mortality is added = -(% change) of Table 2 for all site-years and for the site-years
#      above the Mid (10.6 %) and High (24.0 %) breaks; interval and stars exactly as in Table 2 (39_paired_bootstrap.R,
#      BH over the AutoGluon tests of the site-year level).
# Output: plots/paper_v5_nothresh/nature/AG_24m/fig_skill_bars.pdf/.png + skill_r2_bootstrap_AG.csv
# ============================================================================
suppressMessages({library(data.table); library(ggplot2); library(patchwork)})
ROOT <- Sys.getenv("PAPER_ROOT", Sys.getenv("PAPER_ROOT", ".")); setwd(ROOT)
source("scripts/paper_v41/fig_style.R")
set.seed(20261020)
N <- 10000L
EFP <- EFP_ORDER_FIG; WLAB <- c("12m" = "12 months", "24m" = "24 months")
OUT <- "plots/paper_v5_nothresh/nature/AG_24m"

# ---- a. R² with site-block bootstrap and paired permutation of the gain -----------------------------------------
p <- fread("AutoGluon/outputs/efp_v5_nothresh/AG_predictions_repCV.csv")[response %in% EFP & grepl("^M[34]_(12m|24m)$", model)]
p[, c("mod", "window") := tstrsplit(model, "_")]
w <- dcast(p, response + window + SITE_ID + YEAR + observed ~ mod, value.var = "predicted")[is.finite(M3) & is.finite(M4)]
r2 <- function(o, f) 1 - sum((o - f)^2) / sum((o - mean(o))^2)
S <- rbindlist(lapply(split(w, by = c("response", "window")), function(x) {
  sid <- x$SITE_ID; us <- unique(sid); idx <- split(seq_len(nrow(x)), sid)[us]
  # site-level sums make the bootstrap and the permutation fast: SSE and the moments of the observations per site
  e3 <- rowsum((x$observed - x$M3)^2, sid)[us, 1]; e4 <- rowsum((x$observed - x$M4)^2, sid)[us, 1]
  so <- rowsum(x$observed, sid)[us, 1]; so2 <- rowsum(x$observed^2, sid)[us, 1]; n <- lengths(idx)
  k <- length(us)
  B <- matrix(sample.int(k, N * k, replace = TRUE), N, k)
  cnt <- t(apply(B, 1, tabulate, nbins = k))                         # how often each site is drawn
  sst <- (cnt %*% so2)[, 1] - ((cnt %*% so)[, 1])^2 / (cnt %*% n)[, 1]
  b3 <- 1 - (cnt %*% e3)[, 1] / sst; b4 <- 1 - (cnt %*% e4)[, 1] / sst
  sst0 <- sum(so2) - sum(so)^2 / sum(n)
  obs <- (sum(e3) - sum(e4)) / sst0                                    # R² gain = (SSE3 - SSE4) / SST
  sg <- matrix(sample(c(-1, 1), N * k, replace = TRUE), N, k)          # swap the two models per site
  perm <- (sg %*% (e3 - e4))[, 1] / sst0
  data.table(response = x$response[1], window = x$window[1], n_sites = k, n_sy = nrow(x),
             r2_CT = r2(x$observed, x$M3), r2_CTM = r2(x$observed, x$M4),
             lo_CT = quantile(b3, 0.025), hi_CT = quantile(b3, 0.975), lo_CTM = quantile(b4, 0.025), hi_CTM = quantile(b4, 0.975),
             gain = obs, gain_lo = quantile(b4 - b3, 0.025), gain_hi = quantile(b4 - b3, 0.975),
             p_perm = (1 + sum(perm >= obs)) / (N + 1))
}))
S[, q := p.adjust(p_perm, "BH")]
fwrite(S, file.path(OUT, "skill_r2_bootstrap_AG.csv"))
A <- rbind(S[, .(response, window, model = "C+T", v = r2_CT, lo = lo_CT, hi = hi_CT)],
           S[, .(response, window, model = "C+T+M", v = r2_CTM, lo = lo_CTM, hi = hi_CTM)])
A[, `:=`(response = factor(response, EFP), win = factor(WLAB[window], WLAB))]
S[, `:=`(response = factor(response, EFP), win = factor(WLAB[window], WLAB), ytop = pmax(hi_CT, hi_CTM))]
xl <- setNames(sapply(EFP, function(e) parse(text = EFP_SHORT[[e]])), EFP)
pd <- position_dodge(0.75)
pa <- ggplot(A, aes(response, v, fill = model)) +
  geom_col(width = 0.7, position = pd) +
  geom_errorbar(aes(ymin = lo, ymax = hi), width = 0, linewidth = 0.3, position = pd) +
  geom_text(data = S, aes(response, ytop + 0.03, label = stars(q)), inherit.aes = FALSE) +
  facet_wrap(~ win, nrow = 1) +
  scale_fill_manual(values = PAL$model, labels = c("Without mortality (C+T)", "With mortality (C+T+M)"), name = NULL) +
  scale_x_discrete(labels = xl) + scale_y_continuous(expand = expansion(mult = c(0, 0.08))) +
  labs(x = NULL, y = expression(italic(R)^2)) + theme_pub() + theme(legend.position = "top")

# ---- b. relative MAE decrease, from the paired bootstrap of Table 2 ----------------------------------------------
st <- fread("plots/paper_v5_nothresh/paired_bootstrap_statistics.csv")[metric == "MAE" & learner == "AG"]
st[, q := p.adjust(p_perm, "BH"), by = level]                          # as Table 2 (AutoGluon tests only)
b <- st[level == "siteyear" & subset %in% c("all", "yr_midhigh", "yr_high")]
B_LO <- b$b_lo[1]; B_HI <- b$b_hi[1]
SL <- c(all = "All site-years", yr_midhigh = sprintf("Site-years > %.1f %%", B_LO), yr_high = sprintf("Site-years > %.1f %% (high mortality)", B_HI))
b[, `:=`(dec = -pct, lo = -pct_hi, hi = -pct_lo, response = factor(response, EFP), win = factor(WLAB[window], WLAB),
         sub = factor(SL[subset], SL))]
SC <- setNames(c("#BDBDBD", "#F1B6DA", "#C51B7D"), SL)                   # grey, then the pink end of the Fig. 3 scale
pb <- ggplot(b, aes(response, dec, fill = sub)) +
  geom_hline(yintercept = 0, colour = PAL$zero, linewidth = 0.25) +
  geom_col(width = 0.75, position = pd) +
  geom_errorbar(aes(ymin = lo, ymax = hi), width = 0, linewidth = 0.3, position = pd) +
  geom_text(aes(y = pmax(hi, 0) + 2, label = stars(q), group = sub), position = pd, size = 5 / .pt) +
  facet_wrap(~ win, nrow = 1) +
  scale_fill_manual(values = SC, name = NULL) + scale_x_discrete(labels = xl) +
  labs(x = NULL, y = "Relative MAE decrease (%)") + theme_pub() + theme(legend.position = "top")

f <- (pa / pb) + plot_annotation(tag_levels = "a")
save_pub(f, file.path(OUT, "fig_skill_bars"), W2, 120)
print(S[, .(response, window, r2_CT = round(r2_CT, 3), r2_CTM = round(r2_CTM, 3), gain = round(gain, 3), q = signif(q, 2))])
