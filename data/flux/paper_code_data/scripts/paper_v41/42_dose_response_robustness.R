#!/usr/bin/env Rscript
# ============================================================================
# Robustness of the site-year dose-response slopes (41_mortality_dose_response.R, tests 1a and 2a), 2026-10-09.
#   bootstrap  - site-block bootstrap: sites drawn with replacement (all their years together, a drawn site keeps
#                its own random intercept), mixed model refitted, N_BOOT draws; 95 % percentile interval and a
#                two-sided bootstrap p (2 x share of draws on the other side of 0). No normality assumption.
#   loso       - leave one site out: the mixed model refitted 101 times, each time without one site; range of
#                slopes, share of refits with p < 0.05, and the most influential site (largest change of slope).
#   no_extreme - sensitivity only: without the sites whose maximum relative mortality exceeds EXTREME (%).
# Slopes per 10 % relative mortality of the site-year (mean of the five buffers), EFP units, as in script 41.
# Output: plots/<sub>/dose_response/dose_response_robustness_<L>.csv
#   PAPER_RESP_SET=efp_v5 PAPER_NO_THRESH=1 Rscript 42_dose_response_robustness.R [AG|XGB|LGB]
# ============================================================================
if (Sys.getenv("PAPER_RESP_SET") == "") Sys.setenv(PAPER_RESP_SET = "efp_v5", PAPER_NO_THRESH = "1")
suppressMessages({library(data.table); library(nlme); library(parallel)})
setwd(Sys.getenv("PAPER_ROOT", "."))
source("scripts/paper_v41/paper_common.R")
L <- commandArgs(trailingOnly = TRUE)[1]; if (is.na(L)) L <- "AG"
N_BOOT <- 1000L; EXTREME <- 50; CORES <- as.integer(Sys.getenv("PAPER_CORES", "16"))
f <- if (L == "AG") file.path(PAPER_ROOT, "AutoGluon/outputs", paste0(PAPER_RESP_SET, if (PAPER_NO_THRESH) "_nothresh" else ""),
                              "AG_predictions_repCV.csv") else file.path(PAPER_OUT, sprintf("%s_predictions_repCV.csv", L))
OUT <- file.path(PAPER_FIG, "dose_response"); dir.create(OUT, showWarnings = FALSE, recursive = TRUE)

cd <- paper_class_data()[, .(SITE_ID, YEAR, tm = relative_disturbance_cls)]
p <- fread(f)[response %in% EFP_ORDER & grepl("^M[34]_(12m|24m)$", model)]
p[, c("mod", "window") := tstrsplit(model, "_")]
w <- dcast(p, response + window + SITE_ID + YEAR + observed ~ mod, value.var = "predicted")
w <- merge(w, cd, by = c("SITE_ID", "YEAR"))[is.finite(M3) & is.finite(M4) & is.finite(tm)]
w[, `:=`(r3 = observed - M3, r4 = observed - M4, x = tm / 10)]
w[, d := abs(r4) - abs(r3)]
smax <- w[, .(tmax = max(tm)), by = SITE_ID]
EXT_SITES <- smax[tmax > EXTREME, SITE_ID]

fit1 <- function(y, x, g) {
  m <- tryCatch(lme(y ~ x, random = ~ 1 | g, method = "REML", control = lmeControl(opt = "optim")), error = function(e) NULL)
  if (is.null(m)) return(c(NA_real_, NA_real_, NA_real_))
  tt <- summary(m)$tTable; c(tt["x", "Value"], tt["x", "p-value"], tt["x", "Std.Error"])
}
TESTS <- c(t1a = "d", t2a_CT = "r3", t2a_CTM = "r4")

rows <- list(); set.seed(42)
for (e in EFP_ORDER) for (win in WINDOWS) {
  x <- w[response == e & window == win]
  sites <- unique(x$SITE_ID); idx <- split(seq_len(nrow(x)), x$SITE_ID)[sites]
  draws <- lapply(seq_len(N_BOOT), function(b) sample(length(sites), replace = TRUE))
  for (tn in names(TESTS)) {
    y <- x[[TESTS[[tn]]]]
    full <- fit1(y, x$x, x$SITE_ID)
    # site-block bootstrap (each drawn copy of a site gets its own group label)
    bs <- unlist(mclapply(draws, function(dr) {
      ii <- unlist(idx[dr]); g <- rep(seq_along(dr), lengths(idx[dr]))
      fit1(y[ii], x$x[ii], g)[1]
    }, mc.cores = CORES))
    bs <- bs[is.finite(bs)]
    # leave one site out
    lo <- rbindlist(mclapply(sites, function(s) { k <- x$SITE_ID != s; r <- fit1(y[k], x$x[k], x$SITE_ID[k])
      data.table(site = s, slope = r[1], p = r[2]) }, mc.cores = CORES))
    infl <- lo[which.max(abs(slope - full[1]))]
    # without the extreme-mortality sites
    k <- !x$SITE_ID %in% EXT_SITES; ne <- fit1(y[k], x$x[k], x$SITE_ID[k])
    rows[[length(rows) + 1]] <- data.table(learner = L, response = e, window = win, test = tn,
      slope = full[1], p_lme = full[2], se_lme = full[3],
      boot_lo = quantile(bs, 0.025), boot_hi = quantile(bs, 0.975),
      p_boot = min(1, 2 * min(mean(bs >= 0), mean(bs <= 0))), n_boot = length(bs),
      loso_min = min(lo$slope, na.rm = TRUE), loso_max = max(lo$slope, na.rm = TRUE),
      loso_share_p05 = mean(lo$p < 0.05, na.rm = TRUE), loso_most_infl_site = infl$site, loso_most_infl_slope = infl$slope,
      noext_n_sites = uniqueN(x$SITE_ID[k]), noext_slope = ne[1], noext_p = ne[2], noext_se = ne[3])
  }
  cat(sprintf("%s %s done\n", e, win))
}
R <- rbindlist(rows)
R[, q_boot := p.adjust(p_boot, "BH"), by = test]
R[, q_lme := p.adjust(p_lme, "BH"), by = test]
R[, q_noext := p.adjust(noext_p, "BH"), by = test]
fwrite(R, file.path(OUT, sprintf("dose_response_robustness_%s.csv", L)))
# ---- figure: slope per 10 % relative mortality under each check (interval = 95 %; LOSO = range of the 101 refits) ----
suppressMessages(library(ggplot2))
TL <- c(t1a = "1a  Error change\n|C+T+M| - |C+T|", t2a_CT = "2a  Residual\nC+T (without M)",
        t2a_CTM = "2a  Residual\nC+T+M (with M)")
ML <- c("Mixed model (all sites)", "Site-block bootstrap", "Leave one site out (range)", "Without the 3 extreme sites")
G <- rbind(
  R[, .(response, window, test, method = ML[1], est = slope, lo = slope - 1.96 * se_lme, hi = slope + 1.96 * se_lme, sig = q_lme < 0.05)],
  R[, .(response, window, test, method = ML[2], est = slope, lo = boot_lo, hi = boot_hi, sig = q_boot < 0.05)],
  R[, .(response, window, test, method = ML[3], est = slope, lo = loso_min, hi = loso_max, sig = loso_share_p05 >= 0.95)],
  R[, .(response, window, test, method = ML[4], est = noext_slope, lo = noext_slope - 1.96 * noext_se, hi = noext_slope + 1.96 * noext_se, sig = q_noext < 0.05)])
G[, `:=`(response = factor(response, EFP_ORDER), test = factor(TL[test], TL), method = factor(method, rev(ML)),
         sigl = ifelse(sig, "significant", "not significant"))]
G[, y := match(window, WINDOWS) + (match(as.character(method), ML) - 2.5) * -0.17]   # explicit offsets, top = first method
MC <- setNames(c("#555555", "#C2571A", "#3B6FB6", "#2E9E6A"), ML)
pr <- ggplot(G, aes(est, y, colour = method)) +
  geom_vline(xintercept = 0, colour = "grey40", linewidth = 0.4) +
  geom_segment(aes(x = lo, xend = hi, yend = y), linewidth = 0.7) +
  geom_point(aes(shape = sigl), size = 2.2, fill = "white", stroke = 0.9) +
  scale_y_continuous(breaks = seq_along(WINDOWS), labels = WINDOWS, expand = expansion(add = 0.45)) +
  scale_colour_manual(values = MC, breaks = ML, name = NULL) +
  scale_shape_manual(values = c(significant = 16, `not significant` = 21), name = NULL) +
  ggh4x::facet_grid2(test ~ response, scales = "free_x", independent = "x") +
  labs(x = "Slope per +10 % relative mortality of the site-year (EFP units)", y = NULL,
       title = sprintf("Robustness of the dose-response slopes (%s)", L),
       subtitle = paste0("Interval: mixed model / without extremes = 95 % CI from the model; bootstrap = 95 % percentile interval (", N_BOOT,
                         " site-block draws); leave one site out = range of the slopes over all refits\n",
                         "Filled = significant (BH q < 0.05 across the 8 EFP x window tests; leave one site out: p < 0.05 in >= 95 % of refits). Extreme sites (> ", EXTREME,
                         " % relative mortality): ", paste(EXT_SITES, collapse = ", "))) +
  theme_bw(base_size = 9) + theme(panel.grid.minor = element_blank(), legend.position = "bottom", legend.box = "vertical",
    strip.background = element_rect(fill = "#EFEFEF", colour = "grey80"), strip.text = element_text(face = "bold"),
    strip.text.y = element_text(angle = 0, hjust = 0), panel.grid.major.y = element_blank(),
    plot.title = element_text(face = "bold"), plot.subtitle = element_text(size = 7.5, colour = "grey30"))
ggsave(file.path(OUT, sprintf("dose_response_robustness_%s.png", L)), pr, width = 11, height = 7, dpi = 250, bg = "white")

cat(sprintf("extreme sites (> %d %% relative mortality): %s\n", EXTREME, paste(EXT_SITES, collapse = ", ")))
print(R[, .(response, window, test, slope = round(slope, 3), p_lme = signif(p_lme, 2),
            boot = sprintf("[%.3f, %.3f]", boot_lo, boot_hi), q_boot = signif(q_boot, 2),
            loso = sprintf("[%.3f, %.3f]", loso_min, loso_max), loso_p05 = round(loso_share_p05, 2),
            infl = loso_most_infl_site, noext = round(noext_slope, 3), q_noext = signif(q_noext, 2))])
cat("ROBUST_DONE\n")
