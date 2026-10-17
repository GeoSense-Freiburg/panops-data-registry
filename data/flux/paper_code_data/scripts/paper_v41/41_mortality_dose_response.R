#!/usr/bin/env Rscript
# ============================================================================
# Dose-response tests with mortality as a CONTINUOUS variable (2026-10-09). The group tests (Low / Mid / High)
# lose power because the High group is small (22 sites); these tests use all sites and site-years.
#   Test 1  - does the error reduction grow with mortality?
#     1a site-year: d = |err C+T+M| - |err C+T| ~ relative mortality of that year + (1 | site)   (nlme::lme)
#     1b site:      log(MAE C+T+M / MAE C+T) per site ~ site maximum of relative mortality         (Spearman)
#   Test 2  - is the model WITHOUT mortality wrong where mortality is high?
#     2a site-year: residual of C+T (observed - predicted) ~ relative mortality + (1 | site); the same for C+T+M
#                   (if mortality is the missing information, the slope should shrink towards 0 with C+T+M)
#     2b site:      mean C+T residual per site ~ site maximum of relative mortality                   (Spearman)
# Relative mortality = mean of the five buffers (paper_class_data(), as the site classes). Slopes per 10 % total
# mortality, in EFP units. Tests two-sided; BH-FDR within each test family (8 = 4 EFPs x 2 windows).
# Output: plots/<sub>/dose_response/dose_response_<L>.csv and dose_response_<L>.png
#   PAPER_RESP_SET=efp_v5 PAPER_NO_THRESH=1 Rscript 41_mortality_dose_response.R [AG|XGB|LGB]
# ============================================================================
if (Sys.getenv("PAPER_RESP_SET") == "") Sys.setenv(PAPER_RESP_SET = "efp_v5", PAPER_NO_THRESH = "1")
suppressMessages({library(data.table); library(nlme); library(ggplot2)})
setwd(Sys.getenv("PAPER_ROOT", "."))
source("scripts/paper_v41/paper_common.R")
L <- commandArgs(trailingOnly = TRUE)[1]; if (is.na(L)) L <- "AG"
f <- if (L == "AG") file.path(PAPER_ROOT, "AutoGluon/outputs", paste0(PAPER_RESP_SET, if (PAPER_NO_THRESH) "_nothresh" else ""),
                              "AG_predictions_repCV.csv") else file.path(PAPER_OUT, sprintf("%s_predictions_repCV.csv", L))
OUT <- file.path(PAPER_FIG, "dose_response"); dir.create(OUT, showWarnings = FALSE, recursive = TRUE)

cd <- paper_class_data()[, .(SITE_ID, YEAR, tm = relative_disturbance_cls)]
p <- fread(f)[response %in% EFP_ORDER & grepl("^M[34]_(12m|24m)$", model)]
p[, c("mod", "window") := tstrsplit(model, "_")]
w <- dcast(p, response + window + SITE_ID + YEAR + observed ~ mod, value.var = "predicted")
w <- merge(w, cd, by = c("SITE_ID", "YEAR"))[is.finite(M3) & is.finite(M4) & is.finite(tm)]
w[, `:=`(r3 = observed - M3, r4 = observed - M4)]
w[, d := abs(r4) - abs(r3)]
w[, tm10 := tm / 10]                                   # slope per 10 % relative mortality

slope_lme <- function(y, x, g) {
  fit <- tryCatch(lme(y ~ x, random = ~ 1 | g, method = "REML", control = lmeControl(opt = "optim")), error = function(e) NULL)
  if (is.null(fit)) return(list(int = NA_real_, slope = NA_real_, se = NA_real_, p = NA_real_))
  tt <- summary(fit)$tTable; list(int = tt["(Intercept)", "Value"], slope = tt["x", "Value"], se = tt["x", "Std.Error"], p = tt["x", "p-value"])
}
sp <- function(x, y) { ct <- suppressWarnings(cor.test(x, y, method = "spearman", exact = FALSE)); list(rho = unname(ct$estimate), p = ct$p.value) }

rows <- list()
for (e in EFP_ORDER) for (win in WINDOWS) {
  x <- w[response == e & window == win]
  s <- x[, .(mae3 = mean(abs(r3)), mae4 = mean(abs(r4)), res3 = mean(r3), res4 = mean(r4), tmax = max(tm)), by = SITE_ID]
  a <- slope_lme(x$d, x$tm10, x$SITE_ID)
  b <- sp(s$tmax, log(s$mae4 / s$mae3))
  c3 <- slope_lme(x$r3, x$tm10, x$SITE_ID); c4 <- slope_lme(x$r4, x$tm10, x$SITE_ID)
  d3 <- sp(s$tmax, s$res3); d4 <- sp(s$tmax, s$res4)
  rows[[length(rows) + 1]] <- data.table(learner = L, response = e, window = win, n_sites = nrow(s), n_sy = nrow(x),
    t1a_int = a$int, t1a_slope = a$slope, t1a_se = a$se, t1a_p = a$p,
    t1b_rho = b$rho, t1b_p = b$p,
    t2a_int_CT = c3$int, t2a_int_CTM = c4$int, t2a_slope_CT = c3$slope, t2a_se_CT = c3$se, t2a_p_CT = c3$p,
    t2a_slope_CTM = c4$slope, t2a_se_CTM = c4$se, t2a_p_CTM = c4$p,
    t2b_rho_CT = d3$rho, t2b_p_CT = d3$p, t2b_rho_CTM = d4$rho, t2b_p_CTM = d4$p)
}
R <- rbindlist(rows)
for (pc in grep("_p(_|$)", names(R), value = TRUE)) R[, (sub("_p", "_q", pc)) := p.adjust(get(pc), "BH")]
fwrite(R, file.path(OUT, sprintf("dose_response_%s.csv", L)))

# ---- figure: per site, panel row 1 = log MAE ratio vs mortality (test 1b), row 2 = C+T residual vs mortality (2b) ----
S <- w[, .(lr = log(mean(abs(r4)) / mean(abs(r3))), res3 = mean(r3), res4 = mean(r4), tmax = max(tm)), by = .(response, window, SITE_ID)]
S[, response := factor(response, EFP_ORDER)]
lab <- function(rho, q) sprintf("rho = %+.2f%s", rho, fifelse(q < 0.001, "***", fifelse(q < 0.01, "**", fifelse(q < 0.05, "*", " ns"))))
L1 <- R[, .(response = factor(response, EFP_ORDER), window, txt = lab(t1b_rho, t1b_q))]
L2 <- R[, .(response = factor(response, EFP_ORDER), window,
            txt = paste0("C+T ", lab(t2b_rho_CT, t2b_q_CT), "\nC+T+M ", lab(t2b_rho_CTM, t2b_q_CTM)))]
th <- theme_bw(base_size = 9) + theme(panel.grid.minor = element_blank(), strip.background = element_rect(fill = "#EFEFEF", colour = "grey80"),
                                     strip.text = element_text(face = "bold"), plot.title = element_text(face = "bold"))
mk <- function(yv, ylab, LL, ttl) ggplot(S, aes(tmax, .data[[yv]])) +
  geom_hline(yintercept = 0, linetype = "dashed", colour = "grey50") +
  geom_point(size = 1, alpha = 0.6, colour = "#3B6FB6") +
  geom_smooth(method = "lm", formula = y ~ x, colour = "#C2571A", fill = "#C2571A", alpha = 0.2, linewidth = 0.6) +
  geom_text(data = LL, aes(x = Inf, y = Inf, label = txt), hjust = 1.05, vjust = 1.4, size = 2.6, inherit.aes = FALSE) +
  facet_grid(window ~ response, scales = "free_y") +
  labs(x = "Site maximum of relative mortality (%, mean of 100-500 m buffers)", y = ylab, title = ttl) + th
p1 <- mk("lr", "log(MAE C+T+M / MAE C+T)\n(below 0 = mortality helps)", L1, "1b  Error reduction vs mortality (per site)")
S2 <- rbind(S[, .(response, window, tmax, y = res3, model = "C+T (without mortality)")],
            S[, .(response, window, tmax, y = res4, model = "C+T+M (with mortality)")])
MC2 <- c("C+T (without mortality)" = "#22C4E0", "C+T+M (with mortality)" = "#E8257A")
p2 <- ggplot(S2, aes(tmax, y, colour = model, fill = model)) +
  geom_hline(yintercept = 0, linetype = "dashed", colour = "grey50") +
  geom_point(size = 1, alpha = 0.55) +
  geom_smooth(method = "lm", formula = y ~ x, alpha = 0.15, linewidth = 0.7) +
  geom_text(data = L2, aes(x = Inf, y = Inf, label = txt), hjust = 1.05, vjust = 1.2, size = 2.4, lineheight = 0.9, inherit.aes = FALSE) +
  scale_colour_manual(values = MC2, name = NULL) + scale_fill_manual(values = MC2, name = NULL) +
  ggh4x::facet_grid2(window ~ response, scales = "free_y", independent = "y") +
  labs(x = "Site maximum of relative mortality (%, mean of 100-500 m buffers)", y = "Mean residual per site\n(observed - predicted)",
       title = "2b  Residual vs mortality, per site: without (C+T) and with mortality (C+T+M)") +
  th + theme(legend.position = "bottom")
suppressMessages(library(patchwork))
ggsave(file.path(OUT, sprintf("dose_response_%s.png", L)), p1 / p2, width = 11, height = 8.5, dpi = 250, bg = "white")

# ---- figure: site-year tests 1a and 2a (points = site-years, line = fixed effect of the mixed model) ----
# y axis zoomed to the 1st-99th percentile per panel (no data removed; the fit uses every site-year)
star <- function(q) fifelse(q < 0.001, "***", fifelse(q < 0.01, "**", fifelse(q < 0.05, "*", " ns")))
W <- copy(w)[, response := factor(response, EFP_ORDER)]
xs <- data.table(tm = seq(0, max(W$tm), length.out = 50))
fitline <- function(int, slope) xs[, .(tm, y = int + slope * tm / 10)]
F1 <- R[, fitline(t1a_int, t1a_slope), by = .(response, window)][, response := factor(response, EFP_ORDER)]
T1 <- R[, .(response = factor(response, EFP_ORDER), window,
            txt = sprintf("%+.3f per 10 %%%s", t1a_slope, star(t1a_q)))]
lim1 <- W[, .(lo = quantile(d, 0.01), hi = quantile(d, 0.99)), by = response]
pa <- ggplot(W, aes(tm, d)) +
  geom_hline(yintercept = 0, linetype = "dashed", colour = "grey50") +
  geom_point(size = 0.8, alpha = 0.45, colour = "#3B6FB6") +
  geom_line(data = F1, aes(tm, y), colour = "#C2571A", linewidth = 0.9) +
  geom_text(data = T1, aes(x = Inf, y = Inf, label = txt), hjust = 1.05, vjust = 1.4, size = 2.6, inherit.aes = FALSE) +
  geom_blank(data = lim1, aes(x = 0, y = lo), inherit.aes = FALSE) + geom_blank(data = lim1, aes(x = 0, y = hi), inherit.aes = FALSE) +
  ggh4x::facet_grid2(window ~ response, scales = "free_y", independent = "y") +
  ggh4x::facetted_pos_scales(y = rep(lapply(EFP_ORDER, function(e) scale_y_continuous(limits = unlist(lim1[response == e, .(lo, hi)]), oob = scales::oob_keep)), length(WINDOWS))) +
  labs(x = "Relative mortality of the site-year (%, mean of 100-500 m buffers)",
       y = "|error C+T+M| - |error C+T|\n(below 0 = mortality helps)",
       title = "1a  Error change vs mortality, every site-year (mixed model, random intercept per site)") + th
F2 <- rbind(R[, fitline(t2a_int_CT, t2a_slope_CT), by = .(response, window)][, model := "C+T (without mortality)"],
            R[, fitline(t2a_int_CTM, t2a_slope_CTM), by = .(response, window)][, model := "C+T+M (with mortality)"])
F2[, response := factor(response, EFP_ORDER)]
T2 <- R[, .(response = factor(response, EFP_ORDER), window,
            txt = sprintf("C+T %+.3f%s\nC+T+M %+.3f%s", t2a_slope_CT, star(t2a_q_CT), t2a_slope_CTM, star(t2a_q_CTM)))]
W2 <- rbind(W[, .(response, window, tm, y = r3, model = "C+T (without mortality)")],
            W[, .(response, window, tm, y = r4, model = "C+T+M (with mortality)")])
lim2 <- W2[, .(lo = quantile(y, 0.01), hi = quantile(y, 0.99)), by = response]
MC <- c("C+T (without mortality)" = "#22C4E0", "C+T+M (with mortality)" = "#E8257A")
pb <- ggplot(W2, aes(tm, y, colour = model)) +
  geom_hline(yintercept = 0, linetype = "dashed", colour = "grey50") +
  geom_point(size = 0.7, alpha = 0.3) +
  geom_line(data = F2, aes(tm, y, colour = model), linewidth = 1) +
  geom_text(data = T2, aes(x = Inf, y = Inf, label = txt), hjust = 1.05, vjust = 1.2, size = 2.4, lineheight = 0.9, inherit.aes = FALSE) +
  scale_colour_manual(values = MC, name = NULL) +
  ggh4x::facet_grid2(window ~ response, scales = "free_y", independent = "y") +
  ggh4x::facetted_pos_scales(y = rep(lapply(EFP_ORDER, function(e) scale_y_continuous(limits = unlist(lim2[response == e, .(lo, hi)]), oob = scales::oob_keep)), length(WINDOWS))) +
  labs(x = "Relative mortality of the site-year (%, mean of 100-500 m buffers)", y = "Residual (observed - predicted)",
       title = "2a  Residual vs mortality, every site-year: the model without mortality over-predicts where mortality is high") +
  th + theme(legend.position = "bottom")
ggsave(file.path(OUT, sprintf("dose_response_siteyear_%s.png", L)), pa / pb + plot_layout(heights = c(1, 1.1)),
       width = 11, height = 9.5, dpi = 250, bg = "white")

show <- R[, .(response, window,
  `1a slope` = sprintf("%+.3f%s", t1a_slope, fifelse(t1a_q < 0.05, "*", "")),
  `1b rho` = sprintf("%+.2f%s", t1b_rho, fifelse(t1b_q < 0.05, "*", "")),
  `2a C+T` = sprintf("%+.3f%s", t2a_slope_CT, fifelse(t2a_q_CT < 0.05, "*", "")),
  `2a C+T+M` = sprintf("%+.3f%s", t2a_slope_CTM, fifelse(t2a_q_CTM < 0.05, "*", "")),
  `2b rho C+T` = sprintf("%+.2f%s", t2b_rho_CT, fifelse(t2b_q_CT < 0.05, "*", "")),
  `2b rho C+T+M` = sprintf("%+.2f%s", t2b_rho_CTM, fifelse(t2b_q_CTM < 0.05, "*", "")))]
print(show)
cat("DOSE_DONE\n")
