#!/usr/bin/env Rscript
# ============================================================================
# Quick check (2026-10-21, after T. Musavi's comment that ecosystem respiration hardly changes after mortality):
# does growing-season ecosystem respiration change with relative mortality within sites, compared with GPP and NEP?
# No machine learning: per site-year growing-season means from the FLUXNET daily files (night-time partitioning,
# RECO_NT_VUT_REF, GPP_NT_VUT_REF, NEE_VUT_REF; NEP = -NEE), same 101 sites / 361 site-years and growing seasons as the
# paper (nepmax90_gs_by_site_year.csv). Model: y ~ relative mortality of the site-year (per +10 %) + (1 | site), so the
# slope is mostly a within-site change; checked with a site-block bootstrap (1000 draws). Also as % of the site mean.
# Output: plots/paper_v5_nothresh/reco_check/reco_mortality_check.csv (+ site-year table)
# ============================================================================
suppressMessages({library(data.table); library(nlme)})
setwd(Sys.getenv("PAPER_ROOT", "."))
set.seed(20261021)
OUT <- "plots/paper_v5_nothresh/reco_check"; dir.create(OUT, showWarnings = FALSE, recursive = TRUE)
ds <- fread("derived_tables/paper_v5/paper_dataset_full_lags_monthly_tc30_minrun2_buf100-200-300-400-500.csv")
ds[, tm := rowMeans(.SD), .SDcols = paste0("relative_disturbance_", c(100, 200, 300, 400, 500), "m")]
sy <- unique(ds[, .(SITE_ID, YEAR, tm)])
gs <- fread("derived_tables/paper_v41/nepmax90_gs_by_site_year.csv")[, .(SITE_ID, YEAR, gs_start_doy, gs_end_doy)]
sy <- merge(sy, gs, by = c("SITE_ID", "YEAR"), all.x = TRUE)

# ---- growing-season means from the daily files ---------------------------------------------------------------------
zips <- list.files("fluxnet_2017_2025_V02", pattern = "_FLUXNET_.*\\.zip$", full.names = TRUE)
V <- c("RECO_NT_VUT_REF", "GPP_NT_VUT_REF", "NEE_VUT_REF")
read_dd <- function(site) {
  zs <- zips[grepl(paste0("_", site, "_FLUXNET_"), basename(zips))]
  rbindlist(lapply(zs, function(z) {
    f <- grep("_FLUXMET_DD_", unzip(z, list = TRUE)$Name, value = TRUE)[1]
    if (is.na(f)) return(NULL)
    d <- fread(cmd = sprintf("unzip -p '%s' '%s'", z, f), select = c("TIMESTAMP", V))
    d[, (V) := lapply(.SD, function(x) fifelse(x <= -9990, NA_real_, x)), .SDcols = V][, src := basename(z)]
  }), fill = TRUE)
}
res <- rbindlist(lapply(unique(sy$SITE_ID), function(s) {
  d <- read_dd(s); if (is.null(d) || !nrow(d)) return(NULL)
  d[, `:=`(YEAR = as.integer(substr(TIMESTAMP, 1, 4)), doy = yday(as.IDate(as.character(TIMESTAMP), format = "%Y%m%d")))]
  d <- unique(d, by = "TIMESTAMP")                                     # several products of one site: keep one row per day
  x <- merge(d, sy[SITE_ID == s], by = "YEAR")[doy >= gs_start_doy & doy <= gs_end_doy]
  x[, .(RECO = mean(RECO_NT_VUT_REF, na.rm = TRUE), GPP = mean(GPP_NT_VUT_REF, na.rm = TRUE),
        NEP = mean(-NEE_VUT_REF, na.rm = TRUE), n_days = .N), by = .(SITE_ID, YEAR, tm)]
}))
fwrite(res, file.path(OUT, "reco_gpp_nep_gs_by_site_year.csv"))
cat(sprintf("site-years with growing-season fluxes: %d of %d (%d sites)\n", nrow(res), nrow(sy), uniqueN(res$SITE_ID)))

# ---- within-site change with relative mortality -------------------------------------------------------------------
fit <- function(y, x, g) { m <- tryCatch(lme(y ~ x, random = ~ 1 | g, control = lmeControl(opt = "optim")), error = function(e) NULL)
  if (is.null(m)) return(c(NA, NA)); tt <- summary(m)$tTable; c(tt["x", "Value"], tt["x", "p-value"]) }
R <- rbindlist(lapply(c("RECO", "GPP", "NEP"), function(v) {
  x <- res[is.finite(get(v)) & is.finite(tm)]
  x[, rel := 100 * (get(v) / mean(get(v)) - 1), by = SITE_ID]          # % of the site's mean
  a <- fit(x[[v]], x$tm / 10, x$SITE_ID); r <- fit(x$rel, x$tm / 10, x$SITE_ID)
  idx <- split(seq_len(nrow(x)), x$SITE_ID); k <- length(idx)
  B <- replicate(1000, { dr <- sample(k, replace = TRUE); ii <- unlist(idx[dr]); g <- rep(seq_along(dr), lengths(idx[dr]))
    c(fit(x[[v]][ii], x$tm[ii] / 10, g)[1], fit(x$rel[ii], x$tm[ii] / 10, g)[1]) })
  ba <- B[1, ]; b <- B[2, ]
  data.table(flux = v, n_sy = nrow(x), n_sites = k, mean_gC_m2_d = mean(x[[v]]),
             slope_gC_m2_d_per10 = a[1], p_lme = a[2], boot_lo = quantile(ba, 0.025, na.rm = TRUE),
             boot_hi = quantile(ba, 0.975, na.rm = TRUE), p_boot = min(1, 2 * min(mean(ba <= 0, na.rm = TRUE), mean(ba >= 0, na.rm = TRUE))), slope_pct_per10 = r[1], p_lme_pct = r[2],
             boot_lo_pct = quantile(b, 0.025, na.rm = TRUE), boot_hi_pct = quantile(b, 0.975, na.rm = TRUE),
             p_boot_pct = min(1, 2 * min(mean(b <= 0, na.rm = TRUE), mean(b >= 0, na.rm = TRUE))))
}))
# High-mortality years (> 24 % relative mortality) against the same site's other years, % of the site mean
hi <- rbindlist(lapply(c("RECO", "GPP", "NEP"), function(v) {
  x <- res[is.finite(get(v))][, rel := 100 * (get(v) / mean(get(v)) - 1), by = SITE_ID]
  x <- x[SITE_ID %in% x[tm > 24, unique(SITE_ID)] & SITE_ID %in% x[tm <= 24, unique(SITE_ID)]]
  d <- x[, .(diff = mean(rel[tm > 24]) - mean(rel[tm <= 24])), by = SITE_ID]
  data.table(flux = v, sites_with_both = nrow(d), high_minus_other_pct = mean(d$diff),
             p_wilcox = if (nrow(d) > 2) wilcox.test(d$diff)$p.value else NA_real_)
}))
R <- merge(R, hi, by = "flux")
fwrite(R, file.path(OUT, "reco_mortality_check.csv"))
options(width = 200)
print(R[, .(flux, n_sy, n_sites, mean = round(mean_gC_m2_d, 2), slope = signif(slope_gC_m2_d_per10, 2), p_lme = signif(p_lme, 2),
            boot_abs = sprintf("[%.3f, %.3f]", boot_lo, boot_hi), p_boot_abs = p_boot,
            pct_per10 = round(slope_pct_per10, 2), boot_pct = sprintf("[%.2f, %.2f]", boot_lo_pct, boot_hi_pct), p_boot_pct = p_boot_pct,
            high_sites = sites_with_both, high_vs_other_pct = round(high_minus_other_pct, 1), p_wilcox = signif(p_wilcox, 2))])
