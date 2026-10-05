#!/usr/bin/env Rscript
# ============================================================================
# Paper dataset (deadtrees v4.1), step 03 - assemble EFPs + mortality + climate (+ traits).
# Order (user, 2026-09-25): EFPs (GPPsat, NEPmax, ETmax, WUE; >= 4 continuous years) -> join
# mortality and climate for the same site-years -> check 4 continuous years -> add lag 1/2 for
# climate and mortality -> drop NAs -> report -> tree cover >= 30 % -> traits.
# Inputs (derived_tables/paper_v41/): mortality_v41_multibuffer.csv (step 01),
#   nepmax90_gs_by_site_year.csv + monthly_meteo_2015_2025.csv (step 02),
#   traits_lusk1km_500m_by_site.csv (step 04); GPPsat/ETmax/WUE from yearly_EFP_per_site/.
# Climate lags are STRICTLY PRE-SEASON: lag1 = the 12 months ending the month before the
# growing-season start month of year Y (M01 = oldest, M12 = last month before the season);
# lag2 = the 12 months before that. Mortality lags: lag1 = Y-1, lag2 = Y-2.
# Two climate versions: (1) monthly mean/p05/p95 (TA, VPD, SW_IN) + P_sum per month,
# (2) annual extremes per lag window (mean, max of monthly p95, min of monthly p05, P total,
# driest month, seasonality) - René's suggestion at the 2026-09-14 meeting.
# ============================================================================
suppressMessages(library(data.table))
setwd("/mnt/gsdata/projects/panops/panops-data-registry/data/flux")
D <- "derived_tables/paper_v41"
# scenario switches (defaults = the agreed design). COUNTS_ONLY=1 prints counts and writes nothing.
MIN_RUN <- as.integer(Sys.getenv("PAPER_MIN_RUN", "4"))            # min continuous EFP years
BUFS <- as.integer(strsplit(Sys.getenv("PAPER_BUFFERS", "100,200,300,400,500"), ",")[[1]])
COUNTS_ONLY <- Sys.getenv("COUNTS_ONLY", "0") == "1"
SUF <- if (MIN_RUN == 4 && length(BUFS) == 5) "" else sprintf("_minrun%d_buf%s", MIN_RUN, paste(BUFS, collapse = "-"))  # default names = original 4-year build
log_lines <- character(0)
say <- function(...) { s <- sprintf(...); cat(s, "\n"); log_lines <<- c(log_lines, s) }
count <- function(dt, label) say("%-62s %4d sites | %5d site-years", label, uniqueN(dt$SITE_ID), nrow(dt))
runs4 <- function(dt, key_years = "YEAR") {          # keep years that sit in a run of >= 4 consecutive years
  setorder(dt, SITE_ID, YEAR)
  dt[, run := cumsum(c(1, diff(YEAR) != 1)), by = SITE_ID]
  dt[, run_len := .N, by = .(SITE_ID, run)]
  out <- dt[run_len >= MIN_RUN][, c("run", "run_len") := NULL]
  dt[, c("run", "run_len") := NULL]
  out
}

# ---- 1. EFPs ------------------------------------------------------------------
efp_files <- list.files("fluxnet_2017_2025_V02/EFP_outputs_corrected/yearly_EFP_per_site", full.names = TRUE)
efp <- rbindlist(lapply(efp_files, function(f) fread(f, select = c("SITE_ID", "YEAR", "GPPsat", "ETmax", "WUE", "status"))), fill = TRUE)
efp <- efp[status == "ok", .(SITE_ID, YEAR, GPPsat, ETmax, WUE)]
nep <- fread(file.path(D, "nepmax90_gs_by_site_year.csv"))
gs <- nep[, .(SITE_ID, YEAR, gs_start_doy, gs_end_doy, n_gs_days)]
efp <- merge(efp, nep[, .(SITE_ID, YEAR, NEPmax = NEPmax90_gs_all)], by = c("SITE_ID", "YEAR"))
efp <- efp[YEAR >= 2017 & is.finite(GPPsat) & is.finite(NEPmax) & is.finite(ETmax) & is.finite(WUE)]
say("== Stage counts ==")
count(efp, "EFPs: all four available (2017-2025)")
# Quality control carried over from the June/July 2026 data-quality checks (scripts/fix_flagged_sites.py,
# scripts/prepare_rf_v3_harmonized.py; daily_reports/data_quality_report.md), re-applied 2026-10-02:
#   CN-Sb1 / CN-Sb2: extreme WUE (12-34 vs a median near 3.5, z = 6-14) - removed for every EFP so all
#   responses share the same site-years; negative WUE is not physical - that site-year is removed.
QC_SITES <- c("CN-Sb1", "CN-Sb2")
efp <- efp[!SITE_ID %in% QC_SITES & WUE >= 0]
count(efp, "+ QC: without CN-Sb1 / CN-Sb2 and negative-WUE site-years")
efp4 <- runs4(copy(efp))
count(efp4, sprintf("EFPs: in a run of >= %d continuous years", MIN_RUN))

# ---- 2. mortality (current year) + climate availability ------------------------
mort <- fread(file.path(D, "mortality_v41_multibuffer.csv"))
MVARS <- c("absolute_mortality", "relative_mortality", "new_deadwood_gain_pp", "new_deadwood_gain_pp_thresh",
           "relative_tree_loss_pct", "relative_tree_loss_pct_thresh", "relative_disturbance")
mcols <- as.vector(outer(MVARS, paste0("_", BUFS, "m"), paste0))
met <- fread(file.path(D, "monthly_meteo_2015_2025.csv"))
CV <- c("TA_mean", "TA_p05", "TA_p95", "VPD_mean", "VPD_p05", "VPD_p95", "SW_IN_mean", "SW_IN_p05", "SW_IN_p95", "P_sum")
met <- met[is.finite(TA_mean) & is.finite(VPD_mean) & is.finite(SW_IN_mean) & is.finite(P_sum)]
met[, midx := YEAR * 12L + MONTH - 1L]

base <- merge(efp4, gs, by = c("SITE_ID", "YEAR"))
base <- merge(base, mort[, c("SITE_ID", "YEAR", "tree_cover_mean_pct_500m", mcols), with = FALSE], by = c("SITE_ID", "YEAR"))
count(base, "+ v4.1 mortality exists for the site-year (current year)")
base <- base[is.finite(gs_start_doy)]
count(base, "+ growing-season start known")

# climate lag windows
base[, gs_month := as.integer(format(as.Date(gs_start_doy - 1, origin = sprintf("%d-01-01", YEAR)), "%m"))]
base[, end1 := YEAR * 12L + gs_month - 2L]          # last month before the season (midx)
win <- function(b, lag) {
  rows <- b[, .(midx = (end1 - 12L * lag + 1L - 11L):(end1 - 12L * lag + 1L) - 0L), by = .(SITE_ID, YEAR)]
  rows[, m := rep(1:12, .N / 12)]
  x <- merge(rows, met[, c("SITE_ID", "midx", CV), with = FALSE], by = c("SITE_ID", "midx"), all.x = TRUE)
  complete <- x[, .(ok = all(is.finite(TA_mean) & is.finite(P_sum))), by = .(SITE_ID, YEAR)]
  list(x = x, complete = complete)
}
w1 <- win(base, 0L); w2 <- win(base, 1L)           # lag1 window ends at end1; lag2 is 12 months earlier
monthly_wide <- function(w, tag) {
  long <- melt(w$x, id.vars = c("SITE_ID", "YEAR", "m"), measure.vars = CV)
  long[, col := sprintf("%s_M%02d_%s", tag, m, variable)]
  dcast(long, SITE_ID + YEAR ~ col, value.var = "value")
}
extremes <- function(w, tag) {
  w$x[, .(TA_mean = mean(TA_mean), TA_max = max(TA_p95), TA_min = min(TA_p05), TA_seasonality = sd(TA_mean),
          VPD_mean = mean(VPD_mean), VPD_max = max(VPD_p95),
          SW_IN_mean = mean(SW_IN_mean), SW_IN_max = max(SW_IN_p95),
          P_total = sum(P_sum), P_driest_month = min(P_sum), P_seasonality = sd(P_sum) / mean(P_sum)),
      by = .(SITE_ID, YEAR)] -> e
  setnames(e, setdiff(names(e), c("SITE_ID", "YEAR")), paste0(tag, "_", setdiff(names(e), c("SITE_ID", "YEAR"))))
  e
}
cl_ok <- merge(w1$complete[, .(SITE_ID, YEAR, ok1 = ok)], w2$complete[, .(SITE_ID, YEAR, ok2 = ok)], by = c("SITE_ID", "YEAR"))

# ---- 3. same site-years for all three, then 4 continuous years -------------------
key <- merge(base, cl_ok[ok1 == TRUE], by = c("SITE_ID", "YEAR"))
count(key, "+ lag-1 climate window complete")
key4 <- runs4(copy(key[, .(SITE_ID, YEAR)]))
count(key4, sprintf("EFP + mortality + climate on the same site-years, >= %d continuous", MIN_RUN))

# ---- 4. lags for mortality, then NA cleaning -------------------------------------
mt <- mort[, c("SITE_ID", "YEAR", mcols), with = FALSE]
lagm <- function(k) { x <- copy(mt); x[, YEAR := YEAR + k]; setnames(x, mcols, paste0(mcols, "_lag", k)); x }
full <- merge(base, lagm(1), by = c("SITE_ID", "YEAR"), all.x = TRUE)
full <- merge(full, lagm(2), by = c("SITE_ID", "YEAR"), all.x = TRUE)
full <- merge(full, cl_ok, by = c("SITE_ID", "YEAR"), all.x = TRUE)
full <- full[SITE_ID %in% key4$SITE_ID]
EFP <- c("GPPsat", "NEPmax", "ETmax", "WUE")
lag2_stock <- paste0(as.vector(outer(c("absolute_mortality", "relative_mortality"), paste0("_", BUFS, "m"), paste0)), "_lag2")
pred_full <- c(mcols, paste0(mcols, "_lag1"), paste0(mcols, "_lag2"))
pred_stock <- c(mcols, paste0(mcols, "_lag1"), lag2_stock)
clean_full <- full[ok1 %in% TRUE & ok2 %in% TRUE & complete.cases(full[, c(EFP, pred_full), with = FALSE])]
clean_stock <- full[ok1 %in% TRUE & ok2 %in% TRUE & complete.cases(full[, c(EFP, pred_stock), with = FALSE])]
say("")
say("== After lag 1/2 and NA cleaning ==")
count(clean_full, "A) all mortality metrics with lag 1 and lag 2")
say("     years present: %s", paste(sort(unique(clean_full$YEAR)), collapse = ", "))
count(runs4(copy(clean_full[, .(SITE_ID, YEAR)])), sprintf("   ... of which in a run of >= %d continuous years", MIN_RUN))
count(clean_stock, "B) lag 2 only for stock metrics (abs./rel. mortality), as V10")
say("     years present: %s", paste(sort(unique(clean_stock$YEAR)), collapse = ", "))
count(runs4(copy(clean_stock[, .(SITE_ID, YEAR)])), sprintf("   ... of which in a run of >= %d continuous years", MIN_RUN))

# ---- 5. tree cover >= 30 % (500 m, site mean over 2017-2025, v4.1) ---------------
tcm <- mort[, .(tc_site_mean_500m = mean(tree_cover_mean_pct_500m, na.rm = TRUE)), by = SITE_ID]
say("")
say("== Tree cover >= 30 %% (500 m buffer, site mean of v4.1 tree cover) ==")
for (nm in c("clean_full", "clean_stock")) {
  x <- merge(get(nm), tcm, by = "SITE_ID")
  count(x[tc_site_mean_500m >= 30], sprintf("%s, tree cover >= 30 %%", nm))
}

if (COUNTS_ONLY) { cat("COUNTS_ONLY_DONE\n"); quit(save = "no") }
# ---- 6. traits + write outputs --------------------------------------------------
tr <- fread(file.path(D, "traits_lusk1km_500m_by_site.csv"))
trait_cols <- setdiff(names(tr), c("SITE_ID", grep("^aoa_", names(tr), value = TRUE)))
meta <- fread("fluxnet_2017_2025_V02/fluxnet_site_metadata_clean_with_elevation.csv", select = c("SITE_ID", "IGBP", "LOCATION_LAT", "LOCATION_LONG"))
build <- function(x, climate) {
  x <- merge(x, tcm, by = "SITE_ID"); x <- merge(x, meta, by = "SITE_ID", all.x = TRUE)
  x <- merge(x, tr, by = "SITE_ID", all.x = TRUE)
  clim <- if (climate == "monthly") merge(monthly_wide(w1, "lag1"), monthly_wide(w2, "lag2"), by = c("SITE_ID", "YEAR"))
          else merge(extremes(w1, "lag1"), extremes(w2, "lag2"), by = c("SITE_ID", "YEAR"))
  merge(x, clim, by = c("SITE_ID", "YEAR"))
}
say("")
say("== Traits (Lusk 1 km, area-weighted 500 m) ==")
for (variant in c("full", "stock")) {
  x <- get(paste0("clean_", variant))
  for (climate in c("monthly", "annual_extremes")) {
    all_sites <- build(x, climate)
    drop <- c("ok1", "ok2", "end1")
    all_sites[, (intersect(drop, names(all_sites))) := NULL]
    tc30 <- all_sites[tc_site_mean_500m >= 30]
    tc30_tr <- tc30[complete.cases(tc30[, ..trait_cols])]
    fwrite(tc30, file.path(D, sprintf("paper_dataset_%s_lags_%s_tc30%s.csv", variant, climate, SUF)))
    if (climate == "monthly")
      count(tc30_tr, sprintf("%s lags, tc >= 30, all %d traits non-NA", variant, length(trait_cols)))
  }
}
writeLines(log_lines, file.path(D, sprintf("paper_dataset_counts%s.txt", SUF)))
cat("STEP03_DONE\n")
