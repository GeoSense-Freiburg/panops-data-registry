#!/usr/bin/env Rscript
# ============================================================================
# Mortality-block split with the drop-column test (2026-10-08). For each mortality family:
#   A  SHAP share   - share of the mortality block's mean |SHAP| (C+T+M), XGBoost, LightGBM, AutoGluon
#   B  unique       - % increase of RMSE when the family is removed from C+T+M (37_mortality_dropcol.R)
#   C  alone        - % decrease of RMSE when only this family is added to C+T
# per EFP, mean of the 12 and 24 month windows (both windows in the table). A large SHAP share with a small
# unique contribution = the family is used but replaceable by correlated families.
# Output: plots/<sub>/mortality_split/mortality_block_dropcol.png/.pdf, mortality_block_dropcol.csv
#   PAPER_RESP_SET=efp_v5 PAPER_NO_THRESH=1 Rscript 38_mortality_block_summary.R
# ============================================================================
if (Sys.getenv("PAPER_RESP_SET") == "") Sys.setenv(PAPER_RESP_SET = "efp_v5", PAPER_NO_THRESH = "1")
suppressMessages({library(data.table); library(ggplot2); library(patchwork)})
setwd("/mnt/gsdata/projects/panops/panops-data-registry/data/flux")
source("scripts/paper_v41/paper_common.R")
OUT <- file.path(PAPER_FIG, "mortality_split"); dir.create(OUT, showWarnings = FALSE, recursive = TRUE)
PN <- paper_n()
FAMILIES <- c(absolute_mortality = "Absolute deadwood", relative_mortality = "Relative deadwood",
              new_deadwood_gain_pp = "Deadwood gain", relative_tree_loss_pct = "Relative tree-cover loss",
              relative_disturbance = "Relative mortality")
fam_of <- function(v) sub("_[0-9]+m(_lag[12])?$", "", v)
LEARN <- c(XGB = "XGBoost", LGB = "LightGBM", AG = "AutoGluon")
LCOL <- c(XGBoost = "#3B6FB6", LightGBM = "#2E9E6A", AutoGluon = "#C2571A")
AG_DIR <- file.path(PAPER_ROOT, "AutoGluon/outputs", if (PAPER_RESP_SET == "efp_v5") "efp_v5" else "efp")
if (PAPER_NO_THRESH) AG_DIR <- paste0(AG_DIR, "_nothresh")

# ---- A. SHAP share of each family within the mortality block (per site, then mean over sites) ----
shap_share <- function(f, lab) {
  if (!file.exists(f)) return(NULL)
  s <- fread(f)[group == "Disturbance" & response %in% EFP_ORDER]
  s[, family := fam_of(variable)]
  s <- s[family %in% names(FAMILIES), .(v = sum(mean_abs_shap)), by = .(model, response, test_site, family)]
  s[, share := 100 * v / sum(v), by = .(model, response, test_site)]
  s[, .(value = mean(share)), by = .(model, response, family)][, learner := lab]
}
A <- rbindlist(c(lapply(c("XGB", "LGB"), function(L) shap_share(file.path(PAPER_OUT, sprintf("%s_site_shap_M4.csv", L)), LEARN[[L]])),
                 list(shap_share(file.path(AG_DIR, "AG_site_shap_M4.csv"), "AutoGluon"))))
A[, window := sub("M4_", "", model)][, measure := "A  SHAP share in the mortality block (%)"]

# ---- B, C. drop-column ----
D <- rbindlist(lapply(c("XGB", "LGB"), function(L) {
  f <- file.path(dirname(PAPER_OUT), "dropcol", sprintf("%s_dropcol_rmse.csv", L)); if (file.exists(f)) fread(f) }))
w <- dcast(D, learner + window + response ~ config, value.var = "overall_rmse")
B <- rbindlist(lapply(names(FAMILIES), function(f) w[, .(learner, window, response, family = f,
        unique = 100 * (get(paste0("minus:", f)) / `C+T+M` - 1), alone = 100 * (1 - get(paste0("only:", f)) / `C+T`),
        full_gain = 100 * (1 - `C+T+M` / `C+T`))]))
B[, learner := LEARN[learner]]
tab <- merge(B, A[, .(learner, window, response, family, shap_share = value)], by = c("learner", "window", "response", "family"), all = TRUE)
tab[, family_label := FAMILIES[family]]
setcolorder(tab, c("learner", "window", "response", "family_label"))
fwrite(tab[order(response, window, learner, family)], file.path(OUT, "mortality_block_dropcol.csv"))

long <- rbind(A[, .(learner, window, response, family, measure, value)],
              B[, .(learner, window, response, family, measure = "B  Unique: RMSE increase when removed (%)", value = unique)],
              B[, .(learner, window, response, family, measure = "C  Alone: RMSE decrease vs C+T (%)", value = alone)])
long <- long[, .(value = mean(value), lo = min(value), hi = max(value)), by = .(learner, response, family, measure)]   # mean of windows
long[, `:=`(family = factor(FAMILIES[family], levels = rev(FAMILIES)), response = factor(response, levels = EFP_ORDER),
            learner = factor(learner, levels = LEARN))]
th <- theme_bw(base_size = 9) + theme(panel.grid.minor = element_blank(), panel.grid.major.y = element_blank(),
  strip.background = element_rect(fill = "#EFEFEF", colour = "grey80"), strip.text = element_text(face = "bold", size = 8.5),
  legend.position = "bottom", legend.title = element_blank(), plot.title = element_text(face = "bold", size = 12),
  plot.subtitle = element_text(size = 8, colour = "grey30"))
p <- ggplot(long, aes(value, family, fill = learner)) +
  geom_vline(xintercept = 0, colour = "grey40", linewidth = 0.3) +
  geom_col(position = position_dodge(width = 0.8), width = 0.75) +
  geom_errorbar(aes(xmin = lo, xmax = hi), orientation = "y", position = position_dodge(width = 0.8), width = 0, linewidth = 0.3, colour = "grey25") +
  scale_fill_manual(values = LCOL) +
  facet_grid(response ~ measure, scales = "free_x", labeller = labeller(response = RESP_UNITS)) +
  labs(x = NULL, y = NULL, title = "Mortality block: SHAP share vs drop-column contribution",
       subtitle = sprintf(paste0("%s · %d sites / %d site-years · bars = mean of the 12 and 24 month windows, lines = range of the two\n",
                                 "A: share of the mortality block's mean |SHAP| (C+T+M) · B: unique information (RMSE up when the family is removed)\n",
                                 "C: value on its own (RMSE down when only this family is added to C+T) · ",
                                 "B and C retrained with the same tuned settings and folds (XGBoost, LightGBM); AutoGluon only in A"),
                          PAPER_MAPS, PN[["sites"]], PN[["sy"]])) + th
stem <- file.path(OUT, "mortality_block_dropcol")
ggsave(paste0(stem, ".png"), p, width = 11, height = 9, dpi = 250, bg = "white")
ggsave(paste0(stem, ".pdf"), p, width = 11, height = 9, bg = "white", device = cairo_pdf)
print(tab[, .(shap_share = round(mean(shap_share, na.rm = TRUE), 1), unique = round(mean(unique, na.rm = TRUE), 2),
              alone = round(mean(alone, na.rm = TRUE), 2)), by = .(learner, family_label)][order(learner, -shap_share)])
cat("SUMMARY_DONE\n")
