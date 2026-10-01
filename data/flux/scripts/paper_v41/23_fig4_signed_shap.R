# PORT for the paper_v41 dataset (deadtrees v4.1, 106 sites): input/output paths and site-count labels only.
#!/usr/bin/env Rscript
# ============================================================================
# Coauthor draft — Figure 4: direction of the disturbance effect (signed SHAP)
#
# Layout matches Figure 2: EFPs down the left, learners across the top,
# 12 m and 24 m on the x axis. One figure per disturbance metric.
#
#   negative = the model predicts a LOWER value of the EFP (not worse accuracy)
#
# Significance is a Mann-Whitney U test (Wilcoxon rank-sum) between the Low+Mid
# and High groups. It compares two independent samples of UNEQUAL size (n = 64
# vs 29 here), needs no normality, and works on ranks. The previous version used
# a one-sample test against zero, which never actually compared the two groups.
# ============================================================================
suppressMessages({library(data.table); library(ggplot2); library(ggh4x)})
setwd("/mnt/gsdata/projects/panops/panops-data-registry/data/flux")
source("scripts/paper_v41/paper_common.R")
OUT <- PAPER_FIG; dir.create(OUT, showWarnings = FALSE, recursive = TRUE)

BG <- "white"; PANEL <- "white"; GRID <- "#D9D9D9"; TXT <- "#111111"; AX <- "#444444"
# class colours on the Fig 3b teal -> yellow -> pink scale (colour-vision-deficiency checked: CIEDE2000 >= 28
# between the two under simulated deutan / protan / tritan)
CAT_COLS <- c("Low+Mid" = "#B5CC45", "High" = "#EE6FA8")
EFP_LAB <- RESP_UNITS   # active response set (paper_common.R)
PN <- paper_n()
# 2026-09-09: repointed to true_24m/ - see plot_draft_fig2_M3M4.R for why.
LEARNERS <- list(list(lab="XGBoost (Optuna)",  fam="XGB"),
                 list(lab="LightGBM (Optuna)", fam="LGB"))

d <- rbindlist(lapply(LEARNERS, function(L) {
  f <- file.path(PAPER_OUT, sprintf("%s_site_signed_shap_M4.csv", L$fam))
  if (!file.exists(f)) { message("missing: ", f); return(NULL) }
  fread(f)[, learner := L$lab][]
}))
d <- d[group == "Disturbance" & response %in% EFP_ORDER]
d[, learner := factor(learner, levels = sapply(LEARNERS, `[[`, "lab"))]
d[, window  := factor(sub("M4_","",model), levels = c("12m","24m"))]

# natural-breaks categories, one metric per figure
h <- as.data.frame(fread(PAPER_DATA))
nb <- function(v){m<-mean(v,na.rm=TRUE); s<-sd(v,na.rm=TRUE)
  factor(cut(v, c(-Inf, m-0.5*s, m+0.5*s, Inf), c("Low","Mid","High")), levels=c("Low","Mid","High"))}
METRICS <- list(
  list(key="relmort", col="relative_mortality_500m",   lab="relative mortality"),
  list(key="reldist", col="relative_disturbance_500m", lab="relative disturbance"))

th <- theme_bw(base_size=11) + theme(
  plot.background=element_rect(fill=BG,colour=NA), panel.background=element_rect(fill=PANEL,colour=NA),
  panel.border=element_rect(colour=GRID,fill=NA,linewidth=0.4),
  panel.grid.major=element_line(colour=GRID,linewidth=0.2), panel.grid.minor=element_blank(),
  strip.background=element_rect(fill="#EFEFEF",colour=GRID),
  strip.text=element_text(colour=TXT,size=10,face="bold"),
  axis.text=element_text(colour=AX,size=9), axis.title=element_text(colour=AX,size=10,face="bold"),
  legend.background=element_rect(fill=NA,colour=NA), legend.key=element_rect(fill=NA,colour=NA),
  legend.text=element_text(colour=TXT,size=11), legend.title=element_blank(), legend.position="bottom",
  plot.title=element_text(colour=TXT,size=14,face="bold"),
  plot.subtitle=element_text(colour=AX,size=9))

all_stats <- list()
for (M in METRICS) {
  smax <- as.data.table(h)[, .(v = max(get(M$col), na.rm = TRUE)), by = SITE_ID]   # site value = max across years (as Figs 3, 5)
  cats <- smax[, .(SITE_ID, cat = factor(ifelse(v > mean(v) + 0.5 * sd(v), "High", "Low"), levels = c("Low","Mid","High")))]
  cats[, cat2 := factor(ifelse(cat=="High","High","Low+Mid"), levels=c("Low+Mid","High"))]
  dt <- merge(d, cats[, .(SITE_ID, cat2)], by.x="test_site", by.y="SITE_ID")[!is.na(cat2)]
  dt[, response := factor(response, levels=EFP_ORDER)]

  # Mann-Whitney U between Low+Mid and High (unequal n), BH-FDR across the figure
  st <- dt[, {
      a <- mean_signed_shap[cat2=="Low+Mid"]; b <- mean_signed_shap[cat2=="High"]
      w <- tryCatch(wilcox.test(b, a, exact=FALSE), error=function(e) NULL)
      .(p       = if (is.null(w)) NA_real_ else w$p.value,
        n_low   = length(a), n_high = length(b),
        med_low = median(a), med_high = median(b))
    }, by=.(response, learner, window)]
  st[, q := p.adjust(p, "BH")]
  st[, sig := ifelse(is.na(q),"",ifelse(q<0.001,"***",ifelse(q<0.01,"**",ifelse(q<0.05,"*","ns"))))]
  yr <- dt[, .(hi = quantile(mean_signed_shap, 0.98)), by=.(response, learner)]
  st <- merge(st, yr, by=c("response","learner"))
  all_stats[[M$key]] <- copy(st)[, metric := M$lab]

  p <- ggplot(dt, aes(x=window, y=mean_signed_shap, fill=cat2)) +
    geom_hline(yintercept=0, colour="#666666", linewidth=0.5) +
    geom_violin(position=position_dodge(width=0.8), colour=NA, width=0.78, alpha=0.75, trim=TRUE) +
    geom_boxplot(aes(group=interaction(window,cat2)), position=position_dodge(width=0.8),
                 width=0.14, outlier.shape=NA, colour="#222222", fill=NA, linewidth=0.3) +
    geom_text(data=st, aes(x=window, y=hi*1.12, label=sig), inherit.aes=FALSE,
              colour=TXT, size=3.4, fontface="bold") +
    scale_fill_manual(values=CAT_COLS) +
    ggh4x::facet_grid2(response ~ learner, scales="free_y", independent="y",
                       labeller=labeller(response=EFP_LAB), switch="y") +
    labs(x=NULL, y="Net signed disturbance SHAP\n(effect on the PREDICTED value, response units)",
         title=sprintf("Direction of the disturbance effect — by %s", M$lab),
         subtitle=paste0("Tree cover ≥30 % (", PN[["sites"]], " sites) · ", PAPER_MAPS, " · M4 · natural-breaks split (site max > mean + 0.5 SD) on ", M$lab,
                         " · below zero = the model predicts a LOWER value of the ", RESP_WORD, " (not worse accuracy)\n",
                         "stars = Mann-Whitney U between Low+Mid and High (unequal n), BH-FDR across the figure ",
                         "(*** q<0.001, ** q<0.01, * q<0.05)")) +
    th + theme(strip.placement="outside")

  ggsave(file.path(OUT, sprintf("fig4_signed_shap_%s.png", M$key)), p, width=11, height=11, dpi=300, bg=BG)
  ggsave(file.path(OUT, sprintf("fig4_signed_shap_%s.pdf", M$key)), p, width=11, height=11, bg=BG)
  cat(sprintf("  saved fig4_signed_shap_%s (%d tests)\n", M$key, nrow(st)))
}
fwrite(rbindlist(all_stats)[, .(metric, learner, response, window, n_low, n_high,
        med_low=round(med_low,3), med_high=round(med_high,3), p=signif(p,3), q=signif(q,3), sig)],
       file.path(OUT, "fig4_statistics.csv"))
cat("\nFig 4 done (two metrics)\n")
