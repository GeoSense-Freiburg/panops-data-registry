#!/bin/bash
# AutoGluon-only versions of Fig 2, Fig 2b, the per-site delta RMSE and Fig 4 (coauthor draft, 2026-10-09).
# Takes the three-learner copies made by 05_figures.sh (run that first), drops XGBoost / LightGBM, narrows the
# figures, removes title and subtitle (the draft captions carry them) and draws to AutoGluon/plots/<set>/ag_only/.
#   bash 05b_figures_ag_only.sh [efp_v5_nothresh]
SET=${1:-efp_v5_nothresh}
AG=/mnt/gsdata/projects/panops/panops-data-registry/data/flux/AutoGluon
R=/home/nk1125/miniconda3/envs/clean_r_env/bin/Rscript
FS=$AG/fig_scripts; FA=$FS/ag_only; mkdir -p $FA $AG/plots/$SET/ag_only
NOTITLE='s/, p, (width ?=)/, p + labs(title = NULL, subtitle = NULL), \1/'
sed -E -e '/fam = "XGB"\),$/d' -e '/fam = "LGB"\),$/d' -e 's/width = 15, height = 12/width = 7.5, height = 12/g' \
       -e 's/"AutoGluon \(ensemble\)"/"AutoGluon"/' -e "$NOTITLE" $FS/21_fig2_M3M4.R > $FA/21_fig2_M3M4.R
sed -E -e 's/^LEARNERS <- list\(list\(lab="XGBoost \(Optuna\)",  fam="XGB"\),$/LEARNERS <- list(/' \
       -e '/list\(lab = "LightGBM \(Optuna\)", fam = "LGB"\),$/d' -e 's/width=11, height=11/width=5.5, height=11/g' \
       -e 's/"AutoGluon \(ensemble\)"/"AutoGluon"/' -e "$NOTITLE" $FS/23_fig4_signed_shap.R > $FA/23_fig4_signed_shap.R
sed -E -e 's/^LEARNERS <- c\(XGB = .*$/LEARNERS <- c(AG = "AutoGluon")/' -e 's/width = 15, height = 11/width = 8, height = 11/g' \
       -e "$NOTITLE" $FS/29_site_delta_rmse.R > $FA/29_site_delta_rmse.R
RSET=${SET%_nothresh}; [ "$RSET" != "$SET" ] && export PAPER_NO_THRESH=1
export PAPER_RESP_SET=$RSET PAPER_OUT_DIR=$AG/outputs/$SET/fig_inputs PAPER_FIG_DIR=$AG/plots/$SET/ag_only
for a in "21_fig2_M3M4.R" "21_fig2_M3M4.R high_reldist" "29_site_delta_rmse.R" "23_fig4_signed_shap.R"; do
  set -- $a
  if $R $FA/$1 $2 > $AG/logs/05b_${SET}_$1$2.log 2>&1; then echo "OK   $a"; else echo "FAIL $a (logs/05b_${SET}_$1$2.log)"; fi
done
