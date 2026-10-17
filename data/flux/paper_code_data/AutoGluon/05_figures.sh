#!/bin/bash
# Paper figures with AutoGluon as a third learner next to XGBoost and LightGBM.
# The paper scripts (scripts/paper_v41/2x_*.R) are NOT modified: patched copies are written to
# AutoGluon/fig_scripts/ (only change: learner "AG" added) and run on a staging folder that holds the
# XGB / LGB files (links) and the AG files. Figures go to AutoGluon/plots/<set>/.
# Draws what is available: Fig 2 + site delta RMSE need the LOSO predictions, Figs 3 / 3b / 4 need SHAP.
#   bash 05_figures.sh <efp|flux>
SET=${1:-efp}
ROOT=${PAPER_ROOT:-$(pwd)}
AG=${AG_DIR:-$ROOT/AutoGluon}
R=Rscript
SRC=$ROOT/scripts/paper_v41
case $SET in flux) REF=$ROOT/derived_tables/paper_v41_fluxes/models ;; efp_v5) REF=$ROOT/derived_tables/paper_v5/models ;;
  efp_nothresh) REF=$ROOT/derived_tables/paper_v41_nothresh/models ;; efp_v5_nothresh) REF=$ROOT/derived_tables/paper_v5_nothresh/models ;;
  *) REF=$ROOT/derived_tables/paper_v41/models ;; esac
# R side: <set>_nothresh = the base set with PAPER_NO_THRESH=1 (paper_common.R)
RSET=${SET%_nothresh}; [ "$RSET" != "$SET" ] && export PAPER_NO_THRESH=1
OUT=$AG/outputs/$SET; STAGE=$OUT/fig_inputs; FS=$AG/fig_scripts
mkdir -p $STAGE $FS $AG/plots/$SET $AG/logs
for f in $REF/XGB_*.csv $REF/LGB_*.csv $OUT/AG_predictions_repCV.csv $OUT/AG_metrics_repCV.csv $OUT/AG_site_shap_M4.csv $OUT/AG_site_signed_shap_M4.csv; do
  [ -f "$f" ] && ln -sf "$f" $STAGE/
done
# patched copies: add the AutoGluon learner
for s in 21_fig2_M3M4.R 22_fig3_site_shap.R 23_fig4_signed_shap.R 25_fig3b_site_signed_shap.R 29_site_delta_rmse.R; do
  sed -E -e 's/list\(lab ?= ?"LightGBM \(Optuna\)", fam ?= ?"LGB"\)\)/list(lab = "LightGBM (Optuna)", fam = "LGB"),\n  list(lab = "AutoGluon (ensemble)", fam = "AG"))/' \
         -e 's/LGB = "LightGBM \(Optuna\)"\)/LGB = "LightGBM (Optuna)", AG = "AutoGluon (ensemble)")/' \
         -e 's/c\("XGB", "LGB"\)\)/c("XGB", "LGB", "AG"))/' \
         -e 's/LGB = "LightGBM--Optuna"\)/LGB = "LightGBM--Optuna", AG = "AutoGluon ensemble")/' $SRC/$s > $FS/$s
done
export PAPER_RESP_SET=$RSET PAPER_OUT_DIR=$STAGE PAPER_FIG_DIR=$AG/plots/$SET
run() { local n=$1; shift; if $R "$@" > $AG/logs/05_${SET}_$n.log 2>&1; then echo "OK   $n"; else echo "FAIL $n (logs/05_${SET}_$n.log)"; fi; }
if [ -f $STAGE/AG_predictions_repCV.csv ]; then
  run fig2 $FS/21_fig2_M3M4.R
  run fig2_high_reldist $FS/21_fig2_M3M4.R high_reldist
  run fig2_high_relmort $FS/21_fig2_M3M4.R high_relmort
  run site_delta_rmse $FS/29_site_delta_rmse.R
fi
if [ -f $STAGE/AG_site_shap_M4.csv ]; then
  run fig3_AG $FS/22_fig3_site_shap.R AG
  run fig3b_AG $FS/25_fig3b_site_signed_shap.R AG
  run fig4 $FS/23_fig4_signed_shap.R
  run fig5_AG $ROOT/scripts/paper_v41/24_fig5_igbp_board.R filtered AG
  run fig3_landscape_AG $ROOT/scripts/paper_v41/32_fig3_landscape.R AG all all
fi
ls $AG/plots/$SET | grep -c png
