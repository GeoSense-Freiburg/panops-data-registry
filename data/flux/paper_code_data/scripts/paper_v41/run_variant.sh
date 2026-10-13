#!/bin/bash
# Full EFP analysis (XGB / LGB: Optuna, repeated LOSO, TreeSHAP, figures) for one dataset variant.
#   bash scripts/paper_v41/run_variant.sh <efp|efp_v5>        env PAPER_NO_THRESH=1 -> mortality block without the
#   thresholded metrics (outputs <sub>_nothresh). Logs: logs/<sub>/. CORES / TUNE_PAR set the load.
ROOT=${PAPER_ROOT:-$(pwd)}
cd $ROOT || exit 1
SET=${1:-efp}
case $SET in efp) SUB=paper_v41 ;; efp_v5) SUB=paper_v5 ;; *) echo "unknown set $SET"; exit 1 ;; esac
[ "${PAPER_NO_THRESH:-0}" = 1 ] && SUB=${SUB}_nothresh
LOG=$ROOT/logs/$SUB
ST=$LOG/STATUS.txt
R=Rscript
PY=python3          # base python: the one with optuna
N_TRIALS=${N_TRIALS:-50}
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 PAPER_CORES=${CORES:-20} PAPER_RESP_SET=$SET PAPER_NO_THRESH=${PAPER_NO_THRESH:-0} PAPER_TUNE_DIR=$ROOT/plots/$SUB/Optuna
mkdir -p $LOG plots/$SUB/Optuna/12m plots/$SUB/Optuna/24m derived_tables/$SUB/models
step() { local name=$1; shift; echo "[$(date '+%a %H:%M')] START $name" >> $ST
  if "$@" > $LOG/$name.log 2>&1; then echo "[$(date '+%a %H:%M')] OK    $name" >> $ST; else echo "[$(date '+%a %H:%M')] FAIL  $name (see $name.log)" >> $ST; fi; }
echo "$SUB run started $(date)" > $ST

tune_one() { W=$1; L=$2; RSP=$3
  PAPER_WINDOW=$W V10_TUNE_OBJ=$ROOT/scripts/paper_v41/10_optuna_objective.R V10_TUNE_OUT=$ROOT/plots/$SUB/Optuna/$W \
    $PY $ROOT/scripts/optuna_tune.py $L $RSP $N_TRIALS 4 > $LOG/tune_${W}_${L}_${RSP}.log 2>&1; }
export -f tune_one; export ROOT PY LOG N_TRIALS SUB
tune_all() { for W in 12m 24m; do for L in XGB LGB; do for RSP in GPPsat NEPmax ETmax WUE; do echo "$W $L $RSP"; done; done; done |
  xargs -P ${TUNE_PAR:-6} -L 1 bash -c 'tune_one $0 $1 $2'; grep -h "best CV" $LOG/tune_*.log; }
step 01_optuna tune_all
step 02_collect_configs $PY scripts/paper_v41/13_collect_optuna.py

$R scripts/paper_v41/11_train_repcv.R XGB > $LOG/03_train_XGB.log 2>&1 & p1=$!
$R scripts/paper_v41/11_train_repcv.R LGB > $LOG/03_train_LGB.log 2>&1 & p2=$!
wait $p1 && echo "[$(date '+%a %H:%M')] OK    03_train_XGB" >> $ST || echo "[$(date '+%a %H:%M')] FAIL  03_train_XGB" >> $ST
wait $p2 && echo "[$(date '+%a %H:%M')] OK    03_train_LGB" >> $ST || echo "[$(date '+%a %H:%M')] FAIL  03_train_LGB" >> $ST

$R scripts/paper_v41/12_shap_M4.R XGB > $LOG/04_shap_XGB.log 2>&1 & p1=$!
$R scripts/paper_v41/12_shap_M4.R LGB > $LOG/04_shap_LGB.log 2>&1 & p2=$!
wait $p1 && echo "[$(date '+%a %H:%M')] OK    04_shap_XGB" >> $ST || echo "[$(date '+%a %H:%M')] FAIL  04_shap_XGB" >> $ST
wait $p2 && echo "[$(date '+%a %H:%M')] OK    04_shap_LGB" >> $ST || echo "[$(date '+%a %H:%M')] FAIL  04_shap_LGB" >> $ST

step 05_fig1          $R scripts/paper_v41/20_fig1_site_map.R filtered
step 06_fig2          $R scripts/paper_v41/21_fig2_M3M4.R
step 06_fig2_reldist  $R scripts/paper_v41/21_fig2_M3M4.R high_reldist
step 06_fig2_relmort  $R scripts/paper_v41/21_fig2_M3M4.R high_relmort
step 07_fig3_XGB      $R scripts/paper_v41/22_fig3_site_shap.R XGB
step 07_fig3_LGB      $R scripts/paper_v41/22_fig3_site_shap.R LGB
step 07_fig3b_XGB     $R scripts/paper_v41/25_fig3b_site_signed_shap.R XGB
step 07_fig3b_LGB     $R scripts/paper_v41/25_fig3b_site_signed_shap.R LGB
step 08_fig4          $R scripts/paper_v41/23_fig4_signed_shap.R
step 09_fig5_XGB      $R scripts/paper_v41/24_fig5_igbp_board.R filtered XGB
step 09_fig5_LGB      $R scripts/paper_v41/24_fig5_igbp_board.R filtered LGB
step 10_Dcomp         $R scripts/paper_v41/26_D_block_composition.R
step 11_site_dRMSE    $R scripts/paper_v41/29_site_delta_rmse.R
step 12_fig3_landscape_LGB $R scripts/paper_v41/32_fig3_landscape.R LGB all all
step 12_fig3_landscape_XGB $R scripts/paper_v41/32_fig3_landscape.R XGB all all
echo "ALL_DONE $(date)" >> $ST
