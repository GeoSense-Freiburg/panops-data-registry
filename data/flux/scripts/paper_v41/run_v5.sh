#!/bin/bash
# Full EFP analysis on the deadtrees v5 dataset (paper_v5, PAPER_RESP_SET=efp_v5): same steps as run_rerun_qc.sh,
# outputs in derived_tables/paper_v5/ and plots/paper_v5/, logs in logs/paper_v5/. For the v4.1 vs v5 comparison.
#   tmux new-session -d -s paper_v5 "bash scripts/paper_v41/run_v5.sh"
ROOT=/mnt/gsdata/projects/panops/panops-data-registry/data/flux
cd $ROOT || exit 1
LOG=$ROOT/logs/paper_v5
ST=$LOG/STATUS.txt
R=/home/nk1125/miniconda3/envs/clean_r_env/bin/Rscript
PY=/home/nk1125/miniconda3/bin/python3          # base python: the one with optuna
N_TRIALS=${N_TRIALS:-50}
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 PAPER_CORES=30 PAPER_RESP_SET=efp_v5 PAPER_TUNE_DIR=$ROOT/plots/paper_v5/Optuna
mkdir -p $LOG plots/paper_v5/Optuna/12m plots/paper_v5/Optuna/24m derived_tables/paper_v5/models
step() { local name=$1; shift; echo "[$(date '+%a %H:%M')] START $name" >> $ST
  if "$@" > $LOG/$name.log 2>&1; then echo "[$(date '+%a %H:%M')] OK    $name" >> $ST; else echo "[$(date '+%a %H:%M')] FAIL  $name (see $name.log)" >> $ST; fi; }
echo "paper_v5 (deadtrees v5) run started $(date)" > $ST

tune_one() { W=$1; L=$2; RSP=$3
  PAPER_WINDOW=$W V10_TUNE_OBJ=$ROOT/scripts/paper_v41/10_optuna_objective.R V10_TUNE_OUT=$ROOT/plots/paper_v5/Optuna/$W \
    $PY $ROOT/scripts/optuna_tune.py $L $RSP $N_TRIALS 4 > $LOG/tune_${W}_${L}_${RSP}.log 2>&1; }
export -f tune_one; export ROOT PY LOG N_TRIALS
tune_all() { for W in 12m 24m; do for L in XGB LGB; do for RSP in GPPsat NEPmax ETmax WUE; do echo "$W $L $RSP"; done; done; done |
  xargs -P 8 -L 1 bash -c 'tune_one $0 $1 $2'; grep -h "best CV" $LOG/tune_*.log; }
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
