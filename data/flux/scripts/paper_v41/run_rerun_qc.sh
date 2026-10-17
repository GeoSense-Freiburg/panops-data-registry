#!/bin/bash
# Rerun of the paper_v41 EFP analysis after the 2026-10-02 quality-control fix in 03_build_paper_dataset.R
# (CN-Sb1 / CN-Sb2 and negative-WUE site-years removed -> 104 sites / 381 site-years).
# Same steps as run_weekend.sh plus the figures added later (Fig 2 high-disturbance subsets, Fig 3b,
# disturbance-block composition, site delta RMSE). Logs: logs/paper_v41_rerun_qc/.
# The previous results are kept in derived_tables/paper_v41/with_CNSb_2026-10-01/ and plots/paper_v41_with_CNSb_2026-10-01/.
#   tmux new-session -d -s paper_rerun "bash scripts/paper_v41/run_rerun_qc.sh"
ROOT=/mnt/gsdata/projects/panops/panops-data-registry/data/flux
cd $ROOT || exit 1
LOG=$ROOT/logs/paper_v41_rerun_qc
ST=$LOG/STATUS.txt
R=/home/nk1125/miniconda3/envs/clean_r_env/bin/Rscript
PY=/home/nk1125/miniconda3/bin/python3          # base python: the one with optuna
N_TRIALS=${N_TRIALS:-50}
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 PAPER_CORES=30
mkdir -p $LOG plots/paper_v41/Optuna/12m plots/paper_v41/Optuna/24m derived_tables/paper_v41/models
step() { local name=$1; shift; echo "[$(date '+%a %H:%M')] START $name" >> $ST
  if "$@" > $LOG/$name.log 2>&1; then echo "[$(date '+%a %H:%M')] OK    $name" >> $ST; else echo "[$(date '+%a %H:%M')] FAIL  $name (see $name.log)" >> $ST; fi; }
echo "paper_v41 QC rerun started $(date)" > $ST

tune_one() { W=$1; L=$2; RSP=$3
  PAPER_WINDOW=$W V10_TUNE_OBJ=$ROOT/scripts/paper_v41/10_optuna_objective.R V10_TUNE_OUT=$ROOT/plots/paper_v41/Optuna/$W \
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
echo "ALL_DONE $(date)" >> $ST
