#!/bin/bash
# Unattended run of the paper_v41 analysis: Optuna tuning -> repeated-CV training (M3, M4; 12m, 24m;
# XGB, LGB) -> TreeSHAP for M4 -> Figs 1-5. Each step logs to logs/paper_v41_weekend/; a failing step is
# recorded in STATUS.txt and the later steps still run where they can.
#   setsid nohup bash scripts/paper_v41/run_weekend.sh > /dev/null 2>&1 < /dev/null &
ROOT=/mnt/gsdata/projects/panops/panops-data-registry/data/flux
cd $ROOT || exit 1
LOG=$ROOT/logs/paper_v41_weekend
ST=$LOG/STATUS.txt
R=/home/nk1125/miniconda3/envs/clean_r_env/bin/Rscript
PY=/home/nk1125/miniconda3/bin/python3          # base python: the one with optuna
N_TRIALS=${N_TRIALS:-50}
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 PAPER_CORES=30
mkdir -p $LOG plots/paper_v41/Optuna/12m plots/paper_v41/Optuna/24m derived_tables/paper_v41/models
step() { local name=$1; shift; echo "[$(date '+%a %H:%M')] START $name" >> $ST
  if "$@" > $LOG/$name.log 2>&1; then echo "[$(date '+%a %H:%M')] OK    $name" >> $ST; else echo "[$(date '+%a %H:%M')] FAIL  $name (see $name.log)" >> $ST; fi; }
echo "paper_v41 weekend run started $(date)" > $ST

# 1. Optuna: 2 learners x 4 EFPs x 2 windows = 16 studies, 8 in parallel x 4 jobs each
tune_one() { W=$1; L=$2; RSP=$3
  PAPER_WINDOW=$W V10_TUNE_OBJ=$ROOT/scripts/paper_v41/10_optuna_objective.R V10_TUNE_OUT=$ROOT/plots/paper_v41/Optuna/$W \
    $PY $ROOT/scripts/optuna_tune.py $L $RSP $N_TRIALS 4 > $LOG/tune_${W}_${L}_${RSP}.log 2>&1; }
export -f tune_one; export ROOT PY LOG N_TRIALS
tune_all() { for W in 12m 24m; do for L in XGB LGB; do for RSP in GPPsat NEPmax ETmax WUE; do echo "$W $L $RSP"; done; done; done |
  xargs -P 8 -L 1 bash -c 'tune_one $0 $1 $2'; grep -h "best CV" $LOG/tune_*.log; }
step 01_optuna tune_all
step 02_collect_configs $PY scripts/paper_v41/13_collect_optuna.py

# 2. training (both learners in parallel)
$R scripts/paper_v41/11_train_repcv.R XGB > $LOG/03_train_XGB.log 2>&1 & p1=$!
$R scripts/paper_v41/11_train_repcv.R LGB > $LOG/03_train_LGB.log 2>&1 & p2=$!
wait $p1 && echo "[$(date '+%a %H:%M')] OK    03_train_XGB" >> $ST || echo "[$(date '+%a %H:%M')] FAIL  03_train_XGB" >> $ST
wait $p2 && echo "[$(date '+%a %H:%M')] OK    03_train_LGB" >> $ST || echo "[$(date '+%a %H:%M')] FAIL  03_train_LGB" >> $ST

# 3. SHAP for M4 (both learners in parallel)
$R scripts/paper_v41/12_shap_M4.R XGB > $LOG/04_shap_XGB.log 2>&1 & p1=$!
$R scripts/paper_v41/12_shap_M4.R LGB > $LOG/04_shap_LGB.log 2>&1 & p2=$!
wait $p1 && echo "[$(date '+%a %H:%M')] OK    04_shap_XGB" >> $ST || echo "[$(date '+%a %H:%M')] FAIL  04_shap_XGB" >> $ST
wait $p2 && echo "[$(date '+%a %H:%M')] OK    04_shap_LGB" >> $ST || echo "[$(date '+%a %H:%M')] FAIL  04_shap_LGB" >> $ST

# 4. figures (coauthor-draft style)
step 05_fig1      $R scripts/paper_v41/20_fig1_site_map.R filtered
step 06_fig2      $R scripts/paper_v41/21_fig2_M3M4.R
step 07_fig3_XGB  $R scripts/paper_v41/22_fig3_site_shap.R XGB
step 07_fig3_LGB  $R scripts/paper_v41/22_fig3_site_shap.R LGB
step 08_fig4      $R scripts/paper_v41/23_fig4_signed_shap.R
step 09_fig5_XGB  $R scripts/paper_v41/24_fig5_igbp_board.R filtered XGB
step 09_fig5_LGB  $R scripts/paper_v41/24_fig5_igbp_board.R filtered LGB
echo "ALL_DONE $(date)" >> $ST
