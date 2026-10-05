#!/bin/bash
# Whole AutoGluon chain for the EFP set: full fits -> repeated LOSO and SHAP in parallel -> tables + figures.
# Waits for the XGB / LGB rerun (scripts/paper_v41/run_rerun_qc.sh) before the comparison, if it is running.
#   tmux new-session -d -s ag_efp "bash AutoGluon/run_efp.sh"
ROOT=/mnt/gsdata/projects/panops/panops-data-registry/data/flux
cd $ROOT/AutoGluon || exit 1
PY=/home/nk1125/miniconda3/envs/autogluon_env/bin/python
ST=logs/STATUS_efp.txt; mkdir -p logs; echo "started $(date)" > $ST
ok() { echo "[$(date '+%a %H:%M')] $1" >> $ST; }
$PY 01_fit_full.py efp 16 6 > logs/01_fit_full_efp.log 2>&1 && ok "OK   fit_full" || ok "FAIL fit_full"
$PY 03_shap.py efp ${AG_SHAP_WORKERS:-24} > logs/03_shap_efp.log 2>&1 & ps=$!
$PY 02_loso_repcv.py efp ${AG_WORKERS:-48} 2 > logs/02_loso_efp.log 2>&1 && ok "OK   loso" || ok "FAIL loso"
wait $ps && ok "OK   shap" || ok "FAIL shap"
while [ -f $ROOT/logs/paper_v41_rerun_qc/STATUS.txt ] && ! grep -q ALL_DONE $ROOT/logs/paper_v41_rerun_qc/STATUS.txt; do sleep 60; done
$PY 04_compare.py efp > logs/04_compare_efp.log 2>&1 && ok "OK   compare" || ok "FAIL compare"
bash 05_figures.sh efp > logs/05_figures_efp.log 2>&1; ok "figures: $(tr '\n' ' ' < logs/05_figures_efp.log)"
echo "ALL_DONE $(date)" >> $ST
