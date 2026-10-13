#!/bin/bash
# Whole AutoGluon chain for one EFP set (arg 1: efp = V4.1 [default] | efp_v5 = V5 maps): full fits -> repeated LOSO and SHAP in parallel -> tables + figures.
# Waits for the XGB / LGB rerun (scripts/paper_v41/run_rerun_qc.sh) before the comparison, if it is running.
#   tmux new-session -d -s ag_efp "bash AutoGluon/run_efp.sh [efp|efp_v5]"
ROOT=${PAPER_ROOT:-$(pwd)}
cd $ROOT/AutoGluon || exit 1
S=${1:-efp}
PY=python
L=${AG_WORK:-.}/logs; ST=$L/STATUS_$S.txt; mkdir -p $L; echo "started $(date)" > $ST
ok() { echo "[$(date '+%a %H:%M')] $1" >> $ST; }
$PY 01_fit_full.py $S 16 6 > $L/01_fit_full_$S.log 2>&1 && ok "OK   fit_full" || ok "FAIL fit_full"
$PY 03_shap.py $S ${AG_SHAP_WORKERS:-24} > $L/03_shap_$S.log 2>&1 & ps=$!
$PY 02_loso_repcv.py $S ${AG_WORKERS:-48} 2 > $L/02_loso_$S.log 2>&1 && ok "OK   loso" || ok "FAIL loso"
wait $ps && ok "OK   shap" || ok "FAIL shap"
while [ -f $ROOT/logs/paper_v41_rerun_qc/STATUS.txt ] && ! grep -q ALL_DONE $ROOT/logs/paper_v41_rerun_qc/STATUS.txt; do sleep 60; done
$PY 04_compare.py $S > $L/04_compare_$S.log 2>&1 && ok "OK   compare" || ok "FAIL compare"
bash 05_figures.sh $S > $L/05_figures_$S.log 2>&1; ok "figures: $(tr '\n' ' ' < $L/05_figures_$S.log)"
echo "ALL_DONE $(date)" >> $ST
