#!/bin/bash
# Resume of the EFP leave-one-site-out run (cache-based), then tables + figures once the SHAP chain (tmux ag_efp) has ended.
cd /mnt/gsdata/projects/panops/panops-data-registry/data/flux/AutoGluon || exit 1
PY=/home/nk1125/miniconda3/envs/autogluon_env/bin/python
ST=logs/STATUS_efp.txt; ok() { echo "[$(date '+%a %H:%M')] $1" >> $ST; }
ok "loso resumed without worker recycling (the FAIL above is the stalled process being stopped)"
$PY 02_loso_repcv.py efp ${AG_WORKERS:-48} 2 > logs/02_loso_efp_resume.log 2>&1 && ok "OK   loso (resume)" || ok "FAIL loso (resume)"
while tmux has-session -t ag_efp 2>/dev/null; do sleep 60; done
$PY 04_compare.py efp > logs/04_compare_efp.log 2>&1 && ok "OK   compare (final)" || ok "FAIL compare (final)"
bash 05_figures.sh efp > logs/05_figures_efp.log 2>&1; ok "figures (final): $(tr '\n' ' ' < logs/05_figures_efp.log)"
echo "FINAL_DONE $(date)" >> $ST
