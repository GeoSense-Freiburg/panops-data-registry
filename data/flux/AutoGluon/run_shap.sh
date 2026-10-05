#!/bin/bash
# Permutation SHAP for the AutoGluon M4 ensembles, EFP set then flux set (resumes from cache).
#   AG_WORKERS=20 tmux new-session -d -s ag_shap "bash AutoGluon/run_shap.sh"
cd /mnt/gsdata/projects/panops/panops-data-registry/data/flux/AutoGluon || exit 1
PY=/home/nk1125/miniconda3/envs/autogluon_env/bin/python
WORKERS=${AG_WORKERS:-20}
echo "started $(date)" > logs/STATUS_shap.txt
for S in efp flux; do
  if $PY 03_shap.py $S $WORKERS >> logs/03_shap_$S.log 2>&1; then echo "[$(date '+%a %H:%M')] OK   shap $S" >> logs/STATUS_shap.txt
  else echo "[$(date '+%a %H:%M')] FAIL shap $S (see logs/03_shap_$S.log)" >> logs/STATUS_shap.txt; fi
done
echo "ALL_DONE $(date)" >> logs/STATUS_shap.txt
