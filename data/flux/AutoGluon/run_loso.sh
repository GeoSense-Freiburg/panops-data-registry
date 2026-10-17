#!/bin/bash
# Repeated leave-one-site-out CV with AutoGluon, EFP set then flux set (several hours; resumes from cache).
#   tmux new-session -d -s ag_loso "bash AutoGluon/run_loso.sh"
cd /mnt/gsdata/projects/panops/panops-data-registry/data/flux/AutoGluon || exit 1
PY=/home/nk1125/miniconda3/envs/autogluon_env/bin/python
WORKERS=${AG_WORKERS:-40}; CPUS=${AG_CPUS:-2}
echo "started $(date)" > logs/STATUS_loso.txt
for S in efp flux; do
  if $PY 02_loso_repcv.py $S $WORKERS $CPUS > logs/02_loso_$S.log 2>&1; then echo "[$(date '+%a %H:%M')] OK   loso $S" >> logs/STATUS_loso.txt
  else echo "[$(date '+%a %H:%M')] FAIL loso $S (see logs/02_loso_$S.log)" >> logs/STATUS_loso.txt; fi
done
echo "ALL_DONE $(date)" >> logs/STATUS_loso.txt
