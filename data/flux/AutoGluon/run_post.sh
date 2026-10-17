#!/bin/bash
# Waits for the LOSO and SHAP outputs of each set and then writes RESULTS_<set>.md and the figures.
# Runs on its own so the tables and figures appear as soon as the inputs exist.
#   tmux new-session -d -s ag_post "bash AutoGluon/run_post.sh"
cd /mnt/gsdata/projects/panops/panops-data-registry/data/flux/AutoGluon || exit 1
PY=/home/nk1125/miniconda3/envs/autogluon_env/bin/python
ST=logs/STATUS_post.txt; echo "started $(date)" > $ST
declare -A done
while :; do
  for S in efp flux; do
    for K in predictions_repCV site_signed_shap_M4; do
      if [ -z "${done[$S$K]}" ] && [ -f outputs/$S/AG_$K.csv ]; then
        sleep 20
        $PY 04_compare.py $S > logs/04_compare_$S.log 2>&1
        bash 05_figures.sh $S > logs/05_figures_$S.log 2>&1
        done[$S$K]=1; echo "[$(date '+%a %H:%M')] $S: tables + figures updated after $K" >> $ST
      fi
    done
  done
  [ ${#done[@]} -eq 4 ] && break
  grep -q ALL_DONE logs/STATUS_loso.txt && grep -q ALL_DONE logs/STATUS_shap.txt && [ -n "$fin" ] && break
  grep -q ALL_DONE logs/STATUS_loso.txt && grep -q ALL_DONE logs/STATUS_shap.txt && fin=1
  sleep 120
done
echo "ALL_DONE $(date)" >> $ST
