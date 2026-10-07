#!/bin/bash
# AutoGluon without the thresholded mortality metrics (user 2026-10-07): V4.1 (efp_nothresh), then V5 (efp_v5_nothresh).
# Runs on the local NVMe SSD /mnt/ssds/nk1125/AutoGluon (AG_WORK): models, cache, temporary files and logs are written there,
# because the network drive /mnt/gsdata is slow for thousands of small files. When a set finishes, its results
# (outputs/<set>, plots/<set>, RESULTS_<set>.md, logs) are copied back to AutoGluon/ on /mnt/gsdata.
# M3 has no mortality predictors, so its LOSO cache could be reused from efp / efp_v5 (AG_COPY_M3=1); off by default
# because copying the small cache files off a busy /mnt/gsdata was slower than refitting M3 locally.
#   tmux new-session -d -s ag_nothresh "bash AutoGluon/run_nothresh.sh"
GS=/mnt/gsdata/projects/panops/panops-data-registry/data/flux/AutoGluon
export AG_WORK=/mnt/ssds/nk1125/AutoGluon AG_DIR=/mnt/ssds/nk1125/AutoGluon
mkdir -p $AG_WORK/logs
cd $GS || exit 1
for S in efp_nothresh efp_v5_nothresh; do
  B=${S%_nothresh}
  if [ "${AG_COPY_M3:-0}" = 1 ]; then   # off by default: reading ~1 700 small files from a busy /mnt/gsdata took hours
    mkdir -p $AG_WORK/cache/loso/$S && cp -a $GS/cache/loso/$B/M3_* $AG_WORK/cache/loso/$S/
  fi
  bash run_efp.sh $S
  mkdir -p $GS/outputs/$S $GS/plots/$S $GS/logs
  rsync -a $AG_WORK/outputs/$S/ $GS/outputs/$S/
  rsync -a $AG_WORK/plots/$S/ $GS/plots/$S/ 2>/dev/null
  cp $AG_WORK/RESULTS_$S.md $GS/ 2>/dev/null
  rsync -a $AG_WORK/logs/ $GS/logs/
  echo "[$(date '+%a %H:%M')] results copied back to $GS" >> $GS/logs/STATUS_$S.txt
done
