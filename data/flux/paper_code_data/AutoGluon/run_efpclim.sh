#!/bin/bash
# EFP-specific climate block (user 2026-10-12): AutoGluon repeated LOSO for efp_v5_nothresh_efpclim, without SHAP.
# NEPmax has the same predictors as efp_v5_nothresh, so its LOSO cache is copied and not refitted.
# Runs on the local SSD (AG_WORK); outputs are copied back to AutoGluon/outputs/<set>.
#   tmux new-session -d -s ag_efpclim "bash AutoGluon/run_efpclim.sh"
GS=${PAPER_ROOT:-$(pwd)}/AutoGluon
S=efp_v5_nothresh_efpclim; B=efp_v5_nothresh
export AG_WORK=${AG_WORK:-$PAPER_ROOT/AutoGluon}
PY=python
L=$AG_WORK/logs; ST=$L/STATUS_$S.txt; mkdir -p $L $AG_WORK/cache/loso/$S; echo "started $(date)" > $ST
for d in $AG_WORK/cache/loso/$B/M?_*_NEPmax; do cp -a $d $AG_WORK/cache/loso/$S/; done
echo "[$(date '+%a %H:%M')] NEPmax cache copied ($(ls $AG_WORK/cache/loso/$S | wc -l) task dirs)" >> $ST
cd $GS
$PY 02_loso_repcv.py $S ${AG_WORKERS:-48} 2 > $L/02_loso_$S.log 2>&1 && echo "[$(date '+%a %H:%M')] OK   loso" >> $ST || echo "[$(date '+%a %H:%M')] FAIL loso" >> $ST
mkdir -p $GS/outputs/$S $GS/logs && rsync -a $AG_WORK/outputs/$S/ $GS/outputs/$S/ && cp $L/02_loso_$S.log $ST $GS/logs/
echo "ALL_DONE $(date)" >> $ST; cp $ST $GS/logs/
