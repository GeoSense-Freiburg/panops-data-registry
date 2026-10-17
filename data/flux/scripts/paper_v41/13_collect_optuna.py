#!/usr/bin/env python3
"""Collect <L>_<resp>_best.json from plots/paper_v41/Optuna/<window>/ into best_configs.csv per window."""
import json, glob, os, csv, sys
root = os.environ.get("PAPER_TUNE_DIR", "/mnt/gsdata/projects/panops/panops-data-registry/data/flux/plots/paper_v41/Optuna")
for win in ("12m", "24m"):
    rows = []
    for f in sorted(glob.glob(f"{root}/{win}/*_best.json")):
        b = json.load(open(f)); r = dict(learner=b["learner"], response=b["response"], best_cv=b["best_value"], n_trials=b["n_trials"]); r.update(b["params"]); rows.append(r)
    if not rows: print(f"{win}: no results"); continue
    keys = ["learner", "response", "best_cv", "n_trials"] + sorted({k for r in rows for k in r} - {"learner", "response", "best_cv", "n_trials"})
    with open(f"{root}/{win}/best_configs.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=keys); w.writeheader(); [w.writerow(r) for r in rows]
    print(f"{win}: {len(rows)} configs -> {root}/{win}/best_configs.csv")
