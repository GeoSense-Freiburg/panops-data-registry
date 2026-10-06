#!/usr/bin/env python
"""
With vs without the thresholded mortality metrics (new_deadwood_gain_pp_thresh, relative_tree_loss_pct_thresh),
for V4.1 and V5, XGBoost and LightGBM (run_variant.sh with PAPER_NO_THRESH=1 -> <sub>_nothresh).
Compares skill, the effect of adding mortality (overall dRMSE and Fig 2 / Table 1 statistics) and the Fig 4 tests.
Output: plots/compare_nothresh/COMPARE_NOTHRESH.md (+ CSVs)
    python 36_compare_nothresh.py
"""
import os
import numpy as np, pandas as pd

ROOT = "/mnt/gsdata/projects/panops/panops-data-registry/data/flux"
OUT = f"{ROOT}/plots/compare_nothresh"
os.makedirs(OUT, exist_ok=True)
EFP = ["GPPsat", "NEPmax", "ETmax", "WUE"]
RUNS = {("V4.1", "with"): "paper_v41", ("V4.1", "without"): "paper_v41_nothresh",
        ("V5", "with"): "paper_v5", ("V5", "without"): "paper_v5_nothresh"}
md = ["# Mortality block with vs without the thresholded metrics", "",
      "Without = new_deadwood_gain_pp_thresh and relative_tree_loss_pct_thresh removed (12 m: 193 instead of 213 predictors, "
      "24 m: 338 instead of 368). Everything else identical. XGBoost and LightGBM, Optuna re-tuned for each variant.", ""]


def r2(s):
    return 1 - ((s.observed - s.predicted) ** 2).sum() / ((s.observed - s.observed.mean()) ** 2).sum()


rows, eff = [], []
for (ver, th), sub in RUNS.items():
    for L in ("XGB", "LGB"):
        p = pd.read_csv(f"{ROOT}/derived_tables/{sub}/models/{L}_predictions_repCV.csv")
        p = p[p.response.isin(EFP)]
        for (m, r), s in p.groupby(["model", "response"]):
            rows.append(dict(maps=ver, thresholds=th, learner=L, model=m, response=r, R2=r2(s), RMSE=np.sqrt(((s.observed - s.predicted) ** 2).mean())))
    st = pd.read_csv(f"{ROOT}/plots/{sub}/table_fig2_dRMSE_total_mort_statistics.csv") if os.path.exists(f"{ROOT}/plots/{sub}/table_fig2_dRMSE_total_mort_statistics.csv") else None
    if st is not None:
        st = st[st.learner.isin(["XGB", "LGB"])].assign(maps=ver, thresholds=th); eff.append(st)
R = pd.DataFrame(rows)
R.to_csv(f"{OUT}/skill_all.csv", index=False)

# skill: mean R2 of C+T+M over EFPs and windows
m4 = R[R.model.str.startswith("M4")]
t = m4.pivot_table(index=["maps", "learner"], columns="thresholds", values="R2", aggfunc="mean")[["with", "without"]]
t["diff"] = t["without"] - t["with"]
md += ["## Skill: mean R² of C+T+M (4 EFPs × 2 windows)", "", t.round(3).to_markdown(), ""]
t2 = m4.pivot_table(index=["maps", "response"], columns="thresholds", values="R2", aggfunc="mean")[["with", "without"]]
t2["diff"] = t2["without"] - t2["with"]
md += ["Per EFP (mean of learners and windows):", "", t2.round(3).to_markdown(), ""]

# effect of mortality: overall dRMSE %
R["window"] = R.model.str[3:]; R["mod"] = R.model.str[:2]
w = R.pivot_table(index=["maps", "thresholds", "learner", "window", "response"], columns="mod", values="RMSE").reset_index()
w["dRMSE_pct"] = 100 * (w.M4 - w.M3) / w.M3
d = w.pivot_table(index=["maps", "response"], columns="thresholds", values="dRMSE_pct", aggfunc="mean")[["with", "without"]]
md += ["## Effect of adding mortality: change in overall RMSE (%), mean of learners and windows", "", d.round(2).to_markdown(), ""]

# Table 1 statistics (median per-site change, by total-mortality class)
if eff:
    E = pd.concat(eff)
    E.to_csv(f"{OUT}/table1_stats_all.csv", index=False)
    e = E.pivot_table(index=["maps", "subset", "response"], columns="thresholds", values="median_pct", aggfunc="mean")[["with", "without"]]
    e = e.reindex([(v, s, r) for v in ("V4.1", "V5") for s in ("all", "midhigh", "high") for r in EFP])
    sig = E.assign(s=E.q < 0.05).pivot_table(index=["maps", "subset", "response"], columns="thresholds", values="s", aggfunc="sum")[["with", "without"]]
    e["sig with (of 4)"] = sig["with"].reindex(e.index); e["sig without (of 4)"] = sig["without"].reindex(e.index)
    md += ["## Table 1 statistics: median per-site RMSE change (%) by total-mortality class",
           "(mean of XGB/LGB × 12/24 m; 'sig' = number of the 4 learner-window tests with q < 0.05)", "", e.round(2).to_markdown(), ""]

# Fig 4 tests
f4 = []
for (ver, th), sub in RUNS.items():
    f = f"{ROOT}/plots/{sub}/fig4_statistics.csv"
    if os.path.exists(f):
        x = pd.read_csv(f); f4.append(dict(maps=ver, thresholds=th,
              **{f"{m} significant": f"{(g.q < 0.05).sum()} of {len(g)}" for m, g in x.groupby("metric")}))
if f4:
    md += ["## Fig 4 (direction, High vs Low+Mid): significant tests", "", pd.DataFrame(f4).set_index(["maps", "thresholds"]).to_markdown(), ""]

open(f"{OUT}/COMPARE_NOTHRESH.md", "w").write("\n".join(md))
print("\n".join(md))
