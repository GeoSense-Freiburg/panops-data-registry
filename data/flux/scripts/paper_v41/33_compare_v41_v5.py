#!/usr/bin/env python
"""
v4.1 vs v5 deadtrees maps: does the choice of map version change the paper's results?

Both datasets are built with identical rules (03_build_paper_dataset.R, PAPER_MAPS_VERSION=v41 | v5) and analysed
with identical code (run_rerun_qc.sh / run_v5.sh). Compares
  1. the datasets: sites and site-years kept, metric agreement on shared site-years, high-mortality classes
  2. skill: R2 per learner x model x EFP (all sites of each version, and on the shared site-years only)
  3. the mortality effect: dRMSE % (C+T -> C+T+M) and the Fig 2 high-mortality statistics
  4. the direction: Fig 4 statistics (signed mortality SHAP, High vs Low+Mid)
Outputs: plots/paper_v5/comparison_v41_v5/  (CSV per section + COMPARISON.md)
    python 33_compare_v41_v5.py
"""
import os
import numpy as np, pandas as pd

ROOT = "/mnt/gsdata/projects/panops/panops-data-registry/data/flux"
# env COMPARE_SUFFIX="_nothresh" compares the runs without the thresholded mortality metrics (datasets are the same)
SFX = os.environ.get("COMPARE_SUFFIX", "")
DS = {"v4.1": f"{ROOT}/derived_tables/paper_v41", "v5": f"{ROOT}/derived_tables/paper_v5"}       # datasets
MOD = {v: f"{p}{SFX}/models" for v, p in DS.items()}                                              # model outputs
FIG = {"v4.1": f"{ROOT}/plots/paper_v41{SFX}", "v5": f"{ROOT}/plots/paper_v5{SFX}"}
DATA = "paper_dataset_full_lags_monthly_tc30_minrun2_buf100-200-300-400-500.csv"
OUT = f"{ROOT}/plots/paper_v5{SFX}/comparison_v41_v5"
os.makedirs(OUT, exist_ok=True)
EFP = ["GPPsat", "NEPmax", "ETmax", "WUE"]
B = [100, 200, 300, 400, 500]
MET = {"relative_disturbance": "relative mortality", "relative_mortality": "relative deadwood",
       "absolute_mortality": "absolute deadwood", "tree_cover_mean_pct": "tree cover (500 m)"}
md = ["# deadtrees v4.1 vs v5 - effect on the paper's EFP analysis", ""]


def tbl(df, nd=3):
    return df.round(nd).to_markdown()


# ---- 1. datasets -------------------------------------------------------------------------
d = {v: pd.read_csv(f"{p}/{DATA}") for v, p in DS.items()}
s41, s5 = set(d["v4.1"].SITE_ID), set(d["v5"].SITE_ID)
k = lambda x: set(zip(x.SITE_ID, x.YEAR))
sy41, sy5 = k(d["v4.1"]), k(d["v5"])
md += ["## 1. Datasets (identical build rules)", "",
       f"- v4.1: {len(s41)} sites / {len(sy41)} site-years; v5: {len(s5)} sites / {len(sy5)} site-years",
       f"- shared: {len(s41 & s5)} sites / {len(sy41 & sy5)} site-years",
       f"- only in v4.1: {sorted(s41 - s5)}", f"- only in v5: {sorted(s5 - s41)}", ""]
for v in d:
    for m in MET:   # mean of the five buffers (tree cover exists only at 500 m in the paper dataset)
        cols = [c for c in (f"{m}_{b}m" for b in B) if c in d[v].columns]
        d[v][m + "_avg"] = d[v][cols].mean(axis=1)
mm = d["v4.1"].merge(d["v5"], on=["SITE_ID", "YEAR"], suffixes=("_41", "_5"))
rows = []
for m, lab in MET.items():
    a, b = mm[m + "_avg_41"], mm[m + "_avg_5"]
    rows.append(dict(metric=lab, mean_v41=a.mean(), mean_v5=b.mean(), median_change=(b - a).median(),
                     r_site_years=a.corr(b), r_spearman=a.corr(b, method="spearman")))
agree = pd.DataFrame(rows).set_index("metric")
agree.to_csv(f"{OUT}/1_metric_agreement.csv")
md += ["Mean of the five buffers, shared site-years:", "", tbl(agree), ""]


def high_sites(df, col):
    s = df.groupby("SITE_ID")[col].max()
    return set(s[s > s.mean() + 0.5 * s.std()].index)


cls = []
for m in ["relative_disturbance", "relative_mortality"]:
    h41, h5 = high_sites(d["v4.1"], m + "_avg"), high_sites(d["v5"], m + "_avg")
    cls.append(dict(metric=MET[m], high_v41=len(h41), high_v5=len(h5), both=len(h41 & h5),
                    only_v41=", ".join(sorted(h41 - h5)), only_v5=", ".join(sorted(h5 - h41))))
cls = pd.DataFrame(cls).set_index("metric")
cls.to_csv(f"{OUT}/1_high_mortality_sites.csv")
md += ["High-mortality sites (site maximum > mean + 0.5 SD, mean of the five buffers):", "", cls.to_markdown(), ""]

# ---- 2. skill ----------------------------------------------------------------------------
met, pr = [], []
for v in DS:
    for L in ("XGB", "LGB"):
        x = pd.read_csv(f"{MOD[v]}/{L}_metrics_repCV.csv"); x["maps"] = v; met.append(x)
        p = pd.read_csv(f"{MOD[v]}/{L}_predictions_repCV.csv"); p["maps"] = v; p["learner"] = L; pr.append(p)
met, pr = pd.concat(met), pd.concat(pr)
r2 = met.pivot_table(index=["response", "model"], columns=["learner", "maps"], values="R2")
r2 = r2.reindex([(r, m) for r in EFP for m in ["M3_12m", "M4_12m", "M3_24m", "M4_24m"]])
r2.to_csv(f"{OUT}/2_r2_all_sites.csv")
shared = set(map(tuple, pr.loc[pr.maps == "v4.1", ["SITE_ID", "YEAR"]].drop_duplicates().values)) & \
         set(map(tuple, pr.loc[pr.maps == "v5", ["SITE_ID", "YEAR"]].drop_duplicates().values))
ps = pr[[t in shared for t in zip(pr.SITE_ID, pr.YEAR)]]
r2s = ps.groupby(["response", "model", "learner", "maps"]).apply(
    lambda s: 1 - ((s.observed - s.predicted) ** 2).sum() / ((s.observed - s.observed.mean()) ** 2).sum()).unstack(["learner", "maps"])
r2s = r2s.reindex(r2.index)
r2s.to_csv(f"{OUT}/2_r2_shared_site_years.csv")
mean_r2 = pd.DataFrame({"all sites": r2.mean(), "shared site-years": r2s.mean()}).T
md += ["## 2. Skill (R2, repeated leave-one-site-out)", "", "Each version on its own sites:", "", tbl(r2), "",
       f"Only the {len(shared)} site-years present in both datasets (removes the effect of different site sets):", "",
       tbl(r2s), "", "Mean over all 16 tasks:", "", tbl(mean_r2), ""]

# ---- 3. mortality effect -----------------------------------------------------------------
m2 = met.copy(); m2["window"] = m2.model.str[3:]; m2["mod"] = m2.model.str[:2]
w = m2.pivot_table(index=["response", "window"], columns=["learner", "maps", "mod"], values="RMSE")
dr = pd.DataFrame({(L, v): 100 * (w[(L, v, "M4")] - w[(L, v, "M3")]) / w[(L, v, "M3")]
                   for L in ("XGB", "LGB") for v in DS})
dr = dr.reindex([(r, x) for r in EFP for x in ("12m", "24m")])
dr.to_csv(f"{OUT}/3_dRMSE_pct.csv")
md += ["## 3. Effect of adding mortality: dRMSE % (C+T -> C+T+M, overall RMSE; negative = mortality helps)", "", tbl(dr, 2), ""]
for f, lab, tag in [("fig2_statistics.csv", "all sites", "all"),
                    ("fig2_high_total_mort_statistics.csv", "high relative-mortality sites", "high_total_mort"),
                    ("fig2_high_rel_deadwood_statistics.csv", "high relative-deadwood sites", "high_rel_deadwood")]:
    s = []
    for v in DS:
        x = pd.read_csv(f"{FIG[v]}/{f}"); x["maps"] = v; s.append(x)
    s = pd.concat(s)
    s["cell"] = s.median_pct.round(1).astype(str) + s.sig.replace({"ns": ""}).fillna("") + " (" + s.sites_improved.round(0).astype(int).astype(str) + "%)"
    t = s.pivot_table(index=["response", "grp"], columns=["learner", "maps"], values="cell", aggfunc="first")
    t = t.reindex([(r, x) for r in EFP for x in ("12m", "24m")])
    t.to_csv(f"{OUT}/3_fig2_{tag}.csv")
    md += [f"Fig 2, {lab}: median per-site change % with stars (share of sites improved):", "", t.to_markdown(), ""]

# ---- 4. direction (Fig 4) ----------------------------------------------------------------
s = []
for v in DS:
    x = pd.read_csv(f"{FIG[v]}/fig4_statistics.csv"); x["maps"] = v; s.append(x)
s = pd.concat(s)
s["cell"] = s.med_high.round(3).astype(str) + " vs " + s.med_low.round(3).astype(str) + s.sig.replace({"ns": " ns"}).fillna("")
t = s.pivot_table(index=["metric", "response", "window"], columns=["learner", "maps"], values="cell", aggfunc="first")
t.to_csv(f"{OUT}/4_fig4_direction.csv")
nsig = s.groupby(["metric", "maps"]).apply(lambda x: f"{(x.q < 0.05).sum()} of {len(x)}").unstack()
md += ["## 4. Direction (Fig 4): median net signed mortality SHAP, High vs Low+Mid", "", t.to_markdown(), "",
       "Significant tests per metric:", "", nsig.to_markdown(), ""]

open(f"{OUT}/COMPARISON.md", "w").write("\n".join(md))
print("\n".join(md))
