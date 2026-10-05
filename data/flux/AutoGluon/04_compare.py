#!/usr/bin/env python
"""Step 4 - put AutoGluon next to the tuned XGBoost and LightGBM results (same repeated LOSO, same data).

Outputs (AutoGluon/outputs/<set>/):
  comparison_repCV.csv        RMSE / MAE / R2 per learner x model x response; learners = XGB, LGB (Optuna),
                              AutoGluon (ensemble) and every AutoGluon base learner under the same LOSO folds
  comparison_disturbance.csv  effect of adding mortality: dRMSE % (M4 vs M3) per learner x window x response
  comparison_shap_groups.csv  M4: share of mean |SHAP| per predictor group and mean signed disturbance SHAP
  ensemble_weights_loso.csv   mean AutoGluon ensemble weight per base learner over all LOSO fits
and AutoGluon/RESULTS_<set>.md with the same tables in readable form.
    python 04_compare.py <efp|flux>
"""
import os, sys, glob
import numpy as np, pandas as pd
import ag_common as C

RESP_SET = sys.argv[1]
out = C.out_dir(RESP_SET)
ref = C.SETS[RESP_SET]["ref"]
RESP = C.SETS[RESP_SET]["responses"]
ENS = "AutoGluon"
md = [f"# AutoGluon vs XGBoost vs LightGBM - response set `{RESP_SET}`", ""]


def short(m):
    return m.replace("_BAG_L1", "")


# ---- 1. skill under repeated LOSO -------------------------------------------------------------------
rows = [pd.read_csv(f"{ref}/{L}_metrics_repCV.csv") for L in ("XGB", "LGB")]
f_by = os.path.join(out, "AG_metrics_repCV_by_model.csv")
have_loso = os.path.exists(f_by)
if have_loso:
    ag = pd.read_csv(f_by)
    ag["learner"] = np.where(ag["ag_model"] == ENS, ENS, "AG:" + ag["ag_model"].map(short))
    rows.append(ag[ag["ag_model"] != "WeightedEnsemble_L2"].drop(columns="ag_model"))
met = pd.concat(rows, ignore_index=True)
met.to_csv(os.path.join(out, "comparison_repCV.csv"), index=False)
main = [l for l in ("XGB", "LGB", ENS) if l in set(met["learner"])]
r2 = met.pivot_table(index=["response", "model"], columns="learner", values="R2")
r2 = r2.reindex([(r, m) for r in RESP for m in ("M3_12m", "M4_12m", "M3_24m", "M4_24m")])
md += ["## 1. Skill, repeated leave-one-site-out CV (R2)", "",
       "XGB / LGB = Optuna-tuned (paper runs). AutoGluon = weighted ensemble, rebuilt inside every fold.", "",
       r2[main].round(3).to_markdown(), ""]
if have_loso:
    r2w = r2.copy(); r2w["best"] = r2w[main].idxmax(axis=1)
    md += ["Best of the three per task: " + ", ".join(f"{k} {v}" for k, v in r2w["best"].value_counts().items()),
           "", "Mean R2 over the four responses:", "",
           r2[main].groupby(level="model").mean().reindex(["M3_12m", "M4_12m", "M3_24m", "M4_24m"]).round(3).to_markdown(), "",
           "### Every AutoGluon base learner under the same LOSO folds (R2, default hyperparameters)", "",
           r2[[c for c in r2.columns if c.startswith("AG:")]].round(3).to_markdown(), "",
           "Mean R2 over all tasks, sorted:", "",
           r2.mean().sort_values(ascending=False).round(3).rename("mean_R2").to_frame().to_markdown(), ""]

# ---- 2. effect of mortality: M4 vs M3 ----------------------------------------------------------------
m2 = met.copy()
m2["window"] = m2["model"].str[3:]; m2["mod"] = m2["model"].str[:2]
w = m2.pivot_table(index=["learner", "window", "response"], columns="mod", values=["RMSE", "R2"])
d = pd.DataFrame(dict(RMSE_M3=w[("RMSE", "M3")], RMSE_M4=w[("RMSE", "M4")],
                      dRMSE_pct=100 * (w[("RMSE", "M4")] - w[("RMSE", "M3")]) / w[("RMSE", "M3")],
                      dR2=w[("R2", "M4")] - w[("R2", "M3")])).reset_index()
d.to_csv(os.path.join(out, "comparison_disturbance.csv"), index=False)
dp = d[d["learner"].isin(main)].pivot_table(index=["response", "window"], columns="learner", values="dRMSE_pct")
dp = dp.reindex([(r, x) for r in RESP for x in C.WINDOWS])[main]
md += ["## 2. Effect of adding mortality: dRMSE % (M4 vs M3, negative = mortality helps)", "",
       dp.round(2).to_markdown(), ""]

# ---- 3. 12 vs 24 months -----------------------------------------------------------------------------
m4 = m2[(m2["mod"] == "M4") & m2["learner"].isin(main)].pivot_table(index=["response", "learner"], columns="window", values="R2")
m4["24m_minus_12m"] = m4["24m"] - m4["12m"]
md += ["## 3. 12 vs 24 months (M4, R2)", "", m4.reindex(RESP, level=0).round(3).to_markdown(), ""]

# ---- 4. SHAP group shares, M4 ------------------------------------------------------------------------
sh = []
for L, f in (("XGB", f"{ref}/XGB_site_signed_shap_M4.csv"), ("LGB", f"{ref}/LGB_site_signed_shap_M4.csv"),
             (ENS, os.path.join(out, "AG_site_signed_shap_M4.csv"))):
    if os.path.exists(f):
        s = pd.read_csv(f); s["learner"] = L; sh.append(s)
if sh:
    s = pd.concat(sh, ignore_index=True)
    g = s.groupby(["learner", "model", "response", "group"], as_index=False).agg(
        mean_abs_shap=("mean_abs_shap", "mean"), mean_signed_shap=("mean_signed_shap", "mean"), n_sites=("test_site", "nunique"))
    g["share_pct"] = 100 * g["mean_abs_shap"] / g.groupby(["learner", "model", "response"])["mean_abs_shap"].transform("sum")
    g.to_csv(os.path.join(out, "comparison_shap_groups.csv"), index=False)
    ls = [l for l in ("XGB", "LGB", ENS) if l in set(g["learner"])]
    idx = [(r, m) for r in RESP for m in ("M4_12m", "M4_24m")]
    for grp in ("Disturbance", "Climate", "Traits"):
        t = g[g["group"] == grp].pivot_table(index=["response", "model"], columns="learner", values="share_pct").reindex(idx)[ls]
        md += [f"## 4. SHAP, M4: {grp} share of mean |SHAP| (%)" if grp == "Disturbance" else f"### {grp} share (%)", "",
               t.round(1).to_markdown(), ""]
    t = g[g["group"] == "Disturbance"].pivot_table(index=["response", "model"], columns="learner", values="mean_signed_shap").reindex(idx)[ls]
    md += ["### Mean signed disturbance SHAP (response units, mean over sites)", "", t.round(3).to_markdown(), "",
           "XGB / LGB: TreeSHAP. AutoGluon: permutation SHAP of the ensemble (different baseline definition, "
           "so compare shares and signs, not the third decimal).", ""]

# ---- 5. what AutoGluon chose --------------------------------------------------------------------------
f_lb = os.path.join(out, "AG_leaderboard_groupCV.csv")
if os.path.exists(f_lb):
    lb = pd.read_csv(f_lb); lb = lb[lb["ag_model"] != "WeightedEnsemble_L2"]; lb["ag_model"] = lb["ag_model"].map(short)
    wt = lb.pivot_table(index=["response", "model_id"], columns="ag_model", values="ensemble_weight")
    wt = wt.reindex([(r, m) for r in RESP for m in ("M3_12m", "M4_12m", "M3_24m", "M4_24m")])
    md += ["## 5. What AutoGluon chose: ensemble weights, fit on all site-years (the models explained by SHAP)", "",
           wt.loc[:, wt.sum() > 0].round(2).to_markdown(), ""]
wf = glob.glob(os.path.join(C.AG_ROOT, "cache", "loso", RESP_SET, "*", "*_weights.json"))
if wf:
    ww = []
    for f in wf:
        x = pd.read_json(f); x["task"] = os.path.basename(os.path.dirname(f)); ww.append(x)
    ww = pd.concat(ww, ignore_index=True)
    cols = [c for c in ww.columns if c.endswith("_BAG_L1")]
    mw = ww.groupby("task")[cols].apply(lambda x: x.fillna(0).mean()).rename(columns=short)
    mw.to_csv(os.path.join(out, "ensemble_weights_loso.csv"))
    md += [f"Mean ensemble weight over all LOSO fits ({len(ww)} fits so far), all tasks pooled:", "",
           mw.mean().sort_values(ascending=False).round(3).rename("mean_weight").to_frame().to_markdown(), ""]

open(os.path.join(C.AG_ROOT, f"RESULTS_{RESP_SET}.md"), "w").write("\n".join(md))
print("\n".join(md))
