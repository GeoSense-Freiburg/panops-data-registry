#!/usr/bin/env python
"""
Report for https://negin-katal.github.io/fluxVSmortality/v10/  (fluxVSmortality/v10/index.html), rebuilt 2026-10-06.

Replaces the earlier V10/V4.1 report (27_build_report_v10_v41.py). Three datasets, switchable at the top:
  V4.1  deadtrees v4.1 maps - the paper dataset (104 sites / 381 site-years)
  V5    deadtrees v5 maps, same build rules (101 / 361)
  V2.2  deadtrees v2-2 maps (the analysis earlier called V10; 93 / 395) - only the figures that exist in the
        same form as for V4.1 / V5
Paper structure and naming (memory: paper_naming_mortality): models C+T vs C+T+M; metrics "total mortality"
(relative_disturbance) and "relative deadwood" (relative_mortality); absolute deadwood is not reported.
Fig 3 is the landscape signed-SHAP figure. Table 1 = the Fig 2 summary table (also linked as .tex).
Figures are copied into fluxVSmortality/v10/fig_<ds>/ so the page is self-contained.
Publish afterwards with:  bash scripts/push_fluxVSmortality.sh "message" v10
    python 35_build_report_paper.py
"""
import os, shutil, html, datetime
import numpy as np, pandas as pd

ROOT = "/mnt/gsdata/projects/panops/panops-data-registry/data/flux"
SITE = f"{ROOT}/fluxVSmortality/v10"
EFP = ["GPPsat", "NEPmax", "ETmax", "WUE"]
# V4.1 and V5 are shown WITHOUT the thresholded mortality metrics (user, 2026-10-06; runs <sub>_nothresh).
# Set REPORT_SUFFIX="" to show the earlier runs with thresholds.
SFX = os.environ.get("REPORT_SUFFIX", "_nothresh")
EFPH = {"GPPsat": "GPP<sub>sat</sub>", "NEPmax": "NEP<sub>max</sub>", "ETmax": "ET<sub>max</sub>", "WUE": "WUE"}
DS = {
    "v41": dict(label="V4.1", maps="deadtrees v4.1", plots=f"{ROOT}/plots/paper_v41{SFX}", models=f"{ROOT}/derived_tables/paper_v41{SFX}/models",
                data=f"{ROOT}/derived_tables/paper_v41/paper_dataset_full_lags_monthly_tc30_minrun2_buf100-200-300-400-500.csv",
                tag="", cls="mean of the five buffers (100-500 m)", tex=f"table_fig2_dRMSE_total_mort_v41{SFX}.tex",
                note="The paper dataset."),
    "v5": dict(label="V5", maps="deadtrees v5", plots=f"{ROOT}/plots/paper_v5{SFX}", models=f"{ROOT}/derived_tables/paper_v5{SFX}/models",
               data=f"{ROOT}/derived_tables/paper_v5/paper_dataset_full_lags_monthly_tc30_minrun2_buf100-200-300-400-500.csv",
               tag="", cls="mean of the five buffers (100-500 m)", tex=f"table_fig2_dRMSE_total_mort_v5{SFX}.tex",
               note="Newest maps (delivered 2026-10-06), built and analysed with exactly the V4.1 rules."),
    "v22": dict(label="V2.2", maps="deadtrees v2-2", plots=f"{ROOT}/plots/paper_v10_efp", models=f"{ROOT}/derived_tables/paper_v10_efp/models",
                data=f"{ROOT}/derived_tables/paper_v10_efp/paper_dataset_v10_efp.csv",
                tag="_cls500m", cls="500 m buffer (the only buffer stored for V2.2)", tex="table_fig2_dRMSE_total_mort_v22.tex",
                note="The earlier analysis (called V10): deadtrees v2-2 maps, V10 predictors. Only figures that exist in the same form as for V4.1 and V5 are shown."),
}


# ---------------------------------------------------------------- helpers
def copy_fig(ds, name):
    """Copy plots/<ds>/<name> into the site folder; return the relative path or None."""
    src = f"{DS[ds]['plots']}/{name}"
    if not os.path.exists(src):
        return None
    dst = f"{SITE}/fig_{ds}/{name}"
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    for attempt in range(5):   # the network drive occasionally drops for a moment
        try:
            shutil.copyfile(src, dst); break
        except OSError:
            import time; time.sleep(5)
    else:
        raise
    return f"fig_{ds}/{name}"


def fig(ds, name, caption):
    rel = copy_fig(ds, name)
    if rel is None:
        return ""   # figure does not exist for this dataset (V2.2 has only the paper-style figures)
    return (f'<figure><a href="{rel}"><img src="{rel}" loading="lazy" alt="{html.escape(caption)}"></a>'
            f'<figcaption>{caption}</figcaption></figure>')


def per_ds(make):
    """One block per dataset; only the active dataset is shown."""
    out = []
    for k in DS:
        b = make(k)
        if "<img" not in b and "<table" not in b:
            b = f'<p class="na">Not available for {DS[k]["label"]}: this figure was not produced in the same form.</p>'
        out.append(f'<div class="ds ds-{k}">{b}</div>')
    return "".join(out)


def table_html(df, caption=""):
    h = df.to_html(escape=False, border=0, classes="tbl")
    return f'<figure class="tw">{h}<figcaption>{caption}</figcaption></figure>'


def counts(ds):
    d = pd.read_csv(DS[ds]["data"], usecols=["SITE_ID"])
    return d.SITE_ID.nunique(), len(d)


def r2_table(ds):
    rows = []
    for L, lab in (("XGB", "XGBoost"), ("LGB", "LightGBM")):
        p = pd.read_csv(f"{DS[ds]['models']}/{L}_predictions_repCV.csv")
        p = p[p.response.isin(EFP) & p.model.str.match(r"^M[34]_(12m|24m)$")]
        for (m, r), s in p.groupby(["model", "response"]):
            r2 = 1 - ((s.observed - s.predicted) ** 2).sum() / ((s.observed - s.observed.mean()) ** 2).sum()
            rows.append(dict(learner=lab, window=m[3:].replace("m", " months"), model="C+T+M" if m[:2] == "M4" else "C+T",
                             EFP=EFPH[r], R2=r2))
    t = pd.DataFrame(rows).pivot_table(index=["learner", "window", "model"], columns="EFP", values="R2")
    t = t[[EFPH[e] for e in EFP]]
    t["mean"] = t.mean(axis=1)
    t = t.reindex(sorted(t.index, key=lambda i: (i[0], i[1], i[2] != "C+T")))
    t.index = [f"{a} · {b} · {c}" for a, b, c in t.index]
    return t.round(3)


def fig2_table(ds):
    f = f"{DS[ds]['plots']}/table_fig2_dRMSE_total_mort_statistics.csv"
    st = pd.read_csv(f)
    lab = {"XGB": "XGBoost", "LGB": "LightGBM", "AG": "AutoGluon"}
    st["cell"] = [f"{m:+.1f}{'<sup>' + s + '</sup>' if isinstance(s, str) and s else ''} <span class='muted'>({i:.0f}%)</span>"
                  for m, s, i in zip(st.median_pct, st.sig, st.improved_pct)]
    t = st.pivot_table(index=["learner", "window", "response"], columns="subset", values="cell", aggfunc="first")
    t = t[["all", "midhigh", "high"]]
    n = st.groupby("subset").n.max()
    t.columns = [f"All sites (n = {n['all']})", f"Mid + High (n = {n['midhigh']})", f"High (n = {n['high']})"]
    order = [(L, w, e) for L in ("XGB", "LGB", "AG") for w in ("12m", "24m") for e in EFP if (L, w, e) in t.index]
    t = t.reindex(order)
    t.index = [f"{lab[L]} · {w.replace('m', ' months')} · {EFPH[e]}" for L, w, e in t.index]
    return t


def copy_tex(ds):
    src = f"{ROOT}/manuscript/tables/{DS[ds]['tex']}"
    os.makedirs(f"{SITE}/tables", exist_ok=True)
    shutil.copyfile(src, f"{SITE}/tables/{DS[ds]['tex']}")
    return f"tables/{DS[ds]['tex']}"


T = lambda ds: DS[ds]["tag"]

# ---------------------------------------------------------------- sections
for k in DS:
    shutil.rmtree(f"{SITE}/fig_{k}", ignore_errors=True)
sections = []

about = f"""
<p>How does tree mortality shape ecosystem functional properties (EFPs)? For every flux site-year the EFPs
GPP<sub>sat</sub>, NEP<sub>max</sub>, ET<sub>max</sub> and WUE are predicted from <b>climate + traits (C+T)</b> and from
<b>climate + traits + mortality (C+T+M)</b>. The difference between the two models isolates what mortality adds.</p>
<ul>
<li><b>Learners:</b> XGBoost and LightGBM, tuned with Optuna (TPE, 50 trials, grouped 5-fold CV on disjoint sites).</li>
<li><b>Evaluation:</b> repeated leave-one-site-out (3 × 80 % of the remaining sites per held-out site).</li>
<li><b>Windows:</b> climate in the 12 or 24 months before the growing season; mortality of the current year plus lag 1 (12 months) or lags 1-2 (24 months).</li>
<li><b>Sites:</b> tree cover ≥ 30 %.</li>
</ul>
<h3>Naming</h3>
<table class="tbl gl"><tr><th>Name</th><th>Definition</th></tr>
<tr><td>Total mortality</td><td>(standing deadwood + tree-cover loss) / (tree cover + tree-cover loss) × 100 - dead and lost canopy relative to the canopy present</td></tr>
<tr><td>Relative deadwood</td><td>standing deadwood / tree cover × 100 (where tree cover ≥ 10 %, capped at 100)</td></tr>
<tr><td>Mortality block (M)</td><td>standing deadwood (cover and relative to tree cover), deadwood gain, relative tree-cover loss and total mortality, at all five buffers and lags (V4.1 / V5: no thresholded gain or loss metrics)</td></tr>
<tr><td>Low / Mid / High</td><td>site maximum of the metric across years; breaks mean − 0.5 SD and mean + 0.5 SD over sites</td></tr>
</table>
<h3>Datasets</h3>
<table class="tbl gl"><tr><th>Dataset</th><th>Maps</th><th>Sites / site-years</th><th>Site classes from</th><th>Note</th></tr>
{''.join(f"<tr><td><b>{d['label']}</b></td><td>{d['maps']}</td><td>{counts(k)[0]} / {counts(k)[1]}</td><td>{d['cls']}</td><td>{d['note']}</td></tr>" for k, d in DS.items())}
</table>"""
sections.append(("about", "About this report", about))

sections.append(("sites", "1. Sites and EFP distributions",
                 per_ds(lambda k: fig(k, "fig1_site_map.png", f"Fig 1 ({DS[k]['label']}): site map and EFP distributions."))))

sections.append(("skill", "2. Model skill",
                 per_ds(lambda k: table_html(r2_table(k), f"R² under repeated leave-one-site-out ({DS[k]['label']})."))))

s3 = per_ds(lambda k: fig(k, "fig2_mortality_effect.png",
            f"Fig 2 ({DS[k]['label']}): per-site RMSE without (C+T) and with mortality (C+T+M); % = median paired change, "
            "stars = one-sided paired Wilcoxon, BH-corrected; %↓ = share of sites improved."))
s3 += "<h3>Table 1 - summary of Fig 2 by total-mortality class</h3>"
s3 += per_ds(lambda k: table_html(fig2_table(k),
             f"Median per-site change in RMSE when mortality is added (%), stars = one-sided paired Wilcoxon, BH-corrected across the table; "
             f"(share of sites improved). Classes by total mortality, {DS[k]['cls']}. "
             f"LaTeX: <a href='{copy_tex(k)}'>{DS[k]['tex']}</a>."))
s3 += "<h3>High-mortality sites</h3>"
s3 += per_ds(lambda k: fig(k, f"fig2_high_total_mort{T(k)}_mortality_effect.png", f"Fig 2b ({DS[k]['label']}): high total-mortality sites only (models trained on all sites).")
             + fig(k, f"fig2_high_rel_deadwood{T(k)}_mortality_effect.png", f"Fig 2b ({DS[k]['label']}): high relative-deadwood sites only."))
s3 += "<h3>Per-site ΔRMSE</h3>"
s3 += per_ds(lambda k: fig(k, f"fig_site_dRMSE_total_mort{T(k)}.png", f"Per-site ΔRMSE ({DS[k]['label']}), bars coloured by total-mortality class."))
sections.append(("effect", "3. Does mortality reduce prediction error?", s3))


def fig3(k):
    out = fig(k, f"fig3b_signed_total_mort_24m_LGB{T(k)}_landscape.png",
              f"Fig 3 ({DS[k]['label']}): net signed mortality SHAP per site, C+T+M, LightGBM, 24 months. Sites sorted by total mortality "
              "(highest left); below zero = mortality lowers the predicted EFP.")
    more = "".join(fig(k, f"fig3b_signed_total_mort_{w}_{L}{T(k)}_landscape.png", f"{DS[k]['label']}: {lab}, {w.replace('m', ' months')}.")
                   for L, lab in (("LGB", "LightGBM"), ("XGB", "XGBoost")) for w in ("12m", "24m") if not (L == "LGB" and w == "24m"))
    comp = fig(k, f"fig3_total_mort_24m_LGB{T(k)}_landscape.png", f"{DS[k]['label']}: driver composition (relative |SHAP|), LightGBM, 24 months.")
    return out + f"<details><summary>LightGBM 12 months, XGBoost 12 and 24 months{', driver composition' if comp else ''}</summary>{more}{comp}</details>"


sections.append(("shap", "4. Direction of the mortality effect at each site (Fig 3)", per_ds(fig3)))

sections.append(("direction", "5. High vs Low+Mid mortality sites (Fig 4)",
                 per_ds(lambda k: fig(k, f"fig4_signed_shap_total_mort{T(k)}.png", f"Fig 4 ({DS[k]['label']}): net signed mortality SHAP, High vs Low+Mid total-mortality sites; Mann-Whitney U, BH-corrected.")
                        + fig(k, f"fig4_signed_shap_rel_deadwood{T(k)}.png", f"Fig 4 ({DS[k]['label']}): split by relative deadwood."))))

sections.append(("igbp", "6. Ecosystem type and tree cover (Fig 5)",
                 per_ds(lambda k: fig(k, "fig5_igbp_board_24m_XGB.png", f"Fig 5 ({DS[k]['label']}): benefit and direction by IGBP class, tree cover and total mortality; XGBoost, 24 months.")
                        + (lambda m: f"<details><summary>12 months and LightGBM</summary>{m}</details>" if m else "")(
                            "".join(fig(k, f"fig5_igbp_board_{w}_{L}.png", f"{DS[k]['label']}: {L}, {w}.") for L in ("XGB", "LGB") for w in ("12m", "24m") if not (L == "XGB" and w == "24m")))
                        + fig(k, "mortality_split/mortality_composition_concept_LGB.png", f"{DS[k]['label']}: what kind of mortality information the model uses (LightGBM)."))))

# V4.1 vs V5 comparison (always visible)
C = f"{ROOT}/plots/paper_v5{SFX}/comparison_v41_v5"
agree = pd.read_csv(f"{C}/1_metric_agreement.csv", index_col=0).drop(index="absolute deadwood", errors="ignore").round(3)
hi = pd.read_csv(f"{C}/1_high_mortality_sites.csv", index_col=0)
r2c = pd.read_csv(f"{C}/2_r2_all_sites.csv", header=[0, 1], index_col=[0, 1]).mean()
r2s = pd.read_csv(f"{C}/2_r2_shared_site_years.csv", header=[0, 1], index_col=[0, 1]).mean()
r2m = pd.DataFrame({"own sites": r2c, "shared site-years": r2s}).T.round(3)
r2m.columns = [f"{'XGBoost' if a == 'XGB' else 'LightGBM'} {b.replace('v4.1', 'V4.1').replace('v5', 'V5')}" for a, b in r2m.columns]
dr = pd.read_csv(f"{C}/3_dRMSE_pct.csv", header=[0, 1], index_col=[0, 1]).round(2)
dr.columns = [f"{'XGBoost' if a == 'XGB' else 'LightGBM'} {b.replace('v4.1', 'V4.1').replace('v5', 'V5')}" for a, b in dr.columns]
dr.index = [f"{EFPH[e]} · {w.replace('m', ' months')}" for e, w in dr.index]
cmp = f"""<p>V5 rebuilds the paper dataset from the newest maps with identical rules. It keeps 101 of the 104 V4.1 sites
(loses CA-DB2, CA-LU1, FI-Var, US-BZB through the tree-cover filter, gains AU-How).</p>
{table_html(agree, "Mortality metrics (mean of the five buffers; tree cover at 500 m) on the site-years present in both datasets.")}
{table_html(hi, "High-mortality sites in each version.")}
{table_html(r2m, "Mean R² over the 16 tasks: each version on its own sites, and on the 357 site-years present in both.")}
{table_html(dr, "Change in overall RMSE when mortality is added (%), negative = mortality helps.")}
<ul><li><b>Robust:</b> the NEP<sub>max</sub> effect (−9 to −11 % overall in V4.1, −7 to −10 % in V5; −11 to −22 % at high-mortality sites, significant in both) and the direction of the effect (Fig 4).</li>
<li><b>Map-dependent:</b> the GPP<sub>sat</sub> effect is weaker with V5 (about −3 % instead of −7 % overall) and at high-mortality sites no longer significant; WUE is also weaker overall.</li></ul>"""
sections.append(("compare", "7. V4.1 vs V5", cmp))

# AutoGluon (AutoML) check - AutoGluon/ ; same datasets, predictors, windows and repeated LOSO as XGB / LGB
AGD = f"{ROOT}/AutoGluon"
AG_SET = {"v41": "efp", "v5": "efp_v5"}                       # V2.2 was not run with AutoGluon
AG_SFX = os.environ.get("AG_SUFFIX", "")   # "" = runs WITH the thresholded metrics; "_nothresh" once those reruns are done
AGL = {"XGB": "XGBoost", "LGB": "LightGBM", "AutoGluon": "AutoGluon"}


def ag_fig(k, name, caption):
    src = f"{AGD}/plots/{AG_SET[k]}{AG_SFX}/{name}"
    if not os.path.exists(src):
        return ""
    rel = f"fig_{k}/autogluon/{name}"
    os.makedirs(os.path.dirname(f"{SITE}/{rel}"), exist_ok=True)
    shutil.copyfile(src, f"{SITE}/{rel}")
    return (f'<figure><a href="{rel}"><img src="{rel}" loading="lazy" alt="{html.escape(caption)}"></a>'
            f'<figcaption>{caption}</figcaption></figure>')


def ag_block(k):
    if k not in AG_SET:
        return ""
    o = f"{AGD}/outputs/{AG_SET[k]}{AG_SFX}"
    if not os.path.exists(f"{o}/comparison_repCV.csv"):
        return ""
    lab = DS[k]["label"]
    rep = pd.read_csv(f"{o}/comparison_repCV.csv")
    rep = rep[rep.learner.isin(AGL)]
    rep["task"] = [f"{EFPH[r]} · {m[3:].replace('m', ' months')} · {'C+T+M' if m[:2] == 'M4' else 'C+T'}" for r, m in zip(rep.response, rep.model)]
    order = [f"{EFPH[e]} · {w} · {c}" for e in EFP for w in ("12 months", "24 months") for c in ("C+T", "C+T+M")]
    r2 = rep.pivot_table(index="task", columns="learner", values="R2").reindex(order)[list(AGL)]
    r2.loc["mean"] = r2.mean()
    r2.columns = [AGL[c] for c in r2.columns]
    dist = pd.read_csv(f"{o}/comparison_disturbance.csv")
    dist = dist[dist.learner.isin(AGL)]
    dist["task"] = [f"{EFPH[r]} · {w.replace('m', ' months')}" for r, w in zip(dist.response, dist.window)]
    dr = dist.pivot_table(index="task", columns="learner", values="dRMSE_pct").reindex(
        [f"{EFPH[e]} · {w}" for e in EFP for w in ("12 months", "24 months")])[list(AGL)]
    dr.columns = [AGL[c] for c in dr.columns]
    sh = pd.read_csv(f"{o}/comparison_shap_groups.csv")
    sh = sh[(sh.group == "Disturbance") & sh.learner.isin(AGL)]
    sh["task"] = [f"{EFPH[r]} · {m[3:].replace('m', ' months')}" for r, m in zip(sh.response, sh.model)]
    ms = sh.pivot_table(index="task", columns="learner", values="share_pct").reindex(dr.index)[list(AGL)]
    ms.columns = [AGL[c] for c in ms.columns]
    n_best = (r2.drop(index="mean").idxmax(axis=1) == "AutoGluon").sum()
    m = r2.loc["mean"]
    bullets = (f"<ul><li><b>Skill:</b> mean R² {m['AutoGluon']:.3f} (AutoGluon) vs {m['XGBoost']:.3f} (XGBoost) and {m['LightGBM']:.3f} "
               f"(LightGBM); AutoGluon is best in {n_best} of 16 tasks.</li>"
               f"<li><b>Effect of mortality:</b> overall RMSE change when mortality is added, AutoGluon "
               f"{dr['AutoGluon'].min():+.1f} to {dr['AutoGluon'].max():+.1f} % (XGBoost {dr['XGBoost'].min():+.1f} to {dr['XGBoost'].max():+.1f} %, "
               f"LightGBM {dr['LightGBM'].min():+.1f} to {dr['LightGBM'].max():+.1f} %).</li>"
               f"<li><b>Mortality share of |SHAP|</b> in C+T+M: AutoGluon {ms['AutoGluon'].min():.0f}-{ms['AutoGluon'].max():.0f} %, "
               f"XGBoost {ms['XGBoost'].min():.0f}-{ms['XGBoost'].max():.0f} %, LightGBM {ms['LightGBM'].min():.0f}-{ms['LightGBM'].max():.0f} % "
               "(AutoGluon SHAP is permutation-based, so compare shares and signs rather than magnitudes).</li></ul>")
    return (bullets
            + table_html(r2.round(3), f"R² under repeated leave-one-site-out ({lab}); AutoGluon = weighted ensemble rebuilt inside every fold.")
            + table_html(dr.round(2), f"Change in overall RMSE when mortality is added (%), negative = mortality helps ({lab}).")
            + table_html(ms.round(1), f"Mortality share of mean |SHAP| in C+T+M (%) ({lab}).")
            + ag_fig(k, "fig2_mortality_effect.png", f"Fig 2 with AutoGluon ({lab}): per-site RMSE without and with mortality, XGBoost, LightGBM and AutoGluon.")
            + ag_fig(k, "fig3b_signed_total_mort_24m_AG_landscape.png", f"Fig 3 with AutoGluon ({lab}): net signed mortality SHAP per site, C+T+M, 24 months.")
            + ag_fig(k, "fig4_signed_shap_total_mort.png", f"Fig 4 with AutoGluon ({lab}): net signed mortality SHAP, High vs Low+Mid total-mortality sites."))


ag_intro = ("<p>AutoGluon-Tabular (AutoML) chooses the learner itself: in every fit it trains LightGBM (3 variants), CatBoost, XGBoost, "
            "random forest, extra trees and two neural networks, each bagged over site-disjoint folds, and builds a weighted ensemble. "
            "It is rerun from scratch inside every leave-one-site-out fit, so model choice never sees the held-out site. Datasets, "
            "predictors, windows and the repeated leave-one-site-out evaluation are those of the XGBoost / LightGBM runs. "
            "The ensembles are dominated by the neural networks.</p>"
            + ("<p class='muted'><b>Note:</b> these AutoGluon runs use the full mortality block <b>including</b> the thresholded gain and "
               "loss metrics (C+T+M with 213 / 368 predictors), unlike Sections 1-7, so they are compared here with the XGBoost / LightGBM "
               "runs that also included them. A rerun without the thresholded metrics is in progress.</p>" if AG_SFX == "" else
               "<p class='muted'>Without the thresholded mortality metrics, as in Sections 1-7.</p>"))
sections.append(("autogluon", "8. AutoGluon (AutoML) check", ag_intro + per_ds(ag_block)))

css = """
:root{--bg:#ffffff;--fg:#1b1f24;--muted:#5b6470;--line:#d9dde3;--card:#f6f7f9;--accent:#0f6660;--on:#ffffff}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){--bg:#15181c;--fg:#e8eaed;--muted:#a3abb5;--line:#343a42;--card:#1e2227;--accent:#5fc4b8;--on:#10302d}}
:root[data-theme="dark"]{--bg:#15181c;--fg:#e8eaed;--muted:#a3abb5;--line:#343a42;--card:#1e2227;--accent:#5fc4b8;--on:#10302d}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:15px/1.55 system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
header{position:sticky;top:0;z-index:5;background:var(--bg);border-bottom:1px solid var(--line);padding:8px 16px}
.bar{max-width:1240px;margin:0 auto;display:flex;flex-wrap:wrap;gap:10px;align-items:center}
.bar nav{display:flex;flex-wrap:wrap;gap:4px 12px;font-size:.88rem}.bar nav a{color:var(--accent);text-decoration:none}
.toggle{display:flex;gap:4px;margin-left:auto}.toggle button{border:1px solid var(--accent);background:transparent;color:var(--accent);border-radius:6px;padding:4px 12px;font:inherit;cursor:pointer}
.toggle button.on{background:var(--accent);color:var(--on)}
main{max-width:1240px;margin:0 auto;padding:16px 16px 64px}h1{font-size:1.6rem;margin:.3em 0}h2{font-size:1.25rem;margin-top:2.2em;border-bottom:1px solid var(--line);padding-bottom:.3em}
h3{font-size:1.05rem;margin-top:1.6em}p,li{max-width:85ch}.muted{color:var(--muted)}.na{color:var(--muted);font-style:italic}
.tw{overflow-x:auto;margin:10px 0 18px}.tbl{border-collapse:collapse;font-size:.83rem;font-variant-numeric:tabular-nums}
.tbl th,.tbl td{border-bottom:1px solid var(--line);padding:4px 9px;text-align:right;white-space:nowrap}.tbl th:first-child,.tbl td:first-child{text-align:left}
.tbl.gl td,.tbl.gl th{text-align:left;white-space:normal;vertical-align:top}.tbl thead th{color:var(--muted);font-weight:600}
figure{margin:12px 0 22px}figcaption{color:var(--muted);font-size:.85rem;margin-top:4px;max-width:110ch}
img{max-width:100%;height:auto;border:1px solid var(--line);border-radius:4px;background:#fff}
details{margin:8px 0 18px;border:1px solid var(--line);border-radius:6px;padding:6px 12px;background:var(--card)}summary{cursor:pointer;color:var(--accent)}
.ds{display:none}body[data-ds="v41"] .ds-v41,body[data-ds="v5"] .ds-v5,body[data-ds="v22"] .ds-v22{display:block}
"""
js = """
const KEY='fvm-ds';function setDs(k){document.body.dataset.ds=k;document.querySelectorAll('.toggle button').forEach(b=>b.classList.toggle('on',b.dataset.k===k));try{localStorage.setItem(KEY,k)}catch(e){}}
document.querySelectorAll('.toggle button').forEach(b=>b.addEventListener('click',()=>setDs(b.dataset.k)));
let s='v41';try{s=localStorage.getItem(KEY)||s}catch(e){}if(!['v41','v5','v22'].includes(s))s='v41';setDs(s);
"""
page = f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Mortality and EFPs</title><style>{css}</style></head><body data-ds="v41">
<header><div class="bar"><nav>{''.join(f'<a href="#{k}">{t.split(". ", 1)[-1]}</a>' for k, t, _ in sections)}</nav>
<div class="toggle" role="group" aria-label="Dataset">{''.join(f'<button data-k="{k}">{d["label"]}</button>' for k, d in DS.items())}</div></div></header>
<main><h1>Tree mortality and ecosystem functional properties</h1>
<p class="muted">Updated {datetime.date.today().isoformat()} · switch the dataset at the top right (V4.1 = paper, V5 = newest maps, V2.2 = earlier analysis) ·
built by scripts/paper_v41/35_build_report_paper.py</p>
{''.join(f'<section id="{k}"><h2>{t}</h2>{b}</section>' for k, t, b in sections)}
</main><script>{js}</script></body></html>"""
open(f"{SITE}/index.html", "w").write(page)
n = sum(len(fs) for dp, _, fs in os.walk(SITE) if dp.startswith(f"{SITE}/fig_"))
print(f"written {SITE}/index.html ({len(page)/1e3:.0f} kB), {n} figure files copied")
