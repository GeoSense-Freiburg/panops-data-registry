#!/usr/bin/env python
"""
HTML report: all results of the EFP analysis on the deadtrees v5 maps, next to v4.1.
Reads the comparison tables of 33_compare_v41_v5.py and the figures of both runs
(plots/paper_v41, plots/paper_v5); writes plots/paper_v5/v5_report.html. Images are linked with
relative paths, so the report must stay next to plots/paper_v5/ and plots/paper_v41/.
    python 34_build_v5_report.py
"""
import os, html, datetime
import numpy as np, pandas as pd

ROOT = "/mnt/gsdata/projects/panops/panops-data-registry/data/flux"
V5, V41 = f"{ROOT}/plots/paper_v5", f"{ROOT}/plots/paper_v41"
CMP = f"{V5}/comparison_v41_v5"
OUT = f"{V5}/v5_report.html"
EFP = ["GPPsat", "NEPmax", "ETmax", "WUE"]
EFP_HTML = {"GPPsat": "GPP<sub>sat</sub>", "NEPmax": "NEP<sub>max</sub>", "ETmax": "ET<sub>max</sub>", "WUE": "WUE"}


def table(df, caption="", fmt="{:.3f}"):
    df = df.copy()
    for c in df.columns:
        if pd.api.types.is_float_dtype(df[c]):
            df[c] = df[c].map(lambda v: "" if pd.isna(v) else fmt.format(v))
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [" ".join(str(x) for x in c) for c in df.columns]
    if isinstance(df.index, pd.MultiIndex):
        df.index = [" · ".join(str(x) for x in i) for i in df.index]
    h = df.to_html(escape=True, border=0, classes="tbl")
    return f'<figure class="tw">{h}<figcaption>{caption}</figcaption></figure>' if caption else f'<div class="tw">{h}</div>'


def img(path, caption):
    rel = os.path.relpath(path, V5)
    if not os.path.exists(path):
        return f'<p class="missing">missing: {html.escape(rel)}</p>'
    return f'<figure><a href="{rel}"><img src="{rel}" loading="lazy" alt="{html.escape(caption)}"></a><figcaption>{caption}</figcaption></figure>'


def pair(name41, name5, caption):
    return (f'<div class="pair"><div><h4>v4.1</h4>{img(f"{V41}/{name41}", "v4.1 · " + caption)}</div>'
            f'<div><h4>v5</h4>{img(f"{V5}/{name5}", "v5 · " + caption)}</div></div>')


rd = lambda f, **k: pd.read_csv(f"{CMP}/{f}", **k)
agree = rd("1_metric_agreement.csv", index_col=0)
hi = rd("1_high_mortality_sites.csv", index_col=0)
r2 = rd("2_r2_all_sites.csv", header=[0, 1], index_col=[0, 1])
r2s = rd("2_r2_shared_site_years.csv", header=[0, 1], index_col=[0, 1])
dr = rd("3_dRMSE_pct.csv", header=[0, 1], index_col=[0, 1])
f2a = rd("3_fig2_all.csv", header=[0, 1], index_col=[0, 1]) if os.path.exists(f"{CMP}/3_fig2_all.csv") else None
f2h = rd("3_fig2_high_total_mort.csv", header=[0, 1], index_col=[0, 1])
f2r = rd("3_fig2_high_rel_deadwood.csv", header=[0, 1], index_col=[0, 1])
f4 = rd("4_fig4_direction.csv", header=[0, 1], index_col=[0, 1, 2])
cmp_md = open(f"{CMP}/COMPARISON.md").read()
ds_lines = [l[2:] for l in cmp_md.split("## 1.")[1].split("Mean of")[0].strip().splitlines() if l.startswith("- ")]

# headline numbers
mean_r2 = r2.mean()
m4 = r2.loc[[i for i in r2.index if i[1].startswith("M4")]].mean()
mean_dr = dr.groupby(level=0).mean()

kpis = [("Sites / site-years", ds_lines[0].replace("v4.1: ", "v4.1 ").replace("; v5: ", " · v5 ")),
        ("Mean R², C+T+M (XGB / LGB)",
         f"v4.1 {m4[('XGB','v4.1')]:.3f} / {m4[('LGB','v4.1')]:.3f} · v5 {m4[('XGB','v5')]:.3f} / {m4[('LGB','v5')]:.3f}"),
        ("Relative mortality, shared site-years", f"r = {agree.loc['relative mortality','r_site_years']:.2f}, "
                                               f"median change {agree.loc['relative mortality','median_change']:+.1f} pp"),
        ("High relative-mortality sites", f"v4.1 {hi.loc['relative mortality','high_v41']} · v5 {hi.loc['relative mortality','high_v5']} · "
                                       f"shared {hi.loc['relative mortality','both']}")]

css = """
:root{--bg:#ffffff;--fg:#1b1f24;--muted:#5b6470;--line:#d9dde3;--card:#f6f7f9;--accent:#0f6660;--warn:#9a5b00}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){--bg:#15181c;--fg:#e8eaed;--muted:#a3abb5;--line:#343a42;--card:#1e2227;--accent:#5fc4b8;--warn:#f0b35a}}
:root[data-theme="dark"]{--bg:#15181c;--fg:#e8eaed;--muted:#a3abb5;--line:#343a42;--card:#1e2227;--accent:#5fc4b8;--warn:#f0b35a}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:15px/1.55 system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
main{max-width:1180px;margin:0 auto;padding:24px 16px 64px}h1{font-size:1.6rem;margin:.2em 0}h2{font-size:1.25rem;margin-top:2.2em;border-bottom:1px solid var(--line);padding-bottom:.3em}
h3{font-size:1.05rem;margin-top:1.6em}h4{margin:.2em 0;color:var(--muted);font-size:.9rem}p,li{max-width:80ch}.muted{color:var(--muted)}
nav{position:sticky;top:0;background:var(--bg);border-bottom:1px solid var(--line);padding:8px 16px;z-index:5;overflow-x:auto;white-space:nowrap}
nav a{color:var(--accent);margin-right:14px;text-decoration:none;font-size:.9rem}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:10px;margin:16px 0}.kpi{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:10px 12px}
.kpi b{display:block;font-size:.8rem;color:var(--muted);font-weight:600}.kpi span{font-size:.98rem}
.tw{overflow-x:auto;margin:10px 0 18px}.tbl{border-collapse:collapse;font-size:.83rem;font-variant-numeric:tabular-nums}
.tbl th,.tbl td{border-bottom:1px solid var(--line);padding:4px 8px;text-align:right;white-space:nowrap}.tbl th:first-child,.tbl td:first-child{text-align:left}
.tbl thead th{color:var(--muted);font-weight:600}figure{margin:12px 0 22px}figcaption{color:var(--muted);font-size:.85rem;margin-top:4px}
img{max-width:100%;height:auto;border:1px solid var(--line);border-radius:4px;background:#fff}
.pair{display:grid;grid-template-columns:1fr 1fr;gap:14px}@media (max-width:820px){.pair{grid-template-columns:1fr}}
.note{border-left:3px solid var(--warn);padding:6px 12px;background:var(--card)}.missing{color:var(--warn)}
"""

sec = []
sec.append(("summary", "Summary", f"""
<p>The full EFP analysis (XGBoost and LightGBM, Optuna-tuned, repeated leave-one-site-out, TreeSHAP) was rerun on the
<b>deadtrees v5</b> maps with exactly the same build rules and code as the v4.1 paper dataset. This report shows every
v5 result next to v4.1. Nothing in the paper or the coauthor draft was changed.</p>
<div class="kpis">{''.join(f'<div class="kpi"><b>{k}</b><span>{html.escape(v)}</span></div>' for k, v in kpis)}</div>
<p class="note">v5 (model v5001): newer labels, fixed label-uncertainty loss, prediction date one day later. Standing deadwood is
lower at most sites, tree cover nearly unchanged on average; year-to-year changes depend on the model version more than levels do.</p>"""))

sec.append(("data", "1. Datasets", "<ul>" + "".join(f"<li>{html.escape(l)}</li>" for l in ds_lines) + "</ul>"
            + table(agree, "Mortality metrics (mean of the five buffers) on the site-years present in both datasets.")
            + table(hi, "High-mortality sites: site maximum above mean + 0.5 SD (mean of the five buffers).", fmt="{}")
            + img(f"{V5}/fig1_site_map.png", "Fig 1 (v5): sites and EFP distributions.")))

sec.append(("skill", "2. Skill (R²)", table(r2, "R² under repeated leave-one-site-out, each version on its own sites.")
            + table(r2s, "R² on the site-years present in both datasets only - removes the effect of the different site sets.")
            + table(pd.DataFrame({"all sites": mean_r2, "shared site-years": r2s.mean()}).T, "Mean R² over all 16 tasks.")))

s3 = table(dr, "Change in overall RMSE when mortality is added (C+T → C+T+M), %. Negative = mortality helps.", fmt="{:.2f}")
if f2a is not None:
    s3 += table(f2a, "Fig 2, all sites: median per-site change in RMSE (%), stars = BH-corrected one-sided Wilcoxon, (share of sites improved).", fmt="{}")
s3 += pair("fig2_mortality_effect.png", "fig2_mortality_effect.png", "Fig 2, all sites")
s3 += "<h3>High-mortality sites</h3>" + table(f2h, "Fig 2b, high relative-mortality sites.", fmt="{}")
s3 += pair("fig2_high_total_mort_mortality_effect.png", "fig2_high_total_mort_mortality_effect.png", "Fig 2b, high relative-mortality sites")
s3 += table(f2r, "Fig 2b, high relative-deadwood sites.", fmt="{}")
s3 += pair("fig2_high_rel_deadwood_mortality_effect.png", "fig2_high_rel_deadwood_mortality_effect.png", "Fig 2b, high relative-deadwood sites")
s3 += pair("fig_site_dRMSE_total_mort.png", "fig_site_dRMSE_total_mort.png", "per-site ΔRMSE coloured by relative-mortality class")
sec.append(("effect", "3. Effect of mortality on prediction error", s3))

s4 = pair("fig3b_signed_total_mort_24m_LGB_landscape.png", "fig3b_signed_total_mort_24m_LGB_landscape.png",
          "Fig 3, net signed mortality SHAP per site, LightGBM, 24 months")
s4 += pair("fig3b_signed_total_mort_12m_LGB_landscape.png", "fig3b_signed_total_mort_12m_LGB_landscape.png", "Fig 3, LightGBM, 12 months")
s4 += pair("fig3b_signed_total_mort_24m_XGB_landscape.png", "fig3b_signed_total_mort_24m_XGB_landscape.png", "Fig 3, XGBoost, 24 months")
s4 += pair("fig3_total_mort_24m_LGB_landscape.png", "fig3_total_mort_24m_LGB_landscape.png", "driver composition (|SHAP|), LightGBM, 24 months")
sec.append(("shap", "4. What the models use (Fig 3)", s4))

s5 = table(f4, "Fig 4: median net signed mortality SHAP, High vs Low+Mid sites; stars = BH-corrected Mann-Whitney U.", fmt="{}")
s5 += pair("fig4_signed_shap_total_mort.png", "fig4_signed_shap_total_mort.png", "Fig 4, split by relative mortality")
s5 += pair("fig4_signed_shap_rel_deadwood.png", "fig4_signed_shap_rel_deadwood.png", "Fig 4, split by relative deadwood")
sec.append(("direction", "5. Direction of the effect (Fig 4)", s5))

s6 = pair("fig5_igbp_board_24m_XGB.png", "fig5_igbp_board_24m_XGB.png", "Fig 5 IGBP board, XGBoost, 24 months")
s6 += pair("mortality_split/mortality_composition_concept_LGB.png", "mortality_split/mortality_composition_concept_LGB.png",
           "Composition of the mortality block, LightGBM")
sec.append(("more", "6. Ecosystem type and mortality block", s6))

files5 = sorted(f for f in os.listdir(V5) if f.endswith(".png"))
sec.append(("files", "7. All v5 figures", "<ul>" + "".join(f'<li><a href="{f}">{f}</a></li>' for f in files5) + "</ul>"))

page = f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Deadtrees v5 Results</title><style>{css}</style></head><body>
<nav>{''.join(f'<a href="#{k}">{t}</a>' for k, t, _ in sec)}</nav><main>
<h1>Tree mortality and EFPs - results with deadtrees v5</h1>
<p class="muted">Generated {datetime.date.today().isoformat()} by scripts/paper_v41/34_build_v5_report.py · paper_v5 vs paper_v41 · C+T vs C+T+M · repeated leave-one-site-out (3 × 80 %)</p>
{''.join(f'<section id="{k}"><h2>{t}</h2>{b}</section>' for k, t, b in sec)}
</main></body></html>"""
open(OUT, "w").write(page)
print("written", OUT, f"({len(page)/1e3:.0f} kB)")
