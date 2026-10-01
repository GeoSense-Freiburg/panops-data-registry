#!/usr/bin/env python3
"""
Rebuild plots/V10/V10_report.html as the Optuna-only, tree-cover >= 30 % report
with the paper_v41 (deadtrees v4.1) results added.

Source = the committed V10 report (git HEAD at the time the rebuild was written,
pinned as SRC_REV) so the script is re-runnable: it never reads its own output.

Kept:     dataset "tree cover >= 30 %" (93 sites), learners RF / XGBoost / LightGBM
          Optuna (true_24m figures), both CV structures, sections 1-9, 13-14.
Removed:  all-sites and tc>=50 datasets; baseline / exp02 / untuned / grid-tuned
          learners; sections 10-12 (RF tuning, untuned XGB, untuned LGB).
Added:    dataset "v4.1 paper dataset" (106 sites; XGB + LGB Optuna, repeated CV
          only - RF and LOSO disabled there), v4.1 figures mapped into sections
          1, 3, 7, 8, 9, a disturbance-block-composition section and a
          V10-vs-v4.1 comparison section.
v4.1 images are referenced as ../paper_v41/..., so the report must stay in plots/V10/.
"""
import json, re, subprocess, pathlib

ROOT = pathlib.Path("/mnt/gsdata/projects/panops/panops-data-registry/data/flux")
OUT = ROOT / "plots/V10/V10_report.html"
SRC_REV = "c145816"
# paper-pipeline datasets: key -> model folder ("v41" = v4.1 EFPs, "v41f" = v4.1 flux totals, "v10f" = V10 flux totals)
V41_SETS = {"v41": ROOT / "derived_tables/paper_v41/models", "v41f": ROOT / "derived_tables/paper_v41_fluxes/models",
            "v10f": ROOT / "derived_tables/paper_v10_fluxes/models"}

src = subprocess.run(["git", "-C", str(ROOT), "show", f"{SRC_REV}:./plots/V10/V10_report.html"],
                     capture_output=True, text=True, check=True).stdout


def cut(s, start, end):
    """Return s[start_marker : end_marker) (markers must be unique)."""
    i = s.index(start); j = s.index(end, i)
    return s[i:j]


def replace_block(s, start, end, new):
    i = s.index(start); j = s.index(end, i)
    return s[:i] + new + s[j:]


# ---------------------------------------------------------------- metrics data
def emb(name):
    m = re.search(r"const %s=(\{.*?\});\n" % name, src)
    return json.loads(m.group(1))

TC30 = "sites_with_high_Tcover"
M_LOO = {k: {TC30: emb(n)[TC30]} for k, n in
         [("rfopt", "METRICS_RFOPT"), ("xgbopt", "METRICS_XGBOPT"), ("lgbopt", "METRICS_LGBOPT")]}
M_REP = {k: {TC30: emb(n)[TC30]} for k, n in
         [("rfopt", "METRICS_REP_RFOPT"), ("xgbopt", "METRICS_REP_XGBOPT"), ("lgbopt", "METRICS_REP_LGBOPT")]}

import csv
from collections import defaultdict
for (ds, mdir), (cfg, L) in [(x, y) for x in V41_SETS.items() for y in [("xgbopt", "XGB"), ("lgbopt", "LGB")]]:
    sd = defaultdict(list)
    for r in csv.DictReader(open(mdir / f"{L}_predictions_repCV.csv")):
        sd[(r["model"], r["response"])].append(float(r["pred_sd"]))
    rows = []
    for r in csv.DictReader(open(mdir / f"{L}_metrics_repCV.csv")):
        k = (r["model"], r["response"])
        rows.append(dict(model=r["model"], response=r["response"],
                         n_predictors=int(r["n_predictors"]), n_pairs=int(r["n_pairs"]),
                         RMSE=float(f"{float(r['RMSE']):.4g}"), MAE=float(f"{float(r['MAE']):.4g}"),
                         R2=round(float(r["R2"]), 3), pred_sd=round(sum(sd[k]) / len(sd[k]), 4)))
    M_REP[cfg][ds] = rows

head = src[:src.index("</style>")]
head = head.replace("<title>V10 Disturbance–Flux Analysis · Interactive Report</title>",
                    "<title>Disturbance–Flux Report</title>")
head += """  .toggle button.off{opacity:.3;cursor:not-allowed}
  body.ds-v41 .v10only{display:none !important}
  body:not(.ds-v41) .v41only{display:none !important}
  body:not(.ds-v41) .na41{display:none !important}
  .note.v41only{border-left-color:var(--accent)}
  table.cmp td,table.cmp th{text-align:center}
  table.cmp td.txt,table.cmp th.txt{text-align:left}
  td.pos{color:#7ee2a0} td.neg{color:#f08a8a}
"""
head += "</style>\n</head>\n"

header = """<body>
<header>
  <div class="hwrap">
    <h1>Disturbance effects on ecosystem functional properties · V10 and v4.1</h1>
    <p class="sub">Optuna-tuned learners on the tree-cover &ge;30&nbsp;% datasets: V10 (deadtrees v2-2, 93 sites) and the
      new paper dataset (deadtrees v4.1, 106 sites), each with the EFPs or with growing-season flux totals as the
      response — interactive browser</p>
    <div class="toprow">
      <div class="toggle" id="cvToggle" title="Resampling structure used to validate the models">
        <button data-cv="loo" class="active">Leave-one-site-out</button>
        <button data-cv="rep">Repeated CV (3x80%)</button>
      </div>
      <div class="toggle" id="dsToggle">
        <button data-ds="sites_with_high_Tcover" class="active">V10 · tree cover &ge;30% (93)</button>
        <button data-ds="v10f">V10 · flux totals (93)</button>
        <button data-ds="v41">v4.1 paper dataset (106)</button>
        <button data-ds="v41f">v4.1 · flux totals (102)</button>
      </div>
      <div class="toggle" id="cfgToggle" title="Learner - all tuned with Optuna (TPE, 50 trials)">
        <button data-cfg="rfopt">RF Optuna</button>
        <button data-cfg="xgbopt" class="active">XGBoost Optuna</button>
        <button data-cfg="lgbopt">LightGBM Optuna</button>
      </div>
      <div class="meta" id="metaBar"></div>
    </div>
  </div>
  <nav class="jump">
    <a href="#s0">Overview</a>
    <a href="#s1">1 · Site map</a>
    <a href="#s2">2 · Sites per IGBP</a>
    <a href="#sm">Metrics</a>
    <a href="#s3">3 · Model performance (RMSE)</a>
    <a href="#s4">4 · Threshold methods</a>
    <a href="#s5">5 · Site ΔRMSE</a>
    <a href="#s6">6 · ΔRMSE by threshold</a>
    <a href="#s7">7 · Site SHAP</a>
    <a href="#s8">8 · Grouped SHAP</a>
    <a href="#s9">9 · IGBP &amp; tree cover</a>
    <a href="#s10">10 · Disturbance block</a>
    <a href="#s11">11 · V10 vs v4.1 · EFPs vs fluxes</a>
    <a href="#s13">12 · Model structures</a><a href="#s14">13 · Decisions (V10)</a>
  </nav>
</header>

<main>
"""

# ---------------------------------------------------------------- overview
metric_cards = cut(src, '    <h3 class="mini">Disturbance metrics</h3>', '    <h3 class="mini">Random-forest models (M1–M8)</h3>')
model_tbl = cut(src, '    <table class="deftbl">\n      <thead><tr><th>Model</th><th>Predictors</th>', '    <h3 class="mini">Learner configurations</h3>')
thresholds = cut(src, '    <h3 class="mini">Disturbance-level thresholds</h3>', '  </section>\n\n  <!-- 1 -->')
cv_cards = cut(src, '    <h3 class="mini">Cross-validation structures</h3>', '    <h3 class="mini">Disturbance metrics</h3>')
cv_cards = cv_cards.replace("See Section&nbsp;14.", "See Section&nbsp;13.")

s0 = f"""  <section id="s0">
    <div class="secnum">OVERVIEW</div>
    <h2>About this report</h2>
    <p class="itxt">Two datasets, both restricted to sites with mean tree cover &ge;&nbsp;30&nbsp;% within 500&nbsp;m, and
      three learners, all tuned with <b>Optuna</b> (TPE, 50 trials, grouped 5-fold site CV). Switch between them at the
      top; every figure and the metrics table follow the switches.</p>
    <table class="deftbl">
      <thead><tr><th>Dataset</th><th>Sites</th><th>Site-years</th><th>Mortality maps</th><th>Models</th><th>CV</th><th>Learners</th></tr></thead>
      <tbody>
        <tr><td class="k">V10 · tree cover &ge;30%</td><td>93</td><td>395</td><td>deadtrees v2-2</td><td>M1–M8 (raw / anomaly memory)</td><td>LOSO + repeated</td><td>RF, XGBoost, LightGBM</td></tr>
        <tr class="add"><td class="k">v4.1 paper dataset</td><td>106</td><td>388</td><td>deadtrees v4.1</td><td>M3, M4 only</td><td>repeated only</td><td>XGBoost, LightGBM</td></tr>
        <tr><td class="k">V10 · flux totals</td><td>93</td><td>372</td><td>deadtrees v2-2</td><td>M3, M4 only</td><td>repeated only</td><td>XGBoost, LightGBM</td></tr>
        <tr class="add"><td class="k">v4.1 · flux totals</td><td>102</td><td>362</td><td>deadtrees v4.1</td><td>M3, M4 only</td><td>repeated only</td><td>XGBoost, LightGBM</td></tr>
      </tbody>
    </table>
    <p class="itxt" style="margin-top:10px"><b>v4.1 · flux totals</b> is the v4.1 paper dataset with the four EFPs
      replaced by growing-season totals from the half-hourly FLUXNET data: GPP and NEP (g&nbsp;C&nbsp;m<sup>&minus;2</sup>),
      ET (mm) and WUE = GPP/ET (g&nbsp;C&nbsp;mm<sup>&minus;1</sup>), summed over the same growing-season days as the EFPs.
      Identical predictors and pipeline. A seasonal sum is deflated by missing data, so site-years with &lt;&nbsp;80&nbsp;%
      of the growing season present are dropped (26 of 388; CA-LU1, US-GLE, US-NC3 and US-SHC lose all years).</p>
    <p class="itxt" style="margin-top:10px"><b>V10 · flux totals</b> uses the V10 predictors (deadtrees v2-2, V10 climate
      lags, traits and 11-family mortality block; 93 sites) with exactly the same flux totals, completeness gate (23 of
      395 site-years dropped, no site lost), tuning, repeated CV, SHAP and figures as the v4.1 flux set, so the two flux
      sets differ only in their predictors and site-years. It replaces the earlier <code>predict_fluxes/</code> run
      (daily files, GPP<sub>sat</sub> tuning reused, no SHAP), whose results are kept in that folder.</p>
    <p class="itxt" style="margin-top:10px">With either <b>v4.1</b> dataset or the V10 flux totals selected, <span class="pill">RF Optuna</span> and
      <span class="pill">Leave-one-site-out</span> are greyed out because they were not run for it. Sections without a
      v4.1 counterpart say so. What differs between the two datasets beyond the mortality maps (climate window, NEPmax
      definition, traits, years) is listed in <a href="#s11">Section&nbsp;11</a>, together with a direct comparison of
      the results.</p>

{cv_cards}
{metric_cards}
    <h3 class="mini">Models</h3>
    <p class="itxt">Predictor blocks:
      <span class="pill">C</span> climate,
      <span class="pill">T</span> plant traits,
      <span class="pill">M</span> memory (the response's own previous-year value, V10 only),
      <span class="pill">D</span> disturbance / mortality metrics. Pairs that differ only by <span class="pill">D</span>
      isolate the effect of adding disturbance. Every model runs at a <b>12-month</b> and a <b>24-month</b> lag window.
      <b>The v4.1 paper dataset uses only M3 and M4.</b></p>
{model_tbl}{thresholds}  </section>
"""

# ---------------------------------------------------------------- sections 1-9
body = cut(src, "  <!-- 1 -->", "  <!-- 10 -->")

NOTE41 = '    <div class="note na41">Not produced for this dataset.</div>\n'

# 1 site map
body = body.replace("""distribution of each ecosystem functional property (GPP<sub>sat</sub>, NEP<sub>max</sub>, ET<sub>max</sub>,
      uWUE, WUE) by IGBP class (panels b–f).</p>""", """distribution of each ecosystem functional property by IGBP class. v4.1: GPP<sub>sat</sub>, NEP<sub>max</sub>,
      ET<sub>max</sub>, WUE (uWUE dropped).</p>""")
# 2 IGBP bar
body = body.replace("""    <div class="imgwrap"><img id="img_bar" loading="lazy" alt="sites per IGBP"></div>""",
                    NOTE41 + """    <div class="imgwrap v10only"><img id="img_bar" loading="lazy" alt="sites per IGBP"></div>""")
# metrics
body = body.replace("<h2>Random-forest LOSO performance</h2>", "<h2>Model metrics</h2>")
body = body.replace("""    <p class="desc">Leave-one-site-out cross-validation metrics for every model × window × response
      (24 model configurations × 5 EFPs = 120 rows). Click a column header to sort; filter by response.
      R² cells are shaded from red (low) to green (high).</p>""",
                    """    <p class="desc">Cross-validation metrics for every model × window × response of the selected dataset, learner
      and CV structure (V10: 24 model configurations × 5 EFPs; v4.1: M3/M4 × 2 windows × 4 EFPs). Click a column
      header to sort; filter by response. R² cells are shaded from red (low) to green (high).</p>""")
# 3 RMSE
body = body.replace("""    <div class="imgwrap"><img id="img_rmse" loading="lazy" alt="paired RMSE"></div>""",
                    """    <div class="note v41only"><b>v4.1 and flux sets:</b> one figure for both learners (XGBoost and LightGBM Optuna, repeated CV
      3&nbsp;×&nbsp;80&nbsp;%) and both windows, M3 vs M4 only. Same paired statistics: median per-site change,
      one-sided Wilcoxon, BH-FDR across the 16 tests, share of sites improved. Numbers in
      <code>plots/paper_v41/fig2_statistics.csv</code>.</div>
    <div class="imgwrap"><img id="img_rmse" loading="lazy" alt="paired RMSE"></div>""")
# 4, 5, 6: V10 only
body = body.replace("""    <div class="imrow">
      <div class="imgwrap"><img id="img_thr1\"""", NOTE41 + """    <div class="imrow v10only">
      <div class="imgwrap"><img id="img_thr1\"""")
body = body.replace("""    <div class="controls">
      <div class="ctrl"><label>Response (EFP)</label><select id="sel5_efp"></select></div>
    </div>
    <div class="imgwrap"><img id="img_dbar" loading="lazy" alt="delta RMSE bars"></div>
    <div class="cap" id="cap_dbar"></div>""", NOTE41 + """    <div class="v10only">
    <div class="controls">
      <div class="ctrl"><label>Response (EFP)</label><select id="sel5_efp"></select></div>
    </div>
    <div class="imgwrap"><img id="img_dbar" loading="lazy" alt="delta RMSE bars"></div>
    <div class="cap" id="cap_dbar"></div>
    </div>""")
body = body.replace("""    <div class="controls">
      <div class="ctrl"><label>Comparison</label><select id="sel6_unit"></select></div>""", NOTE41 + """    <div class="v10only">
    <div class="controls">
      <div class="ctrl"><label>Comparison</label><select id="sel6_unit"></select></div>""")
body = body.replace("""    <div class="imgwrap"><img id="img_dcat" loading="lazy" alt="delta RMSE category"></div>
    <div class="cap" id="cap_dcat"></div>""", """    <div class="imgwrap"><img id="img_dcat" loading="lazy" alt="delta RMSE category"></div>
    <div class="cap" id="cap_dcat"></div>
    </div>""")
# 7 site SHAP
body = body.replace("""    <div class="controls">
      <div class="ctrl"><label>Model</label><select id="sel7_model"></select></div>""", """    <div class="note v41only"><b>v4.1 and flux sets:</b> M4 only, one board per mortality metric with the four EFPs side by side.
      <b>Composition</b> = relative mean |SHAP| per driver group with the site's metric value as dots (Fig.&nbsp;3).
      <b>Signed disturbance SHAP</b> = net signed SHAP of the disturbance block in response units (same quantity as
      Fig.&nbsp;4 and 5), bars coloured by the site's disturbance intensity (dark teal → yellow → pink,
      colour-blind safe), sites sorted by intensity, dashed lines = the two natural breaks, mean ± 0.5 SD (High above
      the upper, Mid between, Low below the lower).</div>
    <div class="controls v41only">
      <div class="ctrl"><label>Mortality metric</label><select id="sel7v_metric"></select></div>
      <div class="ctrl"><label>Window</label><select id="sel7v_win"></select></div>
      <div class="ctrl"><label>View</label><select id="sel7v_style"></select></div>
    </div>
    <div class="controls v10only">
      <div class="ctrl"><label>Model</label><select id="sel7_model"></select></div>""")
# 8 grouped SHAP
body = body.replace("""    <div class="controls">
      <div class="ctrl"><label>Model</label><select id="sel8_model"></select></div>""", """    <div class="note v41only"><b>v4.1 and flux sets:</b> net signed disturbance SHAP (M4) for High vs Low+Mid sites, natural break
      = site maximum &gt; mean + 0.5&nbsp;SD. Both learners and windows in one figure; Mann–Whitney, BH-FDR. Numbers in
      <code>plots/paper_v41/fig4_statistics.csv</code>.</div>
    <div class="controls v41only">
      <div class="ctrl"><label>Mortality metric</label><select id="sel8v_metric"></select></div>
    </div>
    <div class="controls v10only">
      <div class="ctrl"><label>Model</label><select id="sel8_model"></select></div>""")
# 9 IGBP
body = body.replace("""    <div class="controls">
      <div class="ctrl"><label>View</label><select id="sel9_type">""", """    <div class="note v41only"><b>v4.1 and flux sets:</b> 4-panel board for M4 (M3 vs M4 per site), per window and learner.</div>
    <div class="controls v41only">
      <div class="ctrl"><label>Window</label><select id="sel9v_win"></select></div>
    </div>
    <div class="controls v10only">
      <div class="ctrl"><label>View</label><select id="sel9_type">""")

a = '''    <div class="controls v10only">
      <div class="ctrl"><label>Model</label><select id="sel7_model"></select></div>'''
assert body.count(a) == 1
body = body.replace(a, '''    <div class="note v10only" id="note7_signed" style="display:none"><b>Signed disturbance SHAP (V10):</b> net signed
      SHAP of the disturbance block for M4 (response units), bars coloured by the site's disturbance intensity (dark teal → yellow → pink,
      colour-blind safe), sites sorted by intensity, dashed lines = the two natural breaks, mean ± 0.5 SD (High above
      the upper, Mid between, Low below the lower). From the V10 Optuna models
      (lag2-corrected 24&nbsp;m); computed for XGBoost and LightGBM only.</div>
    <div class="controls v10only">
      <div class="ctrl"><label>View</label><select id="sel7_view"></select></div>
      <div class="ctrl" style="display:none"><label>Mortality metric</label><select id="sel7_metric10"></select></div>
      <div class="ctrl"><label>Model</label><select id="sel7_model"></select></div>''')

a = '    <div class="imgwrap"><img id="img_rmse"'
assert body.count(a) == 1
body = body.replace(a, '''    <div class="controls">
      <div class="ctrl"><label>Sites</label><select id="sel3_sites"></select></div>
    </div>
    <div class="note" id="note3_high" style="display:none"><b>High-disturbance sites only.</b> The same figure and
      statistics restricted to the sites whose maximum disturbance across years lies above the natural break
      (mean + 0.5&nbsp;SD over sites; the number of sites is given in the figure subtitle). M3 vs M4, XGBoost and LightGBM
      Optuna, repeated CV (3&nbsp;×&nbsp;80&nbsp;%); models are trained on all sites, only the sites shown and tested
      change. With ~20–26 sites the tests have less power, so read the effect size together with the stars. For the V10
      dataset this view does not follow the learner and CV switches.</div>
''' + a)

i3 = body.index('  <section id="s3">'); j3 = body.index('</section>', i3) + len('</section>')
body = body[:i3] + '''  <section id="s3">
    <div class="secnum">SECTION 3</div>
    <h2>Model performance &amp; effect of disturbance on RMSE</h2>
    <p class="desc">Per-site RMSE without disturbance (cyan, M3 = climate + traits) and with it (pink, M4 = + mortality),
      for XGBoost and LightGBM (Optuna) at the 12&nbsp;m and 24&nbsp;m windows, repeated CV (3&nbsp;×&nbsp;80&nbsp;%). The same
      figure for all four datasets; it does not follow the learner and CV switches.</p>
    <p class="itxt"><b>Reading the labels.</b> Each pair carries three numbers:
      <span class="pill">%</span> the <b>median paired change</b> in per-site RMSE (negative = less error, green);
      <span class="pill">stars</span> a <b>one-sided paired Wilcoxon signed-rank test</b>, H<sub>1</sub>: adding D
      <i>reduces</i> per-site RMSE, with Benjamini–Hochberg FDR across the 16 tests in the figure
      (*** q&lt;0.001, ** q&lt;0.01, * q&lt;0.05, ns); and <span class="pill">%↓</span> the share of sites whose RMSE
      improved. Sites are paired on <code>SITE_ID</code>. Statistics are written to <code>fig2_statistics.csv</code>
      (or <code>fig2_high_*_statistics.csv</code>) beside each figure.</p>
    <p class="itxt"><b>Caveat.</b> The effect size is the <i>median of the paired per-site changes</i>, not the difference
      of the two medians. Because the models share training sites, the per-site errors are not fully independent, which
      makes p-values mildly <b>anti-conservative</b>; strong results are safe, marginal ones (q ≈ 0.05) should not be
      over-read.</p>
    <div class="controls">
      <div class="ctrl"><label>Sites</label><select id="sel3_sites"></select></div>
    </div>
    <div class="note" id="note3_high" style="display:none"><b>High-disturbance sites only.</b> The same figure and
      statistics restricted to the sites whose maximum disturbance across years lies above the natural break
      (mean + 0.5&nbsp;SD over sites; the number of sites is given in the figure subtitle). Models are trained on all
      sites; only the sites shown and tested change. With ~20–26 sites the tests have less power, so read the effect
      size together with the stars.</div>
    <div class="imgwrap"><img id="img_rmse" loading="lazy" alt="paired RMSE"></div>
    <div class="cap" id="cap_rmse"></div>
  </section>''' + body[j3:]

i5 = body.index('  <section id="s5">'); j5 = body.index('</section>', i5) + len('</section>')
body = body[:i5] + '''  <section id="s5">
    <div class="secnum">SECTION 5</div>
    <h2>Site-specific ΔRMSE (diverging bars)</h2>
    <p class="desc">Per-site change in RMSE from adding disturbance (ΔRMSE = RMSE<sub>with D</sub> − RMSE<sub>without D</sub>),
      M3 (climate + traits) vs M4 (+ mortality): one bar per site, sorted within each panel; below zero = mortality
      reduces that site's error. Rows = responses, columns = XGBoost and LightGBM (Optuna) at 12&nbsp;m and 24&nbsp;m,
      repeated CV (3&nbsp;×&nbsp;80&nbsp;%). Bars are coloured by the site's disturbance intensity (site maximum, 500&nbsp;m) on
      the same dark teal → yellow → pink scale as the signed-SHAP figure in Section&nbsp;7; each panel gives the share of
      sites improved. Numbers, including the rank correlation between ΔRMSE and intensity, are in
      <code>fig_site_dRMSE_statistics.csv</code> beside the figure. Does not follow the learner and CV switches.</p>
    <div class="controls">
      <div class="ctrl v10only"><label>View</label><select id="sel5_view"></select></div>
      <div class="ctrl"><label>Colour by</label><select id="sel5_metric"></select></div>
      <div class="ctrl" style="display:none"><label>Response (EFP)</label><select id="sel5_efp"></select></div>
    </div>
    <div class="note v10only" id="note5_all" style="display:none"><b>All model pairs (V2.2).</b> The earlier figure:
      one board per EFP with all four model pairs (C vs C+D … C+T+M vs C+T+D+M) × memory type × window, bars coloured by
      relative-disturbance tier. Follows the learner and CV switches.</div>
    <div class="imgwrap"><img id="img_dbar" loading="lazy" alt="delta RMSE bars"></div>
    <div class="cap" id="cap_dbar"></div>
  </section>''' + body[j5:]

# ---------------------------------------------------------------- new sections
def pct_cell(v):
    cls = "pos" if v < 0 else "neg"
    return f'<td class="{cls}">{v:+.1f}%</td>'

fig2_v10 = {  # median per-site % change, sig (XGB 12, XGB 24, LGB 12, LGB 24) - manuscript_coauthor_draft/fig2_statistics.csv
    "GPPsat": [(-7.57, "***"), (-6.73, "**"), (-8.32, "***"), (-8.66, "**")],
    "NEPmax": [(-13.35, "***"), (-4.78, "*"), (-12.72, "***"), (-16.02, "***")],
    "ETmax":  [(1.70, "ns"), (2.16, "ns"), (0.85, "ns"), (1.08, "ns")],
    "WUE":    [(-3.86, "*"), (-6.05, "*"), (-3.79, "*"), (-7.18, "*")]}
fig2_v41 = {  # plots/paper_v41/fig2_statistics.csv
    "GPPsat": [(-4.31, "*"), (-1.52, "ns"), (-3.83, "*"), (-4.84, "*")],
    "NEPmax": [(-7.29, "***"), (-9.63, "***"), (-9.52, "***"), (-7.97, "***")],
    "ETmax":  [(-5.19, "***"), (-3.97, "*"), (-3.20, "*"), (-3.03, "*")],
    "WUE":    [(-6.31, "*"), (-2.31, "ns"), (-1.17, "ns"), (-2.24, "ns")]}
r2 = {  # pooled R2, repeated CV: (V10 M3, V10 M4, v41 M3, v41 M4) for XGB12, XGB24, LGB12, LGB24
    "GPPsat": [(0.19, 0.35, 0.33, 0.39), (0.17, 0.33, 0.33, 0.38), (0.16, 0.33, 0.31, 0.39), (0.19, 0.34, 0.34, 0.41)],
    "NEPmax": [(0.19, 0.40, 0.40, 0.49), (0.18, 0.35, 0.42, 0.50), (0.18, 0.41, 0.41, 0.51), (0.17, 0.40, 0.43, 0.52)],
    "ETmax":  [(0.39, 0.39, 0.41, 0.44), (0.41, 0.40, 0.44, 0.46), (0.41, 0.42, 0.43, 0.45), (0.41, 0.39, 0.42, 0.45)],
    "WUE":    [(0.13, 0.27, 0.44, 0.48), (0.11, 0.23, 0.41, 0.42), (0.12, 0.27, 0.43, 0.45), (0.10, 0.24, 0.43, 0.46)]}
COLS = ["XGB 12m", "XGB 24m", "LGB 12m", "LGB 24m"]

rows_fig2 = ""
for e in ["GPPsat", "NEPmax", "ETmax", "WUE"]:
    rows_fig2 += f'        <tr><td class="txt"><b>{e}</b></td>'
    for (a, sa), (b, sb) in zip(fig2_v10[e], fig2_v41[e]):
        rows_fig2 += f'<td>{a:+.1f}% {sa}</td><td><b>{b:+.1f}% {sb}</b></td>'
    rows_fig2 += "</tr>\n"
rows_r2 = ""
for e in ["GPPsat", "NEPmax", "ETmax", "WUE"]:
    for c, (a3, a4, b3, b4) in zip(COLS, r2[e]):
        rows_r2 += (f'        <tr><td class="txt">{e}</td><td class="txt">{c}</td><td>{a3:.2f}</td><td>{a4:.2f}</td>'
                    f'<td>{a4 - a3:+.2f}</td><td>{b3:.2f}</td><td>{b4:.2f}</td><td><b>{b4 - b3:+.2f}</b></td></tr>\n')
hdr2 = "".join(f'<th colspan="2">{c}</th>' for c in COLS)
sub2 = "".join("<th>V10</th><th>v4.1</th>" for _ in COLS)


# ---- flux-totals subsection of Section 11 (numbers read from the result files)
def fig2_stats(path, grp_col="grp"):
    out = {}
    for r in csv.DictReader(open(path)):
        L = "XGB" if r["learner"].startswith("XGB") else "LGB"
        out[(r["response"], L, r[grp_col])] = (float(r["median_pct"]), r["sig"], float(r["sites_improved"]))
    return out
def pooled_r2(ds):
    out = {}
    for cfg, L in (("xgbopt", "XGB"), ("lgbopt", "LGB")):
        for r in M_REP[cfg][ds]:
            mod, win = r["model"].split("_")
            out[(r["response"], L, win, mod)] = r["R2"]
    return out
F2E = fig2_stats(ROOT / "plots/paper_v41/fig2_statistics.csv")
F2F = fig2_stats(ROOT / "plots/paper_v41_fluxes/fig2_statistics.csv")
F2O = fig2_stats(ROOT / "plots/paper_v10_fluxes/fig2_statistics.csv")
R2E, R2F, R2O = pooled_r2("v41"), pooled_r2("v41f"), pooled_r2("v10f")
PAIRS = [("GPPsat", "GPP_gs", "GPP_gs", "GPP"), ("NEPmax", "NEP_gs", "NEP_gs", "NEP"),
         ("ETmax", "ET_gs", "ET_gs", "ET"), ("WUE", "WUE_gs", "WUE_gs", "WUE")]
def r2c(R, k):
    return f"{R[k + ('M3',)]:.2f} → {R[k + ('M4',)]:.2f}"
rows_flux = ""
for e, f, o, lab in PAIRS:
    for L in ("XGB", "LGB"):
        for w in ("12m", "24m"):
            oa, osg, _ = F2O[(o, L, w)]; fb, fsg, _ = F2F[(f, L, w)]; ea, esg, _ = F2E[(e, L, w)]
            rows_flux += (f'        <tr><td class="txt">{lab}</td><td class="txt">{L} {w}</td>'
                          f'<td>{r2c(R2O, (o, L, w))}</td><td>{oa:+.1f}% {osg}</td>'
                          f'<td>{r2c(R2F, (f, L, w))}</td><td><b>{fb:+.1f}% {fsg}</b></td>'
                          f'<td>{ea:+.1f}% {esg}</td></tr>\n')
flux_html = f"""    <h3 class="mini">Flux totals: V10 (93 sites) vs v4.1 (102 sites), with the v4.1 EFPs for reference</h3>
    <p class="desc">Pooled R² M3&nbsp;→&nbsp;M4 and median per-site RMSE change (one-sided Wilcoxon, BH-FDR), repeated CV.
      Both flux sets: identical half-hourly totals, completeness gate, tuning and pipeline; they differ only in their
      predictors (V10 vs v4.1) and site-years (372 at 93 sites vs 362 at 102 sites).</p>
    <div class="tblwrap"><table class="metrics cmp">
      <thead><tr><th class="txt" rowspan="2">flux</th><th class="txt" rowspan="2">learner · window</th><th colspan="2">V10 flux totals</th><th colspan="2">v4.1 flux totals</th><th>v4.1 EFP</th></tr>
      <tr><th>R²</th><th>ΔRMSE</th><th>R²</th><th>ΔRMSE</th><th>ΔRMSE</th></tr></thead>
      <tbody>
{rows_flux}      </tbody>
    </table></div>
    <ul class="itxt">
      <li><b>Same pattern in all four sets.</b> Mortality improves the carbon fluxes and WUE and not ET, in V10 and v4.1,
        for EFPs and for totals. The ET null holds everywhere except the v4.1 ET<sub>max</sub>.</li>
      <li><b>V10 vs v4.1 totals.</b> Mortality adds more in V10: GPP R² 0.27–0.31&nbsp;→&nbsp;0.40–0.43 (per site
        −6 to −10&nbsp;%, all significant) and NEP −5 to −7&nbsp;% (all significant), versus GPP 0.55–0.57&nbsp;→&nbsp;0.57–0.59
        in v4.1. The difference sits in the <b>baseline</b>: with the same responses and tuning, v4.1 climate + traits
        explain GPP far better (R² ≈&nbsp;0.56 vs 0.29) and NEP too (≈&nbsp;0.24 vs ≈&nbsp;0), leaving less for mortality.
        What makes the v4.1 baseline stronger (pre-season climate window, Lusk traits, site set) has not been isolated.</li>
      <li><b>Windows.</b> 24&nbsp;m beats 12&nbsp;m for the v4.1 fluxes (GPP and WUE significant mainly at 24&nbsp;m); for the
        V10 fluxes and the EFPs the two windows are about equal.</li>
      <li><b>High vs Low+Mid SHAP</b> (Fig.&nbsp;4): GPP and WUE significant in all 8 tests in both flux sets; NEP in 7 of 8
        (V10) and 4 of 8 (v4.1, relative disturbance only); ET never.</li>
      <li><b>Caveat:</b> the v4.1 completeness gate removes all years of two strongly disturbed sites (CA-LU1, US-GLE)
        and some of DE-Akm and US-NC2, so the v4.1 flux set carries less high-disturbance data.</li>
    </ul>
"""

new_sections = f"""  <!-- 10 -->
  <section id="s10">
    <div class="secnum">SECTION 10</div>
    <h2>Disturbance block, metric by metric</h2>
    <p class="desc">Share of the disturbance block's |SHAP| (M4, block renormalised to 100&nbsp;%), per metric family
      (5 buffers and all lags pooled) or per sub-block: <b>mortality</b> (deadwood stock and new deadwood),
      <b>tree cover loss</b>, and the <b>combined index</b> (relative disturbance). Mean over sites. Follows the dataset
      and learner switches.</p>
    <p class="itxt"><b>V10 vs v4.1.</b> v4.1 has 7 metric families instead of 11 (no <code>new_mortality_rate_pct</code>,
      no severity metrics) and lag&nbsp;2 of every metric at 24&nbsp;m. The largest change is <b>tree cover loss</b>:
      <code>relative_tree_loss_pct</code> is now the single largest metric for GPP<sub>sat</sub> and ET<sub>max</sub>
      (27–39&nbsp;%, versus ~10–13&nbsp;% in V10); by sub-block, mortality 39–45&nbsp;%, tree cover loss 34–36&nbsp;%,
      combined index 21–25&nbsp;%.</p>
    <div class="controls">
      <div class="ctrl"><label>View</label><select id="sel10_view"></select></div>
    </div>
    <div class="imgwrap"><img id="img_dcomp" loading="lazy" alt="disturbance block composition"></div>
    <div class="cap" id="cap_dcomp"></div>
  </section>

  <!-- 11 -->
  <section id="s11">
    <div class="secnum">SECTION 11</div>
    <h2>V10 vs v4.1 — what changed and how the results compare</h2>
    <p class="desc">Both on tree cover &ge;&nbsp;30&nbsp;%, repeated CV (3&nbsp;×&nbsp;80&nbsp;%), XGBoost and LightGBM
      Optuna, M3 (climate + traits) vs M4 (+ mortality). Independent of the switches at the top.</p>

    <h3 class="mini">What changed between the two datasets</h3>
    <table class="deftbl">
      <thead><tr><th></th><th>V10</th><th>v4.1 paper dataset</th></tr></thead>
      <tbody>
        <tr><td class="k">Mortality maps</td><td>deadtrees v2-2</td><td>deadtrees v4.1: mid-season prediction date fixed (median shift 38&nbsp;days), cube-corner pixels fixed at 42 sites, per-year <code>covered</code> mask</td></tr>
        <tr><td class="k">Sites / site-years</td><td>93 / 395, 2019–2025</td><td>106 / 388, 2020–2025 (83 sites shared, 10 dropped, 23 new)</td></tr>
        <tr><td class="k">Climate window</td><td>centre-of-season month Y−1 to Y (overlaps the first half of the EFP's own season)</td><td>12 months strictly before the growing-season start (lag&nbsp;1), the 12 before that (lag&nbsp;2)</td></tr>
        <tr><td class="k">NEP<sub>max</sub></td><td>99th pct of daytime NEP</td><td>90th pct of all growing-season half-hourly NEP (Migliavacca et al. 2021); ≈&nbsp;57&nbsp;% of the old values</td></tr>
        <tr><td class="k">Responses</td><td>GPP<sub>sat</sub>, NEP<sub>max</sub>, ET<sub>max</sub>, uWUE, WUE</td><td>GPP<sub>sat</sub>, NEP<sub>max</sub>, ET<sub>max</sub>, WUE</td></tr>
        <tr><td class="k">Traits</td><td>21 hydraulic / leaf / stem traits</td><td>23 Lusk et al. 2025 CWM traits, 1&nbsp;km, area-weighted 500&nbsp;m buffer</td></tr>
        <tr><td class="k">Disturbance block</td><td>11 families × 5 buffers, current + lag&nbsp;1 (24&nbsp;m: + lag&nbsp;2 for the 2 stock metrics)</td><td>7 families × 5 buffers, current + lag&nbsp;1 (12&nbsp;m, 70 vars) or + lag&nbsp;2 (24&nbsp;m, 105 vars)</td></tr>
        <tr><td class="k">Models · CV · learners</td><td>M1–M8 · LOSO + repeated · RF, XGB, LGB Optuna</td><td>M3, M4 · repeated · XGB, LGB Optuna (objective = RMSE averaged over M3 and M4)</td></tr>
      </tbody>
    </table>
    <p class="itxt" style="margin-top:8px">Observed GPP<sub>sat</sub>, ET<sub>max</sub> and WUE are <b>identical</b> at the
      303 site-years both datasets share, so differences for these responses come from the predictors and the site set,
      not from the responses.</p>

    <h3 class="mini">Per-site effect of adding mortality (Fig. 2 statistics)</h3>
    <p class="desc">Median per-site change in RMSE (M3&nbsp;→&nbsp;M4), one-sided Wilcoxon, BH-FDR. Negative = mortality
      reduces the error.</p>
    <div class="tblwrap"><table class="metrics cmp">
      <thead><tr><th class="txt" rowspan="2">EFP</th>{hdr2}</tr><tr>{sub2}</tr></thead>
      <tbody>
{rows_fig2}      </tbody>
    </table></div>

    <h3 class="mini">Pooled R² (all site-years)</h3>
    <div class="tblwrap"><table class="metrics cmp">
      <thead><tr><th class="txt">EFP</th><th class="txt">learner · window</th><th>V10 M3</th><th>V10 M4</th><th>V10 gain</th><th>v4.1 M3</th><th>v4.1 M4</th><th>v4.1 gain</th></tr></thead>
      <tbody>
{rows_r2}      </tbody>
    </table></div>

FLUX_SUBSECTION
    <h3 class="mini">Reading the comparison</h3>
    <ul class="itxt">
      <li><b>Same direction, smaller increment.</b> Mortality still helps GPP<sub>sat</sub>, NEP<sub>max</sub> and WUE,
        and NEP<sub>max</sub> is still the strongest. M4 is as good as or better than in V10, but the M3 baseline is much
        stronger (GPP<sub>sat</sub> ≈&nbsp;0.18&nbsp;→&nbsp;0.33, NEP<sub>max</sub> 0.18&nbsp;→&nbsp;0.42, WUE
        0.12&nbsp;→&nbsp;0.43), so the R² gain from mortality is about a third to a half of V10's (a quarter for WUE).</li>
      <li><b>The stronger baseline is mostly the site set.</b> On the 303 shared site-years M3 is not stronger in v4.1
        (GPP<sub>sat</sub> 0.16–0.20 vs 0.12–0.16; WUE −0.23 to −0.06 vs 0.04–0.09). The 23 new sites and years widen the
        range of WUE between sites, which traits capture. On the shared site-years mortality still adds +0.07 to +0.09 R²
        (GPP<sub>sat</sub>) and +0.07 to +0.27 (WUE). R² depends on the spread of the sample, so compare the datasets on
        RMSE.</li>
      <li><b>ET<sub>max</sub> moved from a slight loss to a slight gain</b> (≈&nbsp;+0.02 R²). It holds on the 83 shared
        sites (median −1 to −4&nbsp;% per site vs +1 to +2&nbsp;% in V10) and is larger at the 23 new sites (−6 to −12&nbsp;%).
        M3 skill for ET<sub>max</sub> is unchanged, so the change is in what mortality adds; it is concentrated in ENF and
        MF, not DBF.</li>
      <li><b>Not explained by the date fix.</b> The per-site change in gain is unrelated to how far v4.1 moved the map
        date (Spearman ρ between −0.15 and 0.12, all p&nbsp;&gt;&nbsp;0.17), and DBF sites, with the largest shifts, show
        no ET<sub>max</sub> change. The corner fix, masking, lag structure, traits, years and tuning also changed; a swap
        test (v4.1 pipeline with the v2-2 mortality block) would isolate the maps.</li>
    </ul>
  </section>

"""

new_sections = new_sections.replace("FLUX_SUBSECTION", flux_html)

# ---------------------------------------------------------------- model structures (trimmed)
s13_cv = cut(src, '    <h3 class="mini">Cross-validation structure (top-left toggle)</h3>', '    <h3 class="mini">What tuning each variant received</h3>')
s13_cv = s13_cv.replace("all 111 remaining sites", "all remaining sites (92 in V10)").replace("(~89 sites)", "(~74 in V10, ~84 in v4.1)")
s13_cv = re.sub(r'    <p class="itxt" style="margin-top:10px"><b>Why it is worth having\.</b>.*?</p>\n', "", s13_cv, flags=re.S)
s13_cv = s13_cv.replace("Sections 7, 8 and the SHAP views of Section 9", "Sections 7, 8, 10 and the SHAP views of Section 9")
fam_tbl = cut(src, '    <h3 class="mini">Model families and predictor counts</h3>', '    <h3 class="mini">Predictor block composition</h3>')
# keep only the tree-cover columns
fam_tbl = fam_tbl.replace('<th colspan="2">all sites (112)</th><th colspan="2">tree cover &ge;30% / &ge;50%</th>', '<th colspan="2">V10 · tree cover &ge;30%</th>')
fam_tbl = re.sub(r"(<td class=\"k\">M[1-8][^<]*</td><td>[^<]*</td>)<td>\d+</td><td>\d+</td>", r"\1", fam_tbl)
fam_tbl = fam_tbl.replace('<th>p (12m)</th><th>p (24m)</th><th>p (12m)</th><th>p (24m)</th>', '<th>p (12m)</th><th>p (24m)</th>')

s13 = f"""  <section id="s13">
    <div class="secnum">SECTION 12 · REFERENCE</div>
    <h2>Model structures</h2>
    <p class="desc">Optuna search, cross-validation structure and predictor counts for both datasets.</p>

    <h3 class="mini">Optuna tuning</h3>
    <table class="deftbl">
      <thead><tr><th>Learner</th><th>Search space</th><th>V10</th><th>v4.1</th></tr></thead>
      <tbody>
        <tr><td class="k">RF Optuna (<code>ranger</code>)</td><td><code>mtry_frac</code> 0.02–1.0, <code>num.trees</code> 250–1500, <code>min.node.size</code> 1–30, <code>sample.fraction</code> 0.4–1.0</td><td>50 trials per response</td><td>not run</td></tr>
        <tr><td class="k">XGBoost Optuna</td><td><code>learning_rate</code> 0.01–0.3, <code>nrounds</code> 100–1000, <code>max_depth</code> 1–8, <code>min_child_weight</code> 1–20, <code>subsample</code> 0.5–1, <code>colsample_bytree</code> 0.2–1, <code>reg_lambda</code> 0.001–50</td><td>50 trials per response</td><td>50 trials per response × window</td></tr>
        <tr><td class="k">LightGBM Optuna</td><td><code>learning_rate</code> 0.01–0.3, <code>nrounds</code> 100–1000, <code>num_leaves</code> 2–63, <code>min_data_in_leaf</code> 2–40, <code>feature_fraction</code> 0.2–1, <code>bagging_fraction</code> 0.5–1, <code>lambda_l2</code> 0.001–50</td><td>50 trials per response</td><td>50 trials per response × window</td></tr>
      </tbody>
    </table>
    <p class="itxt" style="margin-top:8px">TPE sampler; every trial scored by grouped 5-fold CV (folds = disjoint site
      groups), so tuning never touches the reported CV metrics. In v4.1 the objective is the scale-free RMSE averaged
      over M3 and M4, so both models share one configuration per EFP and window (best configurations in
      <code>plots/paper_v41/Optuna/{{12m,24m}}/best_configs.csv</code>).</p>

{s13_cv}
{fam_tbl}
    <h3 class="mini">v4.1 predictor counts</h3>
    <table class="deftbl">
      <thead><tr><th>Model</th><th>Blocks</th><th>p (12m)</th><th>p (24m)</th></tr></thead>
      <tbody>
        <tr><td class="k">M3</td><td>C + T</td><td>143 (120 climate + 23 traits)</td><td>263 (240 + 23)</td></tr>
        <tr class="add"><td class="k">M4</td><td>C + T + D</td><td>213 (+ 70 mortality)</td><td>368 (+ 105 mortality)</td></tr>
      </tbody>
    </table>
    <p class="itxt" style="margin-top:8px">Climate: monthly mean / p05 / p95 of TA, VPD and SW_IN plus precipitation sum,
      12 months per lag (10 × 12 = 120). Mortality: 7 metric families × 5 buffers (100–500&nbsp;m) × current + lag&nbsp;1
      (+ lag&nbsp;2 at 24&nbsp;m).</p>
  </section>

"""

# ---------------------------------------------------------------- decisions (kept, flagged as V10)
s14 = cut(src, '  <section id="s14">', "  <div class=\"note\">All figures")
s14 = s14.replace('<div class="secnum">SECTION 14 · DECISIONS</div>', '<div class="secnum">SECTION 13 · DECISIONS (V10)</div>')
s14 = s14.replace("""<h2>What gets reported — and why</h2>""", """<h2>What gets reported — and why (V10, 2026-08-19)</h2>
    <div class="note"><b>Superseded for the paper.</b> This section records the V10 decisions as written on 2026-08-19.
      The paper now uses the v4.1 dataset (106 sites), M3 and M4 only, repeated CV (3&nbsp;×&nbsp;80&nbsp;%) and
      XGBoost + LightGBM Optuna. The dataset, learner and CV comparisons below still describe V10.</div>""")

tail = """  <div class="note">All figures are also available as PDF next to each PNG. This report references the image files by
    relative path: keep it inside <code>plots/V10/</code> (v4.1, flux and signed-SHAP figures are read from <code>../paper_v41/</code>, <code>../paper_v41_fluxes/</code>, <code>../paper_v10_fluxes/</code> and <code>../paper_v10_efp/</code>).</div>
</main>

<div class="lightbox" id="lb"><img id="lbimg" alt="zoom"></div>

<footer>Disturbance–flux interactive report (V10 and v4.1) · Optuna learners, tree cover &ge;30&nbsp;%.</footer>
"""

# ---------------------------------------------------------------- script
js_consts = cut(src, "const EFPS=", "const LGB_VIEWS=")
script = "<script>\n" + js_consts + f"""
const METRICS_LOO={json.dumps(M_LOO, separators=(',', ':'))};
const METRICS_REP={json.dumps(M_REP, separators=(',', ':'))};
const V41_MODEL_FAMS=['M3','M4'];
let CFG='xgbopt';
let CV='loo';
let DS='sites_with_high_Tcover';
const V10PATH={{loo:{{rfopt:'true_24m/RF_optuna/',xgbopt:'true_24m/XGB_optuna/',lgbopt:'true_24m/LGB_optuna/'}},
  rep:{{rfopt:'true_24m/repeatedCV/RF_optuna/',xgbopt:'true_24m/repeatedCV/XGB_optuna/',lgbopt:'true_24m/repeatedCV/LGB_optuna/'}}}};
// P(): RMSE / prediction figures depend on the resampling structure.
function P(){{return V10PATH[CV][CFG];}}
// PS(): SHAP figures come from a full-data model, identical under both structures.
function PS(){{return V10PATH.loo[CFG];}}
const V41P='../paper_v41/', V41F='../paper_v41_fluxes/', V10F='../paper_v10_fluxes/', V10E='../paper_v10_efp/';
const V41=()=>({{v41:V41P,v41f:V41F,v10f:V10F}})[DS];   // image root of the active paper-pipeline dataset
const LFAM={{rfopt:'RF',xgbopt:'XGB',lgbopt:'LGB'}};
const is41=()=>DS==='v41'||DS==='v41f'||DS==='v10f';   // datasets run through the paper_v41 pipeline

const META={{
  sites_with_high_Tcover:{{sites:93,siteyears:395,igbp:'ENF 45 · DBF 26 · WET 10 · MF 6 · CSH 2 · OSH 2 · WSA 1 · EBF 1'}},
  v10f:{{sites:93,siteyears:372,igbp:'ENF 45 · DBF 26 · WET 10 · MF 6 · CSH 2 · OSH 2 · WSA 1 · EBF 1'}},
  v41:{{sites:106,siteyears:388,igbp:'ENF 51 · DBF 30 · WET 8 · MF 7 · CSH 3 · OSH 3 · EBF 3 · DNF 1'}},
  v41f:{{sites:102,siteyears:362,igbp:'ENF 48 · DBF 30 · WET 8 · MF 7 · CSH 3 · EBF 3 · OSH 2 · DNF 1'}}
}};
const enc=p=>encodeURI(p);
const setImg=(id,path,capId)=>{{const el=document.getElementById(id);el.src=enc(path);
  if(capId)document.getElementById(capId).textContent=path;}};

function opt(val,label){{const o=document.createElement('option');o.value=val;o.textContent=label||val;return o;}}
function fill(sel,items){{sel.innerHTML='';items.forEach(it=>{{if(Array.isArray(it))sel.appendChild(opt(it[0],it[1]));else sel.appendChild(opt(it));}});}}
const V41_METRICS=[['rel_mort','Relative mortality'],['rel_dist','Relative disturbance']];   // absolute mortality not shown
const D_VIEWS=[['family','Metric by metric'],['concept','Three sub-blocks (share and per variable)']];

fill(document.getElementById('sel5_efp'),EFPS);
fill(document.getElementById('sel5_view'),[['m34','M3 vs M4 · XGBoost + LightGBM'],['all','All model pairs (per learner and CV)']]);
fill(document.getElementById('sel5_metric'),[['reldist','Relative disturbance'],['relmort','Relative mortality']]);
const showCtl=(id,on)=>{{document.getElementById(id).closest('.ctrl').style.display=on?'':'none';}};
fill(document.getElementById('sel6_unit'),DELTA_UNITS);
fill(document.getElementById('sel6_method'),METHODS);
fill(document.getElementById('sel7_model'),SHAP_MODELS);
fill(document.getElementById('sel7_efp'),EFPS);
fill(document.getElementById('sel7_win'),WINDOWS);
fill(document.getElementById('sel7_view'),[['comp','Composition (driver groups)'],['signed','Signed disturbance SHAP (M4)']]);
fill(document.getElementById('sel7_metric10'),V41_METRICS);
fill(document.getElementById('sel7v_metric'),V41_METRICS);
fill(document.getElementById('sel7v_win'),WINDOWS);
fill(document.getElementById('sel7v_style'),[['comp','Composition (relative |SHAP|)'],['signed','Signed disturbance SHAP']]);
fill(document.getElementById('sel8_model'),SHAP_MODELS);
fill(document.getElementById('sel8_win'),WINDOWS);
fill(document.getElementById('sel8_method'),METHODS);
fill(document.getElementById('sel8v_metric'),[['relmort','Relative mortality'],['reldist','Relative disturbance']]);
fill(document.getElementById('sel9v_win'),WINDOWS);
fill(document.getElementById('sel10_view'),D_VIEWS);
fill(document.getElementById('sel3_sites'),[['all','All sites'],['high_reldist','High disturbance only (relative disturbance)'],['high_relmort','High disturbance only (relative mortality)']]);
const RESP_OF={{sites_with_high_Tcover:EFPS,v41:['GPPsat','NEPmax','ETmax','WUE'],v41f:['GPP_gs','NEP_gs','ET_gs','WUE_gs'],
  v10f:['GPP_gs','NEP_gs','ET_gs','WUE_gs']}};
function fillResp(){{const sel=document.getElementById('selm_resp'),prev=sel.value;fill(sel,['All',...RESP_OF[DS]]);
  if([...sel.options].some(o=>o.value===prev))sel.value=prev;}}

function pickMetrics(){{const src=(CV==='rep'?METRICS_REP:METRICS_LOO)[CFG]||{{}};return src[DS]||[];}}
let mSort={{k:'response',dir:1}};
function r2color(v){{const t=Math.max(0,Math.min(1,v/0.6));const h=Math.round(t*125);return `hsl(${{h}} 45% 22%)`;}}
function fillFam(){{const sel=document.getElementById('selm_fam'),prev=sel.value;
  fill(sel,['All',...(is41()?V41_MODEL_FAMS:MODEL_FAMS)]);
  if([...sel.options].some(o=>o.value===prev))sel.value=prev;}}
function renderMetrics(){{
  const resp=document.getElementById('selm_resp').value,fam=document.getElementById('selm_fam').value;
  let rows=pickMetrics().slice();
  if(resp!=='All')rows=rows.filter(r=>r.response===resp);
  if(fam!=='All')rows=rows.filter(r=>r.model.split('_')[0]===fam);
  const k=mSort.k,d=mSort.dir;
  rows.sort((a,b)=>{{let x=a[k],y=b[k];return typeof x==='string'?d*x.localeCompare(y):d*(x-y);}});
  document.getElementById('metricsBody').innerHTML=rows.map(r=>`<tr>
    <td class="txt mono">${{r.model}}</td><td class="txt">${{r.response}}</td>
    <td>${{r.n_predictors}}</td><td>${{r.n_pairs}}</td>
    <td>${{r.RMSE}}</td><td>${{r.MAE}}</td>
    <td style="background:${{r2color(r.R2)}};color:#fff">${{r.R2.toFixed(3)}}</td>
    ${{CV==='rep'?`<td>${{r.pred_sd!=null?r.pred_sd:''}}</td>`:''}}</tr>`).join('');
  document.getElementById('th_predsd').style.display = (CV==='rep') ? '' : 'none';
  document.getElementById('cap_metrics').textContent=`${{rows.length}} rows · dataset: ${{dsName()}} · learner: ${{LFAM[CFG]}} Optuna · CV: `+(CV==='rep'?'repeated (3 x 80% of sites, mean of reps; pred SD = stability across reps)':'leave-one-site-out');
  document.querySelectorAll('#metricsTbl th').forEach(th=>{{th.classList.remove('sorted','desc');
    if(th.dataset.k===k){{th.classList.add('sorted');if(d<0)th.classList.add('desc');}}}});
}}
const DSLABEL={{sites_with_high_Tcover:'V10 · tree cover \\u226530% (93)',v10f:'V10 · flux totals (93)',v41:'v4.1 paper dataset (106)',v41f:'v4.1 · flux totals (102)'}};
function dsName(){{return DSLABEL[DS]||DS;}}
function rMeta(){{const m=META[DS];document.getElementById('metaBar').innerHTML=
  `<span class="chip"><b>${{m.sites}}</b> sites</span>`+
  `<span class="chip"><b>${{m.siteyears}}</b> site-years</span>`+
  `<span class="chip">IGBP: ${{m.igbp}}</span>`;}}
const val=id=>document.getElementById(id).value;
function r1(){{setImg('img_fig1',is41()?`${{V41()}}fig1_site_map.png`:`${{DS}}/fig1_site_map_EFP_distributions.png`);}}
function r2(){{if(!is41())setImg('img_bar',`${{DS}}/bar_sites_per_IGBP.png`);}}
function r3(){{const v=val('sel3_sites'),root=is41()?V41():V10E;
  document.getElementById('note3_high').style.display=(v==='all')?'none':'';
  setImg('img_rmse',`${{root}}fig2${{v==='all'?'':'_'+v}}_disturbance_effect_M3M4.png`,'cap_rmse');}}
function r4(){{if(is41())return;setImg('img_thr1',`${{DS}}/01_abs_mort_comparison.png`);
  setImg('img_thr2',`${{DS}}/02_rel_mort_comparison.png`);
  setImg('img_thr3',`${{DS}}/03_rel_dist_comparison.png`);}}
function r5(){{const allv=!is41()&&val('sel5_view')==='all';
  showCtl('sel5_efp',allv);showCtl('sel5_metric',!allv);
  document.getElementById('note5_all').style.display=allv?'':'none';
  if(allv){{setImg('img_dbar',`${{P()}}${{DS}}/RMSE/delta_RMSE_bars_${{val('sel5_efp')}}.png`,'cap_dbar');return;}}
  setImg('img_dbar',`${{is41()?V41():V10E}}fig_site_dRMSE_${{val('sel5_metric')}}.png`,'cap_dbar');}}
function r6(){{if(is41())return;const mg=document.getElementById('sel6_merged').checked;
  const sub=mg?'delta_RMSE_category/merged_low_disturbance/':'delta_RMSE_category/';
  setImg('img_dcat',`${{P()}}${{DS}}/RMSE/${{sub}}delta_RMSE_${{val('sel6_unit')}}_${{val('sel6_method')}}.png`,'cap_dcat');}}
function r7(){{
  const im=document.getElementById('img_sshap'),sig=val('sel7_view')==='signed';
  im.style.display='';
  const show=(id,on)=>{{document.getElementById(id).closest('.ctrl').style.display=on?'':'none';}};
  show('sel7_metric10',sig);show('sel7_model',!sig);show('sel7_efp',!sig);
  document.getElementById('note7_signed').style.display=(sig&&!is41())?'':'none';
  if(!is41()&&sig){{
    if(CFG==='rfopt'){{im.style.display='none';document.getElementById('cap_sshap').textContent='Signed disturbance SHAP was computed for XGBoost and LightGBM only - switch the learner.';return;}}
    setImg('img_sshap',`${{V10E}}fig3b_signed_${{val('sel7_metric10')}}_${{val('sel7_win')}}_${{LFAM[CFG]}}.png`,'cap_sshap');return;}}
  if(is41()){{const m=val('sel7v_metric'),w=val('sel7v_win'),L=LFAM[CFG];
    const f=val('sel7v_style')==='signed'?`fig3b_signed_${{m}}_${{w}}_${{L}}.png`:`fig3_${{m}}_${{w}}_${{L}}.png`;
    setImg('img_sshap',V41()+f,'cap_sshap');return;}}
  setImg('img_sshap',`${{PS()}}${{DS}}/site_SHAP/site_shap_${{val('sel7_model')}}_${{val('sel7_efp')}}_${{val('sel7_win')}}_distmetrics.png`,'cap_sshap');}}
function r8(){{
  if(is41()){{setImg('img_gshap',`${{V41()}}fig4_signed_shap_${{val('sel8v_metric')}}.png`,'cap_gshap');return;}}
  const mg=document.getElementById('sel8_merged').checked;
  const sub=mg?'SHAP_percent/merged_low_disturbance/':'SHAP_percent/';
  setImg('img_gshap',`${{PS()}}${{DS}}/${{sub}}SHAP_percent_${{val('sel8_model')}}_${{val('sel8_win')}}_${{val('sel8_method')}}.png`,'cap_gshap');}}
function r9(){{
  if(is41()){{setImg('img_igbp',`${{V41()}}fig5_igbp_board_${{val('sel9v_win')}}_${{LFAM[CFG]}}.png`,'cap_igbp');return;}}
  const t=val('sel9_type'),id=val('sel9_id');
  let path;
  if(t==='board')path=`${{PS()}}${{DS}}/IGBP_boards/board_IGBP_${{id}}.png`;
  else if(t==='shap_igbp')path=`${{PS()}}${{DS}}/SHAP_percent/IGBP/IGBP_SHAP_${{id}}.png`;
  else if(t==='shap_tc')path=`${{PS()}}${{DS}}/SHAP_percent/treecover/treecover_SHAP_${{id}}.png`;
  else if(t==='rmse_igbp')path=`${{P()}}${{DS}}/RMSE/IGBP/IGBP_deltaRMSE_${{id}}.png`;
  else path=`${{P()}}${{DS}}/RMSE/treecover/treecover_deltaRMSE_${{id}}.png`;
  setImg('img_igbp',path,'cap_igbp');}}
function repop9(){{const t=val('sel9_type'),sel=document.getElementById('sel9_id');
  const prev=sel.value;
  if(t==='rmse_igbp'||t==='rmse_tc'){{fill(sel,DELTA_UNITS);document.getElementById('lbl9').textContent='Comparison';}}
  else{{fill(sel,MODEL_WIN);document.getElementById('lbl9').textContent='Model';}}
  if([...sel.options].some(o=>o.value===prev))sel.value=prev;
  r9();}}
function r10(){{const v=val('sel10_view'),L=LFAM[CFG];
  setImg('img_dcomp',is41()?`${{V41()}}disturbance_split/D_composition_${{v}}_${{L}}.png`:`disturbance_split_v2/D_composition_${{v}}_${{L}}.png`,'cap_dcomp');}}

// dataset-dependent availability: v4.1 and both flux sets have no RF and (here) no LOSO
function syncAvail(){{
  const lim=is41();
  document.body.classList.toggle('ds-v41',is41());
  const rf=document.querySelector('#cfgToggle button[data-cfg="rfopt"]'),
        loo=document.querySelector('#cvToggle button[data-cv="loo"]');
  rf.classList.toggle('off',lim);loo.classList.toggle('off',lim);
  rf.title=lim?'Not run for this dataset':'';loo.title=lim?'Repeated CV only for this dataset':'';
  if(lim&&CFG==='rfopt')CFG='xgbopt';
  if(lim&&CV==='loo')CV='rep';
  document.querySelectorAll('#cfgToggle button').forEach(x=>x.classList.toggle('active',x.dataset.cfg===CFG));
  document.querySelectorAll('#cvToggle button').forEach(x=>x.classList.toggle('active',x.dataset.cv===CV));
  fillFam();fillResp();
}}
function renderAll(){{syncAvail();[rMeta,renderMetrics,r1,r2,r3,r4,r5,r6,r7,r8,r9,r10]
  .forEach(f=>{{try{{f();}}catch(e){{console.error('renderAll: '+f.name+' failed',e);}}}});}}

document.querySelectorAll('#cvToggle button').forEach(b=>b.addEventListener('click',()=>{{
  if(b.classList.contains('off'))return;CV=b.dataset.cv;renderAll();}}));
document.querySelectorAll('#dsToggle button').forEach(b=>b.addEventListener('click',()=>{{
  document.querySelectorAll('#dsToggle button').forEach(x=>x.classList.remove('active'));
  b.classList.add('active');DS=b.dataset.ds;renderAll();}}));
document.querySelectorAll('#cfgToggle button').forEach(b=>b.addEventListener('click',()=>{{
  if(b.classList.contains('off'))return;CFG=b.dataset.cfg;renderAll();}}));
['selm_resp','selm_fam'].forEach(id=>document.getElementById(id).addEventListener('change',renderMetrics));
document.querySelectorAll('#metricsTbl th').forEach(th=>th.addEventListener('click',()=>{{
  const k=th.dataset.k;if(mSort.k===k)mSort.dir*=-1;else{{mSort.k=k;mSort.dir=(k==='R2')?-1:1;}}renderMetrics();}}));
['sel3_sites'].forEach(id=>document.getElementById(id).addEventListener('change',r3));
['sel5_efp','sel5_view','sel5_metric'].forEach(id=>document.getElementById(id).addEventListener('change',r5));
['sel6_unit','sel6_method','sel6_merged'].forEach(id=>document.getElementById(id).addEventListener('change',r6));
['sel7_view','sel7_metric10','sel7_model','sel7_efp','sel7_win','sel7v_metric','sel7v_win','sel7v_style'].forEach(id=>document.getElementById(id).addEventListener('change',r7));
['sel8_model','sel8_win','sel8_method','sel8_merged','sel8v_metric'].forEach(id=>document.getElementById(id).addEventListener('change',r8));
['sel9v_win'].forEach(id=>document.getElementById(id).addEventListener('change',r9));
['sel10_view'].forEach(id=>document.getElementById(id).addEventListener('change',r10));
document.getElementById('sel9_type').addEventListener('change',repop9);
document.getElementById('sel9_id').addEventListener('change',r9);

const lb=document.getElementById('lb'),lbimg=document.getElementById('lbimg');
document.querySelectorAll('.imgwrap img').forEach(im=>im.addEventListener('click',()=>{{
  if(!im.src)return;lbimg.src=im.src;lb.classList.add('open');}}));
lb.addEventListener('click',()=>lb.classList.remove('open'));

repop9();
renderAll();
</script>
</body>
</html>
"""

html = head + header + s0 + "\n" + body + new_sections + s13 + s14 + tail + "\n" + script

# ---------------------------------------------------------------- dataset naming (user, 2026-10-01)
# V2.2 EFP (93) | V2.2 Flux (93) | V4.1 EFP (106) | V4.1 Flux (102). "V2.2" = the analysis so far called
# V10 (deadtrees v2-2 maps); folders, scripts and file names keep "V10" / "v10". Applied as a last pass
# to visible text only: <code> spans (paths), tag attributes and the product names "deadtrees v2-2 /
# v4.1" are left alone.
NAMES = [("V10 · tree cover &ge;30% (93)", "V2.2 EFP (93)"), ("V10 · tree cover \\u226530% (93)", "V2.2 EFP (93)"),
         ("V10 · flux totals (93)", "V2.2 Flux (93)"), ("v4.1 paper dataset (106)", "V4.1 EFP (106)"),
         ("v4.1 · flux totals (102)", "V4.1 Flux (102)"),
         ("V10 · tree cover &ge;30%", "V2.2 EFP"), ("V10 · flux totals", "V2.2 Flux"), ("V10 flux totals", "V2.2 Flux"),
         ("v4.1 paper dataset", "V4.1 EFP"), ("v4.1 · flux totals", "V4.1 Flux"), ("v4.1 flux totals", "V4.1 Flux")]
for a_, b_ in NAMES:
    html = html.replace(a_, b_)
cut_ = html.index("<script>")
def _visible(txt):
    out = []
    for part in re.split(r"(<code>.*?</code>|<[^>]+>)", txt, flags=re.S):      # keep code spans and tags as they are
        if not part.startswith("<"):
            part = re.sub(r"\bV10\b", "V2.2", part)
            part = re.sub(r"(?<!deadtrees )\bv4\.1\b", "V4.1", part)
        out.append(part)
    return "".join(out)
html = _visible(html[:cut_]) + html[cut_:]
note = ('    <p class="itxt"><b>Naming.</b> <b>V2.2</b> is the analysis previously called V10 (deadtrees v2-2 maps, 93 sites); '
        '<b>V4.1</b> is the new paper dataset (deadtrees v4.1 maps). Folder and file names still use <code>V10</code>.</p>\n')
anchor = '    <table class="deftbl">\n      <thead><tr><th>Dataset</th><th>Sites</th>'
assert html.count(anchor) == 1
html = html.replace(anchor, note + anchor)
OUT.write_text(html)
print(f"written {OUT} ({len(html)/1024:.0f} KB)")
