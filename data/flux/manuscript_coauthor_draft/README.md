# Coauthor working draft

Bullet points and figures only — no prose. Built to support decisions, not to be submitted.

## Scope (updated 2026-10-09: deadtrees v5, AutoGluon as main learner)

| | |
|---|---|
| Dataset | deadtrees v5, tree cover >= 30 % (500 m, site mean), CN-Sb1/2 removed — **101 sites / 361 site-years** |
| Models | **C+T vs C+T+M** only |
| Learners | **AutoGluon** (main text); XGBoost, LightGBM (Optuna-tuned) in the Appendix |
| Windows | 12 m and 24 m, climate strictly pre-season |
| Cross-validation | repeated leave-one-site-out (3 x 80 %) |
| Responses | GPPsat, NEPmax, ETmax, WUE |
| Site classes | mean of the five buffers 100-500 m (site maximum, mean +/- 0.5 SD) |
| Statistics | MAE: site-level log-ratio permutation test (Table 2), paired bootstrap incl. site-year level (Table 3); RMSE and XGB/LGB versions in the Appendix |

Sources: AutoGluon-only figures from `AutoGluon/plots/efp_v5_nothresh/ag_only/` (made by the copies in
`AutoGluon/fig_scripts/ag_only/`), three-learner figures from `AutoGluon/plots/efp_v5_nothresh/`, XGB/LGB figures
from `plots/paper_v5_nothresh/`. Tables from `manuscript/tables/` (`31_table_fig2_summary.R` with
`TABLE1_TEST=permlog TABLE1_SHOW=AG|XGB,LGB`, and `40_table_bootstrap.R`), copied to `tables/` with short names.
The v4.1 version of the draft is in git history and in Overleaf's history.

## Files

- `draft_coauthors.tex` — the draft (compile with pdflatex, no bibliography needed)
- `figures/fig1_site_map.*` — sites and EFP distributions (copied from the report)
- `figures/fig2_disturbance_effect_M3M4.*` — paired violins, 4 EFPs × 3 learners × (12m/24m × LOO/rep)
- `figures/fig3_site_shap_<EFP>.*` — per-site SHAP composition, M4, 12 m, XGBoost-Optuna
  (taken unchanged from the report, Section 8)
- `figures/fig4_grouped_shap_<learner>.*` — disturbance SHAP by Low+Mid vs High, M4, 12 m,
  natural breaks (taken unchanged from the report, Section 9; still includes uWUE)
- `fig2_statistics.csv`, `fig3_shap_shares.csv`, `fig4_statistics.csv` — the numbers behind each figure

## Regenerating

```bash
RS=/home/nk1125/miniconda3/envs/clean_r_env/bin/Rscript
cd /mnt/gsdata/projects/panops/panops-data-registry/data/flux
$RS scripts/plot_draft_fig2_M3M4.R
$RS scripts/plot_draft_fig3_site_shap.R
$RS scripts/plot_draft_fig4_grouped_shap.R
```

## Known issue

`RF_v10_optuna`'s SHAP table lost the names of the 14 trait variables containing spaces,
parentheses or slashes (`Leaf C`, `Leaf N (area)`, `Leaf C/N ratio`, …), leaving ~35% of the
attribution in an unlabelled group. Group membership is unambiguous — 14 unnamed + 7 named =
the 21 traits the other two learners show — so the summary percentages quoted in the draft reassign them to Traits. Figures 3 and 4 are
copied unchanged from the report, so the RF panels of Fig 4 still carry the unfixed grouping.
The extraction in `scripts/run_v10_RF_optuna_SHAP.R` should still be fixed at source; this
also affects the RF-Optuna panels of Sections 8–9 in the interactive report.

## Sharing with coauthors via Overleaf

Overleaf cannot create a project from a git push, so the project has to exist first.

### Option A — upload the zip (fastest, no git)

1. `coauthor_draft_overleaf.zip` (3.9 MB) is in this folder.
2. overleaf.com → **New Project → Upload Project** → pick that zip.
3. **Menu → Share** → invite coauthors, or turn on link sharing.

Re-uploading later creates a *new* project, so use Option B if you expect to iterate.

### Option B — link it to git (keeps one project, updates in place)

1. overleaf.com → **New Project → Blank Project**, name it e.g. *EFP mortality — coauthor draft*.
2. Copy the 24-character id from the URL: `https://www.overleaf.com/project/<PROJECT_ID>`.
3. Run once with the id:

```bash
cd /mnt/gsdata/projects/panops/panops-data-registry/data/flux
bash scripts/push_coauthor_draft_to_overleaf.sh <PROJECT_ID> "initial coauthor draft"
```

The id is remembered afterwards, so later updates are just:

```bash
bash scripts/push_coauthor_draft_to_overleaf.sh "" "update figures"
```

4. **Menu → Share** → invite coauthors (or link sharing for comment-only access).

The script reuses the Overleaf token already stored in the main manuscript clone
(`/home/nk1125/overleaf_panops`), so no new credential is created or stored. It syncs only
`draft_coauthors.tex` and `figures/` — the CSVs, README and zip stay out of Overleaf.

**Note:** this is a *separate* Overleaf project from the main manuscript
(`698b0715a5817a2efadd24b6`), so coauthors commenting on the draft cannot touch the submission.
