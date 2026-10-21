# Project Context — FLUXNET × Tree Mortality

**Researcher:** Negin Katal (neggy.k.92@gmail.com)
**Phase (updated 2026-10-21):** analysis complete for the paper; coauthor draft on Overleaf being turned into the full
manuscript for **Nature Ecology & Evolution**.

## Research question

Does tree mortality mapped from remote sensing make ecosystem functional properties (EFPs) of woody ecosystems more
predictable, beyond what climate and plant traits explain — and how does mortality change the predictions?

## Study design (paper set)

- **Sites:** 101 eddy-covariance sites with ≥ 30 % tree cover (mean within 500 m), 361 site-years, years 2020–2025
  (2017–2019 used only as lags). ENF 49, DBF 30, MF 7, WET 6, EBF 3, CSH 3, OSH 2, WSA 1; median 4 years per site.
  "Woody ecosystems", not "forests" (wetlands, shrublands and a woody savanna are included).
- **Flux data:** FLUXNET ONEFlux v1.3 via the FLUXNET Shuttle (`fluxnet_2017_2025_V02/`, downloaded 12 May 2026).
- **Responses (4 EFPs, per site-year):** GPPsat (µmol m⁻² s⁻¹), NEPmax (90th percentile of half-hourly NEP in the growing
  season, after Migliavacca et al. 2021; µmol m⁻² s⁻¹), ETmax (95th percentile of half-hourly ET; labelled mm d⁻¹ by the
  authors' convention), WUE (bigleaf, median GPP/ET; g C mm⁻¹).
- **Predictors:** climate (strictly pre-season monthly lags, 12- or 24-month window), community-weighted plant traits
  (Lusk et al. 1 km maps), mortality block (5 metrics × 5 buffers of 100–500 m, current year + 1 or 2 lag years).
- **Mortality data:** deadtrees.earth maps **version 5**, mortality block **without thresholded metrics**
  (`PAPER_RESP_SET=efp_v5`, `PAPER_NO_THRESH=1`). V4.1 and V2.2 are comparisons only.
- **Models:** C+T (climate + traits, internal name M3) vs C+T+M (+ mortality, M4); **AutoGluon** only (XGBoost and
  LightGBM gave similar patterns and are no longer shown, decision Oct 2026).
- **Validation:** repeated leave-one-site-out cross-validation (3 × random 80 % of the other sites; model selection never
  sees the held-out site).

## Language

| Term | Meaning |
|---|---|
| **EFP** | Ecosystem functional property — a site-year summary of ecosystem function |
| **C+T / C+T+M** | Model without / with the mortality block (internal M3 / M4) |
| **relative mortality** | (deadwood + tree-cover loss) / (tree cover + tree-cover loss) × 100; column `relative_disturbance_*` (called "total mortality" until 2026-10-14) |
| **relative deadwood** | deadwood cover / tree cover; column `relative_mortality_*` |
| **absolute deadwood** | deadwood cover / buffer area; column `absolute_mortality_*` |
| **deadwood gain**, **relative tree-cover loss** | the other two metrics of the mortality block |
| **site maximum relative mortality** | maximum over a site's years of the mean of the five buffers; used for site classes |
| **Low / Mid / High** | site classes: < 10.6 %, between, > 24.0 % site maximum relative mortality (mean ± 0.5 SD); 41 / 38 / 22 sites |
| **site-year level** | every site-year is one observation; whole sites resampled / swapped in the tests |
| **net mortality SHAP** | sum of the signed SHAP values of all mortality predictors, mean over a site's years |
| **correction** | residual of C+T+M minus residual of C+T = how much adding mortality lowers the prediction |
| **12m / 24m window** | length of the climate predictor window (and 1 or 2 mortality lag years) |

Avoid: "disturbance" in paper labels (use mortality); "forests" for the whole site set; "total mortality".

## Key findings

1. Adding mortality makes the EFPs more predictable, most in the years in which trees died: NEPmax MAE −11 to −12 % over
   all site-years and −29 to −32 % in high-mortality years; WUE mainly where mortality occurred; GPPsat less; ETmax little.
2. Without mortality, the models over-predict carbon uptake and WUE in high-mortality years; the bias grows with mortality
   and adding mortality removes about two-thirds of it (significant correction, robust to site resampling and leaving out
   sites).
3. The direction of the mortality effect follows the amount of mortality (Spearman ρ ≈ −0.8 for GPPsat, NEPmax, WUE), not
   vegetation type or tree cover. Within sites, growing-season respiration does not change with mortality while GPP and
   NEP decrease (quick check, decision 0002).

## Current state

- **Coauthor draft:** `manuscript_coauthor_draft/draft_coauthors.tex`, Overleaf project `6a9e806d7176169e80fbae9e`, pushed with
  `bash scripts/push_coauthor_draft_to_overleaf.sh "" "<message>"` (commit each change locally, push to Overleaf at the
  end of the day).
- **Main text display items:** Fig. 1 sites + EFPs (to be replaced by a workflow schematic), Tables 1–2 (skill, MAE
  reduction), Fig. 2 bar plots of R² and MAE decrease, Fig. 3 residuals by mortality class, Figs 4–6 SHAP (per site, High vs
  Low+Mid, vs relative mortality). Appendix A1–A13, AutoGluon only.
- **Open to-dos:** see `meeting's note/2026-10-20-teja-coauthor-draft-meeting.md` and
  `meeting's note/2026-10-21-talie-musavi-recommendations.md` (schematic, figure order, supplement ≤ 10 files, full
  draft; tree-cover threshold test, deadwood edge/patch density).
- **HTML report:** `fluxVSmortality/v10/index.html`, built by `scripts/paper_v41/35_build_report_paper.py`, published only
  via `scripts/push_fluxVSmortality.sh` (never commit inside `fluxVSmortality/`).
- **Code-and-data package:** `paper_code_data/` (`build_package.py`, README, `make_source_data.R` with one Excel file per
  main display item); matches the draft of 21 Oct 2026 (Figs 1–6, Tables 1–2, appendix A1–A13, AutoGluon only).

## Canonical files

| File | Description |
|---|---|
| `derived_tables/paper_v5/paper_dataset_full_lags_monthly_tc30_minrun2_buf100-200-300-400-500.csv` | Paper dataset (EFPs, climate, mortality, traits per site-year) |
| `AutoGluon/outputs/efp_v5_nothresh/AG_predictions_repCV.csv` | AutoGluon leave-one-site-out predictions (C+T and C+T+M) |
| `AutoGluon/outputs/efp_v5_nothresh/AG_site_signed_shap_M4.csv`, `AG_site_shap_M4.csv` | Per-site SHAP |
| `AutoGluon/outputs/efp_v5_nothresh_efpclim/` | Circularity check (EFP-specific climate block) |
| `plots/paper_v5_nothresh/paired_bootstrap_statistics.csv` | Site-year paired bootstrap / permutation (Table 2) |
| `plots/paper_v5_nothresh/dose_response/` | Residual by class, correction tests, mixed models and robustness |
| `plots/paper_v5_nothresh/reco_check/` | Respiration quick check |

## Scripts (paper)

| Script | Purpose |
|---|---|
| `scripts/paper_v41/01–04_*` | Mortality metrics, NEPmax and climate, paper dataset, traits |
| `AutoGluon/01–03_*.py`, `run_efp.sh` | AutoGluon fits, repeated leave-one-site-out, SHAP |
| `scripts/paper_v41/39_paired_bootstrap.R`, `40_table_bootstrap.R`, `31_table_fig2_summary.R` | Error tests and tables |
| `scripts/paper_v41/41–43_*` | Dose–response, robustness, residual by class |
| `scripts/paper_v41/50_figures_nature.R`, `51_appendix_figures_nature.R`, `52_skill_bars.R` | Main, appendix and skill figures (Nature style, `fig_style.R`) |
| `scripts/paper_v41/53_reco_mortality_check.R` | Respiration quick check |

Runtime: R scripts with `/home/nk1125/miniconda3/envs/clean_r_env/bin/Rscript`; AutoGluon and Python with
`/home/nk1125/miniconda3/envs/autogluon_env/bin/python`.

## Key decisions

- **V5 maps, no thresholded mortality metrics** for the paper (2026-10-07).
- **AutoGluon only** in the paper; Benjamini–Hochberg corrections over the AutoGluon tests (2026-10-20).
- **Site-year level as the main test** (years with mortality are not diluted by undisturbed years); whole sites resampled
  and swapped because years of a site are not independent.
- **Strictly pre-season climate predictors**; circularity check without the climate variables inside each EFP formula.
- **Respiration not modelled** (Rb/Rbmax only 55 of 361 site-years) — `context/decisions/0002-respiration-not-modelled-quick-check.md`.
- **Unit labels:** ETmax mm d⁻¹ and WUE g C mm⁻¹ as used by the authors (2026-10-15).

## Earlier phase (June–August 2026, superseded)

Random-forest study on the "24mbench" dataset (644 site-years, 166 sites; deadtrees v2-2), eight model variants
M01–M08 (C, +D, +T, +EFP memory), anomaly vs raw-lag EFP memory, uWUE instead of WUE, noWET subset, main manuscript in
`manuscript/` (Overleaf project `698b0715a5817a2efadd24b6`). Findings then: disturbance helped forests (ENF, DBF, MF) more
than open ecosystems; accumulated standing deadwood carried the signal. Decisions of that phase:
`context/decisions/0001-project-context.md`. These datasets and names (M01–M08, "D", 24mbench) are not used in the
current paper.
