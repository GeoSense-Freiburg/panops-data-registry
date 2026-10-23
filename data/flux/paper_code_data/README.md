# Code and data: tree mortality and ecosystem functional properties

Code, analysis-ready data, model outputs and figure source data for
*[title]* ([authors], submitted to *Nature Ecology & Evolution*).
Intended for deposit on Zenodo; the DOI goes into the paper's **Data availability** and **Code availability**
statements. This folder is rebuilt from the project with `build_package.py`.

## What the journal asks for (Nature Ecology & Evolution / Nature Portfolio)

| Requirement | Where |
|---|---|
| Data availability statement; data in a public repository with a DOI where possible | `data/` + Zenodo DOI |
| Code availability statement; code in a public repository, cited with a DOI | `code/` + Zenodo DOI |
| Source data for figures, "one file for each relevant figure", statistics source data in Excel | `source_data/*.xlsx` |
| Third-party data cited and attributed (FLUXNET CC-BY-4.0: one citation per site in the reference list) | `data/site_metadata.csv` (column `product_citation`), paper references 41–141 |

## Folder structure

The package mirrors the analysis project's layout, because the scripts find each other and their inputs through these
relative paths. **Run every command from the package root** (or set `PAPER_ROOT` to it); no server paths are left.

```
paper_code_data/
├── README.md, build_package.py, make_source_data.R
├── site_metadata.csv          site, coordinates, IGBP, network, years, FLUXNET data citation (PID/DOI) per site
├── environment/               R and Python package versions
├── source_data/               one Excel file per main display item
├── notebooks/                 12_FLUXNET_shuttle.ipynb (download), 14_calc_EFPs.ipynb (ecosystem functional properties)
├── scripts/
│   ├── optuna_tune.py         Optuna driver for XGBoost / LightGBM (earlier check, not shown in the paper)
│   └── paper_v41/             paper_common.R (shared settings) and all R/Python steps, numbered:
│                              01-04 data (mortality metrics, NEPmax/growing season/climate, dataset, traits)
│                              10-13 XGBoost / LightGBM (earlier check, not shown in the paper)
│                              31, 37-43 statistics; 53 respiration check
│                              50-52, 55 + fig_style.R: figures of the current draft (Nature style)
│                              20-32 older figure layouts (kept for reference)
├── AutoGluon/                 ag_common.py, 01-03 (fits, repeated leave-one-site-out, SHAP), run_*.sh, 05*_figures.sh
│   └── outputs/               efp_v5_nothresh (main run), efp_v5_nothresh_efpclim (circularity check)
├── derived_tables/
│   ├── paper_v5/              analysis-ready dataset: 101 sites, 361 site-years, all predictors
│   └── paper_v5_nothresh/models/   XGBoost / LightGBM predictions, metrics, SHAP (earlier check, not shown)
├── fluxnet_2017_2025_V02/     FLUXNET Shuttle snapshot (site coordinates and IGBP for the map)
└── plots/paper_v5_nothresh/   statistics tables (bootstrap, dose-response, R² bootstrap, respiration check) and
                               tuned hyperparameters (Optuna/*/best_configs.csv)
```

Server-specific inputs are read from environment variables when needed: `PAPER_ROOT` (package root, default `.`),
`AG_WORK` (scratch folder for AutoGluon models, default `AutoGluon/`), `TRAIT_MAPS_DIR` (raw trait maps, default
`raw/trait_maps`). Raw FLUXNET and mortality-map files are expected under the same relative paths as in the project
(`fluxnet_2017_2025_V02/`, `deadtree/deadtrees_maps_v5/`) if the pipeline is run from the raw data.

Checked on 2026-10-21: in a copy of the package, `50_figures_nature.R`, `51_appendix_figures_nature.R` and
`52_skill_bars.R` run from the package files alone (the R² bootstrap is reproduced byte-for-byte).
Checked on 2026-10-13: a copy of the package in another folder reproduces the bootstrap statistics and residual
corrections byte-for-byte (`scripts/paper_v41/39_paired_bootstrap.R`, `43_residual_by_mortality_bin.R`) and rebuilds all
source data.

## Figures and tables

Main display items (numbering of the coauthor draft, 21 October 2026; AutoGluon only). Script `50_figures_nature.R`
keeps its own internal figure names, given in brackets.

| Item | Content | Script(s) | Source data |
|---|---|---|---|
| Fig. 1 | Sites and EFP distributions (to be replaced by a workflow schematic) | `scripts/paper_v41/50_figures_nature.R AG 24m` [fig1] | `Fig1_sites_and_EFPs.xlsx` |
| Table 1 | Skill (R²) with and without mortality | `AutoGluon/02_loso_repcv.py` | `Table1_skill.xlsx` |
| Table 2 | Site-year MAE change, paired bootstrap and permutation | `scripts/paper_v41/39_paired_bootstrap.R`, `40_table_bootstrap.R` (`TABLE_COLS=siteyear`) | `Table2_siteyear_MAE_bootstrap.xlsx` |
| Fig. 2 | R² and relative MAE decrease (bar plots of Tables 1 and 2) | `scripts/paper_v41/52_skill_bars.R` | `Fig2_skill_R2_and_MAE_decrease.xlsx` |
| Fig. 3 | Residual by mortality class, with correction test | `43_residual_by_mortality_bin.R AG 24m`, then `50_figures_nature.R AG 24m` [fig5] | `Fig3_residual_by_mortality_class.xlsx` |
| Fig. 4 | Net mortality SHAP per site, site codes | `FIG2_PALETTE=piyg FIG2_NAMES=1 Rscript scripts/paper_v41/50_figures_nature.R AG 24m` [fig2] | `Fig4_net_mortality_shap_per_site.xlsx` |
| Fig. 5 | High vs Low+Mid sites | `50_figures_nature.R AG 24m` [fig3] | `Fig5_high_vs_lowmid.xlsx` |
| Fig. 6 | Site maximum relative mortality vs net mortality SHAP | `50_figures_nature.R AG 24m` [fig4] | `Fig6_mortality_vs_direction.xlsx` |

Appendix figures A1–A13 (all AutoGluon): `scripts/paper_v41/51_appendix_figures_nature.R` draws per-site MAE (A1, A2),
per-site and per-site-year error change (A3, A4), High vs Low+Mid by relative deadwood (A6), IGBP and tree-cover boards
(A7, A8), metric shares in the mortality block (A9), mixed-model robustness and site-year slopes (A11, A12) and the
circularity check (A13); A5 and A10 (12-month window) come from `50_figures_nature.R AG 12m` [fig2, fig5]. Appendix
tables: `31_table_fig2_summary.R` (`TABLE1_TEST=permlog`, site-level log-ratio test), `40_table_bootstrap.R` (full table,
RMSE, circularity check with `AG_EXTRA=efp_v5_nothresh_efpclim`). Benjamini–Hochberg corrections run over the AutoGluon
tests shown. Respiration check (Discussion): `53_reco_mortality_check.R`.

## Input data (not redistributed here)

| Data | Source | Licence / attribution |
|---|---|---|
| Eddy covariance, FLUXNET ONEFlux v1.3, half-hourly | FLUXNET Shuttle (ICOS Carbon Portal, AmeriFlux), downloaded 12 May 2026 (`code/01_flux_data/12_FLUXNET_shuttle.ipynb`) | CC-BY-4.0; cite each site (`data/site_metadata.csv`), FLUXNET acknowledgement text, Pastorello et al. 2020 |
| Tree mortality maps (standing deadwood and tree cover, version 5) | deadtrees.earth (Mosig et al.) | [check licence] |
| Plant trait maps, 1 km community-weighted means | Lusk et al. 2026, Zenodo doi:10.5281/zenodo.14646322 | [check licence] |

The derived data in `data/` (ecosystem functional properties, mortality metrics, climate and trait predictors per
site-year) are sufficient to reproduce all models, statistics and figures without the raw inputs.

## Reproducing

Run from the package root. From the analysis-ready dataset (hours, CPU):

1. AutoGluon: `bash AutoGluon/run_efp.sh efp_v5_nothresh` (repeated leave-one-site-out and SHAP; about 5 h on ~100 cores,
   < 250 GB RAM); circularity check `bash AutoGluon/run_efpclim.sh` (about 3 h).
2. Optional, not shown in the paper: XGBoost / LightGBM, `PAPER_NO_THRESH=1 bash scripts/paper_v41/run_variant.sh efp_v5`.
3. Statistics: `Rscript scripts/paper_v41/39_paired_bootstrap.R`, `40_…`, `31_…`, `41_… AG`, `42_… AG`,
   `43_… AG 24m` and `43_… AG 12m` (minutes).
4. Figures: `Rscript scripts/paper_v41/50_figures_nature.R AG 24m` (and `AG 12m`; Fig. 4 with
   `FIG2_PALETTE=piyg FIG2_NAMES=1`), `51_appendix_figures_nature.R`, `52_skill_bars.R`; output in
   `plots/paper_v5_nothresh/nature/`.
5. Source data: `Rscript make_source_data.R`.
6. Respiration check: `Rscript scripts/paper_v41/53_reco_mortality_check.R` needs the raw FLUXNET daily files
   (`fluxnet_2017_2025_V02/*.zip`); its results are included (`plots/paper_v5_nothresh/reco_check/`).

From the raw data (days): `notebooks/12_FLUXNET_shuttle.ipynb` (download) → `notebooks/14_calc_EFPs.ipynb` →
`scripts/paper_v41/02_…` (NEPmax, growing season, monthly climate) → `01_…` (mortality metrics, `DEADTREE_VERSION=v5`) →
`04_…` (traits) → `03_…` (dataset, `PAPER_MAPS_VERSION=v5`), then the steps above.

Settings used for the paper: deadtrees maps v5, mortality block without thresholded metrics
(`PAPER_RESP_SET=efp_v5`, `PAPER_NO_THRESH=1`; AutoGluon set `efp_v5_nothresh`). Software: R 4.3.3 and Python 3
(AutoGluon 1.6.3); exact versions in `environment/`.

## Still to do before deposit

- Licences: code (e.g. MIT) and data (e.g. CC-BY-4.0) to be chosen by the authors; check the licences of the
  deadtrees.earth maps and the trait maps (only derived values are included).
- Raw inputs are not included (size, licences); the notebooks and scripts document how they were obtained.
- Some scripts write tables to `manuscript/tables/` (LaTeX); create that folder if those steps are rerun.
