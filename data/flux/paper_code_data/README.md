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
│   ├── optuna_tune.py         Optuna driver for XGBoost / LightGBM
│   └── paper_v41/             paper_common.R (shared settings) and all R/Python steps, numbered:
│                              01-04 data (mortality metrics, NEPmax/growing season/climate, dataset, traits)
│                              10-13 XGBoost / LightGBM (tuning, repeated leave-one-site-out, TreeSHAP)
│                              20-32 figures; 31, 37-43 statistics
├── AutoGluon/                 ag_common.py, 01-03 (fits, repeated leave-one-site-out, SHAP), run_*.sh, 05*_figures.sh
│   └── outputs/               efp_v5_nothresh (main run), efp_v5_nothresh_efpclim (circularity check)
├── derived_tables/
│   ├── paper_v5/              analysis-ready dataset: 101 sites, 361 site-years, all predictors
│   └── paper_v5_nothresh/models/   XGBoost / LightGBM predictions, metrics, SHAP
└── plots/paper_v5_nothresh/   tuned hyperparameters (Optuna/*/best_configs.csv) and statistics tables
```

Server-specific inputs are read from environment variables when needed: `PAPER_ROOT` (package root, default `.`),
`AG_WORK` (scratch folder for AutoGluon models, default `AutoGluon/`), `TRAIT_MAPS_DIR` (raw trait maps, default
`raw/trait_maps`). Raw FLUXNET and mortality-map files are expected under the same relative paths as in the project
(`fluxnet_2017_2025_V02/`, `deadtree/deadtrees_maps_v5/`) if the pipeline is run from the raw data.

Checked on 2026-10-13: a copy of the package in another folder reproduces the bootstrap statistics and residual
corrections byte-for-byte (`scripts/paper_v41/39_paired_bootstrap.R`, `43_residual_by_mortality_bin.R`) and rebuilds all
source data.

## Figures and tables

Main display items (numbering of the current draft):

| Item | Content | Script(s) | Source data |
|---|---|---|---|
| Fig. 1 | Sites and EFP distributions | `scripts/paper_v41/20_fig1_site_map.R` | `Fig1_sites_and_EFPs.xlsx` |
| Table 1 | Skill (R²) with and without mortality, AutoGluon | `AutoGluon/02_loso_repcv.py` | `Table1_skill.xlsx` |
| Table 2 | Site-year MAE change, paired bootstrap and permutation | `scripts/paper_v41/39_paired_bootstrap.R`, `40_table_bootstrap.R` | `Table2_siteyear_MAE_bootstrap.xlsx` |
| Fig. 2 | Net mortality SHAP per site (compact) | `scripts/paper_v41/32_fig3_landscape.R` (via `AutoGluon/05_figures.sh`) | `Fig2_net_mortality_shap_per_site.xlsx` |
| Fig. 3 | High vs Low+Mid sites | `scripts/paper_v41/23_fig4_signed_shap.R` (wide layout via `AutoGluon/05b_figures_ag_only.sh`) | `Fig3_high_vs_lowmid.xlsx` |
| Fig. 4 | Relative mortality vs direction (panel E of the board) | `scripts/paper_v41/24_fig5_igbp_board.R` | `Fig4_mortality_vs_direction.xlsx` |
| Fig. 5 | Residual by mortality class, with correction test | `scripts/paper_v41/43_residual_by_mortality_bin.R` | `Fig5_residual_by_mortality_class.xlsx` |

Extended Data / Supplementary items (all in `scripts/paper_v41/`): per-site MAE and log-ratio permutation (`21_fig2_M3M4.R`
with `FIG2_STAT=mae_perm`, `31_table_fig2_summary.R`), per-site ΔRMSE (`29_site_delta_rmse.R`), per-site SHAP with site
names (`25_…`, `32_…`), IGBP board (`24_…`), mortality metrics and drop-column test (`26_…`, `37_…`, `38_…`), mixed
models and robustness (`41_…`, `42_…`), circularity check (`AutoGluon/run_efpclim.sh`, `39_…` with
`AG_EXTRA=efp_v5_nothresh_efpclim`).

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
2. XGBoost / LightGBM: `PAPER_NO_THRESH=1 bash scripts/paper_v41/run_variant.sh efp_v5` (Optuna tuning, repeated
   leave-one-site-out, TreeSHAP).
3. Statistics: `Rscript scripts/paper_v41/39_paired_bootstrap.R`, `40_…`, `41_…`, `42_…`, `43_… AG 24m` (minutes).
4. Figures: `scripts/paper_v41/2*_…`, `32_…`, `bash AutoGluon/05_figures.sh efp_v5_nothresh`, `bash AutoGluon/05b_figures_ag_only.sh`.
5. Source data: `Rscript make_source_data.R`.

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
