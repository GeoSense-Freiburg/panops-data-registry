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

```
paper_code_data/
├── README.md                     this file
├── build_package.py              assembles the package from the project (re-run after any change)
├── make_source_data.R            writes source_data/ from data/ only
├── environment/                  R and Python package versions used
├── code/
│   ├── 01_flux_data/             FLUXNET download (Shuttle), ecosystem functional properties, growing season, monthly climate
│   ├── 02_mortality/             tree mortality metrics from deadtrees.earth maps (5 buffers, per year)
│   ├── 03_dataset/               assembly: pre-season climate lags, mortality lags, traits, site selection
│   ├── 04_models_autogluon/      main learner: AutoGluon, repeated leave-one-site-out, permutation SHAP
│   ├── 05_models_xgb_lgb/        checks: XGBoost and LightGBM with Optuna tuning, TreeSHAP
│   ├── 06_statistics/            bootstrap and permutation tests, mixed models, robustness, drop-column test
│   └── 07_figures/               one script per figure type
├── data/
│   ├── analysis_dataset_site_years.csv     101 sites, 361 site-years: EFPs, climate, mortality, traits (all predictors)
│   ├── site_metadata.csv                   site, coordinates, IGBP, network, years, FLUXNET data citation (PID/DOI)
│   ├── model_outputs/                      cross-validated predictions, metrics and SHAP of all learners
│   └── statistics/                         statistics behind the tables and figures
└── source_data/                  one Excel file per main display item
```

## Figures and tables

Main display items (numbering of the current draft):

| Item | Content | Script(s) | Source data |
|---|---|---|---|
| Fig. 1 | Sites and EFP distributions | `07_figures/20_fig1_site_map.R` | `Fig1_sites_and_EFPs.xlsx` |
| Table 1 | Skill (R²) with and without mortality, AutoGluon | `04_models_autogluon/02_loso_repcv.py` | `Table1_skill.xlsx` |
| Table 2 | Site-year MAE change, paired bootstrap and permutation | `06_statistics/39_paired_bootstrap.R`, `40_table_bootstrap.R` | `Table2_siteyear_MAE_bootstrap.xlsx` |
| Fig. 2 | Net mortality SHAP per site (compact) | `07_figures/32_signed_shap_per_site.R` (via `05_figures_autogluon.sh`) | `Fig2_net_mortality_shap_per_site.xlsx` |
| Fig. 3 | High vs Low+Mid sites | `07_figures/23_high_vs_lowmid.R` (wide layout via `05b_figures_autogluon_only.sh`) | `Fig3_high_vs_lowmid.xlsx` |
| Fig. 4 | Total mortality vs direction (panel E of the board) | `07_figures/24_igbp_board_and_mortality_vs_direction.R` | `Fig4_mortality_vs_direction.xlsx` |
| Fig. 5 | Residual by mortality class, with correction test | `07_figures/43_residual_by_mortality_class.R` | `Fig5_residual_by_mortality_class.xlsx` |

Extended Data / Supplementary items: per-site MAE and log-ratio permutation (`21_per_site_error.R`,
`06_statistics/31_table_site_level_tests.R`), per-site ΔRMSE (`29_site_delta_rmse.R`), per-site SHAP with site names
(`25_…`, `32_…`), IGBP board (`24_…`), mortality metrics and drop-column test (`26_…`, `37_…`, `38_…`), mixed models and
robustness (`41_…`, `42_…`), circularity check (`04_models_autogluon/run_efpclim.sh`, `39_…` with `AG_EXTRA`).

## Input data (not redistributed here)

| Data | Source | Licence / attribution |
|---|---|---|
| Eddy covariance, FLUXNET ONEFlux v1.3, half-hourly | FLUXNET Shuttle (ICOS Carbon Portal, AmeriFlux), downloaded 12 May 2026 (`code/01_flux_data/12_FLUXNET_shuttle.ipynb`) | CC-BY-4.0; cite each site (`data/site_metadata.csv`), FLUXNET acknowledgement text, Pastorello et al. 2020 |
| Tree mortality maps (standing deadwood and tree cover, version 5) | deadtrees.earth (Mosig et al.) | [check licence] |
| Plant trait maps, 1 km community-weighted means | Lusk et al. 2026, Zenodo doi:10.5281/zenodo.14646322 | [check licence] |

The derived data in `data/` (ecosystem functional properties, mortality metrics, climate and trait predictors per
site-year) are sufficient to reproduce all models, statistics and figures without the raw inputs.

## Reproducing

From the analysis-ready dataset (hours, CPU):

1. AutoGluon: `code/04_models_autogluon/run_efp.sh efp_v5_nothresh` (repeated leave-one-site-out, SHAP; about 5 h on
   ~100 cores, < 250 GB RAM); circularity check `run_efpclim.sh` (about 3 h).
2. XGBoost / LightGBM: `PAPER_NO_THRESH=1 bash code/05_models_xgb_lgb/run_variant.sh efp_v5` (Optuna tuning, repeated
   leave-one-site-out, TreeSHAP).
3. Statistics: `06_statistics/39_paired_bootstrap.R`, `40_table_bootstrap.R`, `41_…`, `42_…`, `43_…` (minutes).
4. Figures: `07_figures/…` and `05_figures_autogluon.sh`, `05b_figures_autogluon_only.sh`.
5. Source data: `Rscript make_source_data.R`.

From the raw data (days): `01_flux_data` (download, EFPs, growing season, monthly climate) → `02_mortality` →
`03_dataset` (`PAPER_MAPS_VERSION=v5`), then the steps above.

Settings used for the paper: deadtrees maps v5, mortality block without thresholded metrics
(`PAPER_RESP_SET=efp_v5`, `PAPER_NO_THRESH=1`; AutoGluon set `efp_v5_nothresh`).

Software: R 4.3.3 and Python 3 (AutoGluon 1.6.3); exact versions in `environment/`.

## Known limitations of this package (to fix before deposit)

- Scripts still contain absolute paths of the analysis server (`/mnt/gsdata/...`) and read inputs from the project
  layout; a `config` with relative paths is needed so the package runs as deposited.
- Raw inputs are not included (licences and size); `code/01_flux_data` documents how they were obtained.
- Licences for code (e.g. MIT) and data (e.g. CC-BY-4.0) still have to be chosen by the authors.
