# ICOS forest sites: half-hourly GPP and NEE (FLUXNET / ONEFlux v1.3)

Prepared by Negin Katal, 29 Sept 2026.

## What is in this folder

Half-hourly net ecosystem exchange (NEE) and gross primary production (GPP) for the
**37 forest sites of the ICOS network** that have a FLUXNET product, over each site's full
record (1996–2024, 526 site-years in total). Ecosystem respiration (RECO) and a few
meteorological drivers are included too, so the partitioned fluxes can be checked or
re-aggregated without going back to the source.

| File / folder | Contents |
|---|---|
| `data_HH/<SITE>_HH_GPP_NEE.csv.gz` | One file per site, half-hourly, same 40 columns in every file (see `variables.csv`) |
| `sites.csv` | One row per site: name, IGBP, lat/lon/elevation, years, which NEE variants exist, source zip, **PID and citation**, PI/team names and e-mails |
| `variables.csv` | Column names, units and definitions |
| `data_coverage_by_site_year.csv` | For each site-year, the fraction of half-hours that are measured (QC = 0), measured or good gap-fill (QC ≤ 1), and non-missing for NEE, GPP_NT and GPP_DT |
| `CITATION.md` | **What you must cite and acknowledge** when using these data (read before publishing) |
| `citations_per_site.txt`, `site_citations.bib` | Data citation for each of the 37 sites, as plain text and as BibTeX |
| `DATA_POLICY_LICENSE_AND_INSTRUCTIONS.txt` | Original FLUXNET licence text (CC-BY-4.0) |
| `FLUXNET_PRODUCT_README.txt` | Original FLUXNET product README |
| `metadata/<SITE>/` | Original site metadata files: `*_BIF_*` (site description, canopy height, LAI, biomass, DBH, soils, team) and `*_BIFVARINFO_HH_*` (instrument heights and per-variable definitions) |
| `scripts/build_ICOS_forest_HH.py` | Script that built this folder from the original zips |

## Site selection

- **Source:** FLUXNET data products (ONEFlux v1.3, released 2026), downloaded with the
  FLUXNET Shuttle from the ICOS Carbon Portal on 12 May 2026. DE-RuW was downloaded again
  on 29 Sept 2026.
- **"ICOS network":** products whose source network is ICOS (zip prefix `ICOS_`), which gives 36 sites.
  I also added **IT-SR2 (San Rossore 2)**. It is an ICOS station, but its product was
  processed under the `FLX` prefix. Other European forest sites processed by the European
  Fluxes Database (for example CH-Lae, DE-Lnf, DE-Obe, CZ-RAJ, IL-Yat, RU-Fyo) are **not**
  included. I can add them if you want wider coverage.
- **"Forest":** IGBP class ENF (22 sites), DBF (9), MF (3), EBF (2) or DNF (1).
- **Time:** the full available record for each site (not cut to a study period).

Sites to look at before using them:
- **DK-RCW** is classed DBF but is a short-rotation willow coppice (Risø campus), not a natural forest.
- **GF-Guy** (French Guiana) and **CD-Ygb** (DR Congo) are tropical and have **no night-time GPP (GPP_NT)** for any year. Use GPP_DT for these two.
- **FR-Hes** has **no VUT variant**. Use the `_CUT_` columns.
- **NO-Hur, FI-Sod and DK-RCW** have only VUT, and only 2 years (2023–2024).

## About NPP

**NPP is not available from eddy covariance.** The towers measure the net CO₂ exchange of the
whole ecosystem (NEE). GPP and RECO are then derived from NEE by flux partitioning. Autotrophic
and heterotrophic respiration are not separated, so NPP cannot be derived from these files.

If you need NPP, the site metadata files (`metadata/<SITE>/*_BIF_*`) include biomass
inventories (`GRP_BIOMASS`, 35 sites), DBH (33), tree numbers (34), basal area (30),
litterfall (14) and LAI (36). The number and timing of these surveys vary a lot between sites.
In principle they could give biometric NPP estimates, and the site PIs (listed in `sites.csv`)
would be the right people to ask.

## How to use the data

- **Missing values** are `NA`. The originals used `-9999`.
- **Time:** local standard time, with no daylight saving. `TIMESTAMP_START` / `TIMESTAMP_END` use the format `YYYYMMDDHHMM`. All 37 sites are half-hourly (30 min).
- **Sign convention:** NEE < 0 means CO₂ uptake by the ecosystem. GPP and RECO are positive. Approximately, NEE = RECO − GPP.
- **Units:** µmol CO₂ m⁻² s⁻¹. To get g C m⁻² per half hour, multiply by `1800 × 12.011e-6` (≈ 0.02162).
- **Which variant to use.** This follows the FLUXNET2015 guidance, Pastorello et al. 2020:
  - `NEE_VUT_REF` is the recommended default for NEE. VUT means the u* threshold is allowed to vary from year to year. CUT uses one constant threshold for all years and is better for comparing years at one site. For FR-Hes use the CUT columns.
  - `GPP_NT_VUT_REF` (night-time method) and `GPP_DT_VUT_REF` (daytime method) are two independent partitioning methods. Their difference is a useful measure of partitioning uncertainty. `_05` / `_95` give the spread across the u* ensemble.
  - GPP and RECO are derived from gap-filled NEE, so they have no QC flag of their own. Use `NEE_*_REF_QC` to judge how much of a period was actually measured: 0 = measured, 1 = good gap-fill, 2–3 = medium/poor gap-fill.
- **Gaps:** some site-years have long gaps with NEE and GPP missing, for example FR-Hes 2011–2013 and 2021, IT-Ren 2021, IT-BFt 2023, and BE-Bra 2003–2004. Check `data_coverage_by_site_year.csv` before aggregating. A common screen is to keep site-years with `FRAC_NEE_MEAS_OR_GOODGAPFILL_QC01` ≥ 0.5 (or a similar threshold).
- **Known issue:** `TA_F` has 3 impossible values (about −260 °C) in the original files at FR-Hes and at NL-Loo. I kept them as delivered, so filter them if you use TA_F.

## Reading a file

```python
import pandas as pd
d = pd.read_csv("data_HH/DE-Tha_HH_GPP_NEE.csv.gz",
                dtype={"TIMESTAMP_START": str, "TIMESTAMP_END": str})
d["time"] = pd.to_datetime(d.TIMESTAMP_START, format="%Y%m%d%H%M")
```

```r
d <- readr::read_csv("data_HH/DE-Tha_HH_GPP_NEE.csv.gz",
                     col_types = readr::cols(TIMESTAMP_START = "c", TIMESTAMP_END = "c"))
d$time <- as.POSIXct(d$TIMESTAMP_START, format = "%Y%m%d%H%M", tz = "UTC")  # local standard time
```

## Contact

Negin Katal. For questions about a site's measurements, contact the site
PI listed in `sites.csv`. For questions about processing, contact FLUXNET (support@fluxnet.org).
