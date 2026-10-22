# Test: do static CWM plant traits explain EFP variation beyond climate?

Motivating question (Reichstein et al. 2014, PNAS): do plant traits matter for
spatial variation in ecosystem functional properties (EFPs), in addition to
and independently of climate?

## Pipeline

| Step | Script | Output |
|---|---|---|
| Site table (EFP + metadata, QC'd) | `00_build_site_table.R` | `data/site_table.csv` (517 sites) |
| Climate normals from raw FLUXNET zips | `01_extract_climate.R` | `data/climate_by_site.csv` |
| CWM trait extraction (500m buffer) | `02_extract_traits.R`, `02b_apply_trait_codec.R` | `data/traits_by_site.csv`, `data/trait_metadata.csv` |
| Merge + complete-case filter | `03_build_clean_dataset.R` | `data/clean_dataset.csv` (413 sites) |
| PCA + variance partitioning + bootstrap | `04_analysis.R` | `results/*.csv`, `results/analysis_results.rds` |

### Data sources
- **EFPs**: `Flux4Daniel/EFP_outputs/ALL_SITES_EFP.csv` — 694 sites, already a
  multi-year composite per site (not a site-year panel). `status=="ok"`,
  `nyears>=2` required.
- **Climate**: extracted directly from the yearly FLUXMET table bundled in
  each site's raw FLUXNET zip (`Flux4Daniel/flux_raw_zip`) — `TA_F`, `VPD_F`,
  `P_F`, `SW_IN_F`, `PA_F`, `WS_F`, averaged across available years per site,
  plus a derived De Martonne aridity index. All 517/517 candidate sites
  extracted successfully.
- **Traits**: Lusk et al. 2025 CWM maps (`Shrub_Tree_Grass/1km`), analysis-ready
  (17 traits, TRY-ID coded, renamed per the codec supplied) + hydraulic (6
  traits: gsmax, P12, P50, P88, rdmax, WUE_trait). Extracted as a 500m-buffer
  mean around each tower (rasters are EASE-Grid 2.0, equal-area).
- **IGBP**: from site metadata; CRO/CVM/WAT/URB/BSV/SNO excluded (trait maps
  don't cover cropland/non-vegetated classes).

### Sample size funnel
694 (EFP) → 517 (+ metadata + raw zip + QC gates + natural-vegetation IGBP) →
**413 complete-case sites** (traits + climate + EFPs all present).

## Caveats (read before trusting the numbers)

1. **This dataset is already cross-sectional** — one EFP value per site. There
   is no within-site (interannual) variance to decompose, so the "ICC ceiling"
   step from the original recommendation doesn't apply here; every EFP here is
   already the between-site quantity the Reichstein hypothesis is about.
2. **Area-of-applicability (AOA) coverage is near the global baseline (~0.5%)
   for all analysis-ready traits** — i.e. almost no land on Earth (not just
   flux towers) falls inside this CWM model's training domain, per the
   Meyer/Pebesma AOA method embedded in the rasters. This is a real property
   of the trait product (sparse sPlot/GBIF training coverage extrapolated
   globally), not an extraction bug — confirmed by checking the global
   raster: only 601,835 of 128,203,057 land cells are inside AOA for SSD.
   Point trait values at towers carry real extrapolation uncertainty; this
   analysis does not filter on it because doing so would drop ~99% of sites.
3. **18 sites had fully missing trait rasters** (CH-Crs, CH-Dav, CH-Oe1/3,
   CN-BeO, CZ-wet, ES-FtD, FR-Tou, JP-Fmt/Nuf/Om2, NZ-ScF, US-CGG/Cms/DPW/EKN/LA3/WPT)
   — NoData in the 1km product at those exact pixels, disproportionately
   wetland (8/18).
4. **Trait-block PCA labels are TRY-ID based**, cross-validated against the
   codec supplied; hydraulic trait units (gsmax, P12/50/88, rdmax, WUE_trait)
   were not in that codec and are my best-effort standard units — worth
   double-checking against the Lusk et al. 2025 documentation before
   publication use.

## Results

### PCA reduction
- **Climate**: 4 PCs explain 89% of variance. PC1 (loadings: aridity_index
  +0.31, P_mean +0.20, TA −0.51, SW_IN −0.54, VPD −0.56) is a warm/dry/sunny
  vs. cool/wet gradient — a standard aridity–energy axis.
- **Traits**: 5 PCs explain 83% of variance (23 traits retained; no pair
  exceeded |r|=0.95, so nothing was dropped as redundant). PC1 separates
  thick/dense/high-C:N/low-SLA traits from high-SLA/high-leaf-N/high-gsmax
  traits — this is a recognizable **leaf economics spectrum** axis (Wright et
  al. 2004; Díaz et al. 2016), a good external sanity check that the trait
  PCA recovered real biology rather than noise.

### Variance partitioning: climate vs. traits (adjusted R², vegan::varpart)

| EFP | unique climate | unique traits | shared | residual | unique-trait 95% bootstrap CI |
|---|---|---|---|---|---|
| GPPsat | ~0.000 | **0.365** | 0.103 | 0.532 | [0.296, 0.441] |
| NEPmax | 0.011 | **0.389** | 0.081 | 0.519 | [0.312, 0.465] |
| ETmax  | 0.043 | **0.221** | 0.060 | 0.677 | [0.161, 0.308] |
| uWUE   | 0.005 | 0.067 | 0.014 | 0.914 | [0.036, 0.134] |
| WUE    | 0.0005 | 0.059 | 0.004 | 0.937 | [0.029, 0.189] |

For every EFP, the unique-trait fraction's bootstrap CI excludes zero, and is
5–100x larger than the unique-climate fraction. **Traits carry almost all of
the explainable cross-site signal here; climate alone (net of its overlap
with traits) explains almost nothing on its own.**

### Adding IGBP (land-cover class) as a third block

| EFP | unique climate | unique traits (beyond climate+IGBP) | unique IGBP | interpretation |
|---|---|---|---|---|
| GPPsat | 0.008 | **0.150** | 0.104 | traits keep a real, biome-independent signal |
| NEPmax | 0.003 | **0.158** | 0.096 | same |
| ETmax  | 0.050 | **0.119** | 0.059 | same, weaker |
| uWUE   | −0.002 | 0.016 | **0.150** | trait signal mostly *was* biome identity |
| WUE    | −0.004 | 0.015 | **0.118** | same |

This is the sharper test. For the **carbon EFPs (GPPsat, NEPmax)**, traits
retain a substantial unique effect even after controlling for both climate
and coarse biome class — direct, fairly strong support for the Reichstein
paragraph. For **ETmax**, the effect survives but is weaker. For the
**water-efficiency EFPs (uWUE, WUE)**, the apparent trait effect in the
2-block model was mostly a proxy for biome identity — once IGBP is in the
model, unique trait R² collapses to ~1.5%, and IGBP itself carries the real
unique signal (~12–15%).

## Tension with the existing RF/SHAP pipeline worth flagging

Your production panel-RF pipeline found traits carry 56–59% SHAP share for
water EFPs (ETmax) — but this partition finds traits' *unique, biome-independent*
contribution to water-efficiency EFPs (uWUE/WUE) is small once IGBP is
controlled. Plausible reconciliation: RF/SHAP can use continuous trait values
as an implicit biome classifier (since traits and IGBP are highly collinear)
without that showing up as "IGBP effect" — SHAP would then credit traits with
variance that's actually just re-deriving biome identity, whereas
`varpart`'s partial-R² construction explicitly strips that out. Worth
resolving before this becomes a manuscript claim.

## Bottom line

Yes — this dataset can test the Reichstein 2014 hypothesis, and on this
first pass, static CWM traits do explain EFP variation across 413 sites
independently of climate, especially for carbon EFPs (GPPsat, NEPmax), even
after controlling for coarse biome class. The signal is weaker for ETmax and,
once biome is properly controlled, essentially absent for the water-efficiency
EFPs (uWUE, WUE) — where it initially looked strong only because traits and
IGBP are collinear.
