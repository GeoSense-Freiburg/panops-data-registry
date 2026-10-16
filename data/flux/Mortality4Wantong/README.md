# Tree mortality around flux towers: ICOS/GEDI and NEON site lists

Prepared by Negin Katal ([negin.katal@geosense.uni-freiburg.de](mailto:negin.katal@geosense.uni-freiburg.de)), 16 October 2026.


This folder has five annual tree mortality metrics for the sites in your two lists
(`ICOS_GEDI_good_61sites.txt` and `NEON_gedi_good_23sites.txt`). Each metric is given for circular buffers of
100, 200, 300, 400 and 500 m around each tower, for 2017-2025. They are the same metrics and code we use in our
paper on tree mortality and ecosystem carbon fluxes.

**77 of your 84 sites are included** (56 of 61 ICOS, 21 of 23 NEON; 68 distinct towers, because nine NEON
towers are also in your ICOS list). The 7 sites without data are listed below.

**Quick look:** open [mortality_sites_viewer.html](mortality_sites_viewer.html) in a web browser for a map of all
towers and, for each tower, every metric over time with one line per buffer.

## Files

| File | What it holds |
|---|---|
| `mortality_v5.1_sites_buffers_long.csv` | one row per site, year and buffer (3,455 rows); easiest for filtering and plotting |
| `mortality_v5.1_sites_buffers_wide.csv` | one row per site and year (691 rows); each metric has one column per buffer, e.g. `absolute_deadwood_pct_300m` |
| `site_coverage_v5.1.csv` | all 84 requested sites: whether it is included, the map ID used, and for missing sites the nearest mapped site and its distance |
| [`mortality_sites_viewer.html`](mortality_sites_viewer.html) | interactive overview: map of all towers, and for the selected tower each metric over time with one line per buffer. Open in a web browser (needs internet for the map and chart libraries) |

The two data files hold the same numbers in two layouts: pick whichever suits your workflow.

## Data source

Annual 10 m maps of **tree cover** and **standing deadwood cover** from deadtrees.earth (product version 5.1:
a deep-learning model applied to Sentinel-2 time series and trained on dead-tree labels from deadtrees.earth). Each map
covers a 2 x 2 km square centred on the tower and represents one date per year in the site's mid-growing season.

For each site, year and buffer we averaged the 10 m pixels inside the buffer. Pixels without a prediction in a
year are left out. If less than 90 % of a buffer has a prediction in a year, all metrics for that site, year and
buffer are left empty. 2016 is not included because the map producer considers it the least reliable year.

## Columns

Site columns (both data files):

| Column | Meaning |
|---|---|
| `source` | which of your lists the site comes from (`ICOS_list` or `NEON_list`) |
| `site_id` | the ID from your list |
| `map_site_id` | the FLUXNET ID of the map used; differs from `site_id` only for the NEON codes (see below) |
| `igbp` | IGBP land-cover class from the FLUXNET site metadata |
| `lat`, `lon` | the coordinates from your list |
| `year` | map year (2017-2025) |
| `buffer_m` | buffer radius in m (long file only) |

Metrics. "Tree cover" and "deadwood" are the cover fractions from the maps, in % of the area.

| Column | Name in our paper | What it measures | How it is computed |
|---|---|---|---|
| `relative_mortality_pct` | relative mortality | share of the tree canopy that is dead or was lost since last year; combines standing deadwood and canopy loss | (deadwood + tree cover loss) / (tree cover + tree cover loss) x 100, capped at 100 |
| `relative_deadwood_pct` | relative deadwood | standing deadwood relative to the living canopy | deadwood / tree cover x 100, capped at 100 |
| `absolute_deadwood_pct` | absolute deadwood | standing deadwood as a share of the whole buffer | mean deadwood cover |
| `deadwood_gain_pp` | deadwood gain | new standing deadwood since the previous year | mean over pixels of the increase in deadwood cover (decreases count as 0), in percentage points |
| `relative_tree_cover_loss_pct` | relative tree cover loss | canopy lost since the previous year (e.g. felling, windthrow, fire, dead trees falling) | mean over pixels of the decrease in tree cover (increases count as 0), divided by last year's mean tree cover, x 100 |
| `tree_cover_pct` | (context) | mean tree cover in the buffer | for judging how meaningful the metrics are at a site |
| `covered_fraction` | (quality flag, long file only) | share of the buffer's pixels with a prediction that year | 1 = complete |

Relative mortality, deadwood gain and relative tree cover loss compare a year with the previous one, so their
first value is in **2018**.

## How the sites were matched

- **ICOS list:** matched by site ID.
- **NEON list:** the maps use the AmeriFlux IDs of the NEON towers, so NEON codes were matched by coordinates
  (all within 6 m), for example ABBY -> US-xAB, SJER -> US-xSJ.
- **Duplicates:** nine NEON towers are also in your ICOS list under their AmeriFlux ID: CLBJ (US-xCL),
  DELA (US-xDL), MLBS (US-xML), RMNP (US-xRM), SJER (US-xSJ), TREE (US-xTR), UKFS (US-xUK), UNDE (US-xUN) and
  YELL (US-xYE). They appear once per list with identical values.
- **US-Seg:** the map is centred on the FLUXNET tower coordinate, which is 272 m from the coordinate in your list.

## Sites not included (7)

| Site | List | Reason |
|---|---|---|
| CA-Cbo | ICOS | no map for this site |
| US-KLS | ICOS | no map for this site |
| US-Me2 | ICOS | no map; nearest mapped tower US-Me7 is 254 m away |
| US-UMB | ICOS | no map; nearest mapped tower US-UMd is 1.3 km away |
| US-Var | ICOS | no map; nearest mapped tower US-Ton is 2.4 km away |
| GUAN | NEON | no map; nearest mapped tower PR-xLA is 23 km away |
| HARV | NEON | no map; nearest mapped tower US-Ha1 is 137 m away (a different tower) |

We did not substitute neighbouring towers. If a nearby tower is good enough for your purpose (for example
US-Ha1 for HARV, or US-UMd for US-UMB), let us know and we can add it.

## Missing values and caveats

- **Low tree cover.** Relative deadwood and relative tree cover loss are empty when tree cover (this year's, or
  last year's for the loss) is below 10 %, because a ratio to a very small canopy is meaningless. 20 sites have a
  median tree cover below 10 % in the 500 m buffer: US-xJR, US-Seg, US-xCP, US-xNQ, US-Vcm, US-Rws, US-Whs,
  US-xWD, US-xAE, US-Rwf, US-xDC, CA-EM1, US-Kon, CA-EM2, US-Wjs, US-SRM, US-Rls, US-xSR, US-Tw5 and US-Rms.
  Another 14 sites are between 10 and 30 %.
- **Open ecosystems are noisy.** Our paper uses only sites with at least 30 % tree cover. At grasslands,
  shrublands and other open sites, deadwood values of a few percent are mostly model noise rather than real tree
  death. Filter on `tree_cover_pct` if you need robust values.
- **2025 gaps.** US-Whs and US-Seg have no 2025 map. US-Wjs 2025 is empty at all buffers (incomplete map).
- **Small buffers vary more.** A 100 m buffer holds about 310 pixels, a 500 m buffer about 7,850. Year-to-year
  values at 100 m are more sensitive to single trees and to map noise.
- **Annual changes are less certain than levels.** Deadwood gain and tree cover loss come from the difference
  between two annual maps, so small values (below about 1 percentage point) are within the map uncertainty.


