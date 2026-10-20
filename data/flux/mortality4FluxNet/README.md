# Tree mortality metrics (deadtrees v5.1) for FLUXNET sites, without grassland and cropland

Source: deadtrees.earth maps **v5.1** (model v5001, 10 m; 717 sites, sha256 11c18577...), site list
`Flux4Daniel/fluxnet_site_metadata_clean_with_elevation.csv` (717 sites). Sites with IGBP **GRA** (130) or
**CRO** (130) are removed, leaving **457 sites**; all of them have a map. Other IGBP classes are kept,
including CVM (cropland/natural vegetation mosaic), WET, OSH, SAV and BSV.

Values are means over a circular buffer around the tower (radius 100, 200, 300, 400, 500 m), one value per year,
computed with the same code as our paper (`scripts/paper_v41/01_mortality_metrics_v41.py`, function `site_rows`).
Years 2017-2025 (2016 is left out because the data producer considers it unreliable). Rebuild with
`python make_mortality_metrics.py`.

## Files

| File | Content |
|---|---|
| `sites.csv` | the 457 sites: name, IGBP, data hub, coordinates, first/last map year, median tree cover at 500 m |
| `mortality_v5.1_long.csv` | one row per site x year x buffer (20,445 rows) |
| `mortality_v5.1_wide.csv` | one row per site x year, buffers as columns (`..._100m` ... `..._500m`) |
| `make_mortality_metrics.py` | the script |
| [`mortality_sites_viewer.html`](mortality_sites_viewer.html) | interactive overview: map of all 457 towers with a land-cover filter, and for the selected tower each metric over time with one line per buffer. Open in a web browser (needs internet for the map and chart libraries); rebuild with `python _build/make_html.py` |

## Metrics (all in %, or percentage points)

| Column | Name in the paper | Definition |
|---|---|---|
| `relative_mortality_pct` | relative mortality | (deadwood + tree cover loss) / (tree cover + tree cover loss) x 100, capped at 100 |
| `relative_deadwood_pct` | relative deadwood | standing deadwood cover / tree cover x 100, capped at 100; NA if tree cover < 10 % |
| `absolute_deadwood_pct` | absolute deadwood | mean standing deadwood cover in the buffer (% of area) |
| `deadwood_gain_pp` | deadwood gain | mean over pixels of max(deadwood_t - deadwood_t-1, 0), in percentage points |
| `relative_tree_cover_loss_pct` | relative tree cover loss | mean over pixels of max(tc_t-1 - tc_t, 0) / mean tc_t-1 x 100; NA if previous-year tree cover < 10 % |
| `tree_cover_pct` | (context) | mean tree cover in the buffer (%) |
| `covered_fraction` | (QC, long file only) | share of buffer pixels with a real prediction that year |

## Gaps (NA)

- Deadwood gain, relative tree cover loss and relative mortality need the previous year, so they start in **2018**.
- **24 sites have no 2025 map** (they end in 2024).
- **2025 partly missing** (buffer < 90 % covered, all metrics NA): BR-Ma3, JP-Khw, MX-PMm, SD-Dem, US-Wjs
  (all buffers) and CD-Ygb (200-500 m).
- **Relative deadwood** and **relative tree cover loss** are NA where tree cover < 10 %.
- **Low tree cover:** 193 of the 457 sites have a median tree cover at 500 m below 30 %
  (107 below 10 %). Our paper used only sites with >= 30 %. At open sites the metrics are small and noisy
  (standing deadwood at ~1-3 % is mostly model noise), so filter on `tree_cover_pct` as needed.
- DE-Hte and ES-Yst were moved in the new site list and re-cut at the new tower position (398 m and 709 m),
  so their values differ from v5.
