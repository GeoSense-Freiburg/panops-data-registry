# Trait extraction for FLUXNET v1.3 sites with SWC

All Lusk et al. 2025 CWM trait maps (Shrub_Tree_Grass, 1 km; analysis-ready,
experimental, hydraulic) extracted per 1 km cell around each tower, so the user
can judge which traits are reliable at which sites.

Source: `data/trait_maps/lusk_et-al_2025/cwms/Shrub_Tree_Grass/1km/`

## Steps

```bash
python3 -I scripts/01_site_coordinates.py           # data/site_coordinates.csv
$RSCRIPT scripts/02_extract_traits_per_cell.R       # per-cell + site summary
$RSCRIPT scripts/03_trait_diagnostics.R             # per-trait diagnostics + wide tables
```

## Sites

494 sites in `data/fluxnet_v1.3_sites_with_swc.csv`; 473 have coordinates
(440 from the BIF file in the raw v1.3 zip, 33 from older metadata tables).
21 have no coordinates locally (no or empty zip): `data/sites_without_coordinates.txt`.
15 sites have no trait value in the tower cell (water / urban / masked cell;
mostly some valid neighbours, see `n_cells_valid`).

## Cells

The 3 x 3 block of 1 km cells centred on the tower cell (`drow`, `dcol` = -1..1,
`is_tower_cell`, `dist_cell_centre_m` = tower to cell centre). This covers every
cell a 500 m buffer can touch.

## Bands

- analysis-ready / experimental: `mean` (CWM), `cov` (coefficient of variation,
  fraction, not %), `aoa` (area-of-applicability mask, 1 = outside AOA, i.e.
  extrapolation; 1 occurs at GL-ZaF, US-UTB, US-YK1/2, CN-You).
- hydraulic: `mean` only (no COV / AOA layer exists).

## Outputs (`outputs/`)

| File | Contents |
|---|---|
| `traits_per_cell_long.csv` | site x trait x cell: mean, cov, aoa |
| `traits_site_summary.csv` | site x trait: tower-cell mean/cov/aoa; 3x3 mean, sd, spatial CV, mean/max COV, AOA fraction |
| `trait_diagnostics.csv` | per trait across sites: distinct values, share of the most common value, negatives, COV summary, sites outside AOA, flags |
| `traits_wide_tower_{mean,cov,aoa}.csv` | site x trait tables of the tower cell |
| `trait_list.csv` | trait id, name, group, file |

## Known problems in the maps

- Collapsed maps (few values repeated over many sites): Seed mass X26 (5 values,
  68 % of sites at -0.30, negative), Plant height veg. X3106 (16 values, 53 % one
  value, some negative), Plant height gen. X3107 (13 values), Chromosome cDNA
  content X224 (17 values). Treat as unusable.
- terra 1.7.83 returns wrong values when many cells of these scaled Int16
  3-band rasters are read at once (`r[cells]`, `extract(r, xy)`); 02 reads each
  site window with `crop()` and was checked against single-cell reads.
  Polygon `extract(fun = mean)` as in `trait_biogeography_test/` is unaffected.
