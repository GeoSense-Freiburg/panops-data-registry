#!/usr/bin/env python3
"""
Mortality metrics (deadtrees v5.1) for the FLUXNET site list without grassland (GRA) and cropland (CRO).

Same per-site-year-buffer code as the paper (site_rows from scripts/paper_v41/01_mortality_metrics_v41.py):
circular buffers 100..500 m around the tower, covered == 0 pixels masked, 2016 dropped, a site-year-buffer
with < 90 % of pixels covered set to NA. Metrics kept (paper names):
  relative_mortality_pct        (relative_disturbance)  (dw + tree loss) / (tc + tree loss) x 100
  relative_deadwood_pct         (relative_mortality)    dw / tc x 100, NA if tc < 10 %
  absolute_deadwood_pct         (absolute_mortality)    mean standing deadwood cover
  deadwood_gain_pp              (new_deadwood_gain_pp)  mean max(dw_t - dw_t-1, 0)
  relative_tree_cover_loss_pct  (relative_tree_loss_pct) mean max(tc_t-1 - tc_t, 0) / tc_t-1 x 100
Usage: python make_mortality_metrics.py [maps_dir]   (default deadtree/deadtrees_maps_v5.1)
"""
import importlib.util, os, sys
from concurrent.futures import ThreadPoolExecutor
import pandas as pd

ROOT = "/mnt/gsdata/projects/panops/panops-data-registry/data/flux"
ZDIR = sys.argv[1] if len(sys.argv) > 1 else f"{ROOT}/deadtree/deadtrees_maps_v5.1"
SITES = f"{ROOT}/Flux4Daniel/fluxnet_site_metadata_clean_with_elevation.csv"
OUT = os.environ.get("MORT_OUT", f"{ROOT}/mortality4FluxNet")
DROP_IGBP = ["GRA", "CRO"]
BUFFERS = [100, 200, 300, 400, 500]
NAMES = {"relative_disturbance": "relative_mortality_pct",
         "relative_mortality": "relative_deadwood_pct",
         "absolute_mortality": "absolute_deadwood_pct",
         "new_deadwood_gain_pp": "deadwood_gain_pp",
         "relative_tree_loss_pct": "relative_tree_cover_loss_pct",
         "tree_cover_mean_pct": "tree_cover_pct",
         "qc_covered_frac": "covered_fraction"}

spec = importlib.util.spec_from_file_location("step01", f"{ROOT}/scripts/paper_v41/01_mortality_metrics_v41.py")
step01 = importlib.util.module_from_spec(spec); spec.loader.exec_module(step01)

meta = pd.read_csv(SITES)
meta = meta[["SITE_ID", "site_name", "IGBP", "data_hub", "LOCATION_LAT", "LOCATION_LONG", "LOCATION_ELEV"]]
keep = meta[~meta.IGBP.isin(DROP_IGBP)].copy()
keep["zarr"] = keep.SITE_ID.map(lambda s: f"{ZDIR}/{s}_inference.zarr")
keep["in_maps"] = keep.zarr.map(os.path.isdir)
print(f"{len(meta)} sites, {len(keep)} after dropping {DROP_IGBP}, {keep.in_maps.sum()} with a map in {ZDIR}")

with ThreadPoolExecutor(8) as ex:
    rows = [r for rs in ex.map(step01.site_rows, keep.loc[keep.in_maps, "zarr"]) for r in rs]
d = pd.DataFrame(rows)

parts = []
for b in BUFFERS:
    x = d[["SITE_ID", "YEAR"] + [f"{k}_{b}m" for k in NAMES]].copy()
    x.columns = ["SITE_ID", "year"] + list(NAMES.values())
    x.insert(2, "buffer_m", b)
    parts.append(x)
info = keep[["SITE_ID", "site_name", "IGBP", "data_hub", "LOCATION_LAT", "LOCATION_LONG"]]
long = info.merge(pd.concat(parts), on="SITE_ID").sort_values(["SITE_ID", "buffer_m", "year"])
long.to_csv(f"{OUT}/mortality_v5.1_long.csv", index=False)

met = list(NAMES.values())[:-1]
w = long.set_index(list(info.columns) + ["year", "buffer_m"])[met].unstack("buffer_m")
w.columns = [f"{a}_{b}m" for a, b in w.columns]
w.reset_index().to_csv(f"{OUT}/mortality_v5.1_wide.csv", index=False)

yrs = long.groupby("SITE_ID").year.agg(first_year="min", last_year="max")
tc = long[long.buffer_m == 500].groupby("SITE_ID").tree_cover_pct.median().rename("median_tree_cover_500m_pct")
sites = keep.drop(columns="zarr").merge(yrs, on="SITE_ID", how="left").merge(tc, on="SITE_ID", how="left")
sites.to_csv(f"{OUT}/sites.csv", index=False)
print(f"sites {long.SITE_ID.nunique()} | rows {len(long)} | missing maps: {keep.loc[~keep.in_maps, 'SITE_ID'].tolist()}")
print(sites.IGBP.value_counts().to_string())
