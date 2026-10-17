#!/usr/bin/env python3
"""Builds mortality_sites_viewer.html (site map + per-site time series by buffer) from the v5.1 CSVs in this folder."""
import json, math, os
import pandas as pd

BUILD = os.path.dirname(os.path.abspath(__file__))
HERE = os.path.dirname(BUILD)
long = pd.read_csv(f"{HERE}/mortality_v5.1_sites_buffers_long.csv")
cov = pd.read_csv(f"{HERE}/site_coverage_v5.1.csv")
METRICS = ["relative_mortality_pct", "relative_deadwood_pct", "absolute_deadwood_pct",
           "deadwood_gain_pp", "relative_tree_cover_loss_pct", "tree_cover_pct"]
YEARS = list(range(2017, 2026))
BUFFERS = [100, 200, 300, 400, 500]


def clean(v):
    return None if v is None or (isinstance(v, float) and math.isnan(v)) else round(float(v), 3)


towers = []
for mid, g in long.groupby("map_site_id"):
    names = g[["source", "site_id"]].drop_duplicates()
    series = {}
    for m in METRICS:
        series[m] = {}
        for b in BUFFERS:
            s = g[g.buffer_m == b].drop_duplicates("year").set_index("year")[m].reindex(YEARS)
            series[m][str(b)] = [clean(v) for v in s]
    tc500 = g[g.buffer_m == 500].drop_duplicates("year").tree_cover_pct.median()
    first = g.iloc[0]
    towers.append(dict(
        id=mid, igbp=first.igbp, lat=float(first.lat), lon=float(first.lon),
        icos=[r.site_id for r in names.itertuples() if r.source == "ICOS_list"],
        neon=[r.site_id for r in names.itertuples() if r.source == "NEON_list"],
        tc500=clean(tc500), series=series))
towers.sort(key=lambda t: t["id"])
missing = [dict(id=r.site_id, list=r.source.replace("_list", ""), lat=float(r.lat), lon=float(r.lon), note=r.note)
           for r in cov[~cov.included].itertuples()]

data = json.dumps(dict(years=YEARS, buffers=BUFFERS, towers=towers, missing=missing), separators=(",", ":"))
html = open(f"{BUILD}/viewer_template.html").read().replace("/*__DATA__*/null", data)
open(f"{HERE}/mortality_sites_viewer.html", "w").write(html)
print(f"{len(towers)} towers, {len(missing)} missing, {len(html)/1e3:.0f} kB")
