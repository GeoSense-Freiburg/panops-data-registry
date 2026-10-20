#!/usr/bin/env python3
"""Builds mortality_sites_viewer.html (site map + per-site time series by buffer) from the v5.1 CSVs in this folder."""
import json, math, os
import pandas as pd

BUILD = os.path.dirname(os.path.abspath(__file__))
HERE = os.path.dirname(BUILD)
long = pd.read_csv(f"{HERE}/mortality_v5.1_long.csv")
sites = pd.read_csv(f"{HERE}/sites.csv").set_index("SITE_ID")
METRICS = ["relative_mortality_pct", "relative_deadwood_pct", "absolute_deadwood_pct",
           "deadwood_gain_pp", "relative_tree_cover_loss_pct", "tree_cover_pct"]
YEARS = list(range(2017, 2026))
BUFFERS = [100, 200, 300, 400, 500]


def clean(v):
    return None if v is None or (isinstance(v, float) and math.isnan(v)) else round(float(v), 3)


towers = []
for sid, g in long.groupby("SITE_ID"):
    s = sites.loc[sid]
    series = {m: {str(b): [clean(v) for v in g[g.buffer_m == b].set_index("year")[m].reindex(YEARS)]
                  for b in BUFFERS} for m in METRICS}
    towers.append(dict(id=sid, name=s.site_name, igbp=s.IGBP, hub=s.data_hub,
                       lat=float(s.LOCATION_LAT), lon=float(s.LOCATION_LONG),
                       y0=int(s.first_year), y1=int(s.last_year),
                       tc500=clean(s.median_tree_cover_500m_pct), series=series))
towers.sort(key=lambda t: t["id"])

data = json.dumps(dict(years=YEARS, buffers=BUFFERS, towers=towers), separators=(",", ":"))
html = open(f"{BUILD}/viewer_template.html").read().replace("/*__DATA__*/null", data)
open(f"{HERE}/mortality_sites_viewer.html", "w").write(html)
print(f"{len(towers)} towers, {len(html)/1e3:.0f} kB")
