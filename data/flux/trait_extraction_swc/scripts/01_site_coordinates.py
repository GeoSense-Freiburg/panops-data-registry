# 01_site_coordinates.py
# Tower coordinates for the FLUXNET v1.3 sites with SWC, read from the BIF file
# inside each raw v1.3 zip (Flux4Daniel/flux_raw_zip). Some zips are empty
# (0 bytes); those sites fall back to the existing metadata tables.

import csv
import glob
import os
import re
import zipfile

base_dir = "/mnt/gsdata/projects/panops/panops-data-registry/data/flux"
proj_dir = os.path.join(base_dir, "trait_extraction_swc")
zip_dir = os.path.join(base_dir, "Flux4Daniel/flux_raw_zip")

sites = [r["site_id"] for r in csv.DictReader(open(os.path.join(proj_dir, "data/fluxnet_v1.3_sites_with_swc.csv")))]

zips = {}
for z in sorted(glob.glob(os.path.join(zip_dir, "*.zip"))):
    m = re.match(r"^[A-Za-z]+_([A-Za-z]{2}-[A-Za-z0-9]+)_", os.path.basename(z))
    if m:
        zips.setdefault(m.group(1), z)


def from_bif(site):
    z = zips.get(site)
    if z is None or os.path.getsize(z) == 0:
        return None
    try:
        with zipfile.ZipFile(z) as zf:
            bif = [n for n in zf.namelist() if "_BIF_" in n]
            if not bif:
                return None
            vals = {}
            for line in zf.read(bif[0]).decode("utf-8", "replace").splitlines():
                p = line.split(",")
                if len(p) >= 5 and p[3] in ("LOCATION_LAT", "LOCATION_LONG", "IGBP") and p[3] not in vals:
                    vals[p[3]] = p[4]
            if "LOCATION_LAT" in vals and "LOCATION_LONG" in vals:
                return float(vals["LOCATION_LAT"]), float(vals["LOCATION_LONG"]), vals.get("IGBP", ""), "BIF_v1.3"
    except zipfile.BadZipFile:
        return None
    return None


# fallback tables: (path, lat column, lon column)
fallbacks = [
    ("combined_site_metadata.csv", "LOCATION_LAT", "LOCATION_LONG"),
    ("fluxnet_2017_2025_V02/fluxnet_site_metadata_clean.csv", "LOCATION_LAT", "LOCATION_LONG"),
    ("trait_biogeography_test/data/site_table.csv", "lat", "lon"),
    ("deadtree/deadtrees_maps_v5.1/site_table.csv", "lat", "lon"),
]
fb = {}
for path, la, lo in fallbacks:
    for r in csv.DictReader(open(os.path.join(base_dir, path))):
        sid = r.get("SITE_ID") or r.get("site_id")
        if sid not in fb and r.get(la) and r.get(lo):
            fb[sid] = (float(r[la]), float(r[lo]), r.get("IGBP") or r.get("igbp", ""), path)

out = []
for s in sites:
    rec = from_bif(s) or fb.get(s)
    if rec is None:
        print("no coordinates:", s)
        continue
    out.append({"SITE_ID": s, "lat": rec[0], "lon": rec[1], "IGBP": rec[2], "coord_source": rec[3]})

with open(os.path.join(proj_dir, "data/site_coordinates.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=["SITE_ID", "lat", "lon", "IGBP", "coord_source"])
    w.writeheader()
    w.writerows(out)

n_bif = sum(r["coord_source"] == "BIF_v1.3" for r in out)
print(f"{len(sites)} sites, {len(out)} with coordinates ({n_bif} from BIF, {len(out) - n_bif} from fallback tables)")
