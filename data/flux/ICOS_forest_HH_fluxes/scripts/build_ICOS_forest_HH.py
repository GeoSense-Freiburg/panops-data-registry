"""Build the ICOS forest half-hourly GPP/NEE subset from FLUXNET (ONEFlux v1.3) zips.

Source: FLUXNET Shuttle download of 2026-05-12
        (../fluxnet_2017_2025_V02, snapshot fluxnet_shuttle_snapshot_20260512T115423.csv).
Selection: product_source_network == ICOS (plus IT-SR2, an ICOS station processed under FLX)
           and IGBP in {ENF, EBF, DBF, DNF, MF}.

Run from the ICOS_forest_HH_fluxes/ folder:
    python3 scripts/build_ICOS_forest_HH.py
"""
import os
import zipfile
from multiprocessing import Pool

import numpy as np
import pandas as pd

OUT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC = os.path.abspath(os.path.join(OUT, "..", "fluxnet_2017_2025_V02"))
FIXED = os.path.join(OUT, "raw_zip_fixed")  # re-downloaded zips replacing corrupt ones
SNAPSHOT = os.path.join(SRC, "fluxnet_shuttle_snapshot_20260512T115423.csv")
FOREST = ["ENF", "EBF", "DBF", "DNF", "MF"]
EXTRA_ICOS_STATIONS = ["IT-SR2"]

# Fixed column set: every output file has the same header; absent variables are NA.
COLS = ["TIMESTAMP_START", "TIMESTAMP_END", "NIGHT"]
for m in ["VUT", "CUT"]:
    COLS += [f"NEE_{m}_REF", f"NEE_{m}_REF_QC", f"NEE_{m}_REF_RANDUNC",
             f"NEE_{m}_REF_JOINTUNC", f"NEE_{m}_05", f"NEE_{m}_95"]
for m in ["VUT", "CUT"]:
    for p in ["NT", "DT"]:
        COLS += [f"GPP_{p}_{m}_REF", f"GPP_{p}_{m}_05", f"GPP_{p}_{m}_95"]
for m in ["VUT", "CUT"]:
    for p in ["NT", "DT"]:
        COLS += [f"RECO_{p}_{m}_REF"]
COLS += ["TA_F", "TA_F_QC", "SW_IN_F", "SW_IN_F_QC", "VPD_F", "VPD_F_QC", "P_F", "USTAR"]


def select_sites():
    s = pd.read_csv(SNAPSHOT)
    sel = s[((s.product_source_network == "ICOS") | s.site_id.isin(EXTRA_ICOS_STATIONS))
            & s.igbp.isin(FOREST)].copy()
    return sel.sort_values("site_id").reset_index(drop=True)


def zip_path(name):
    p = os.path.join(FIXED, name)
    return p if os.path.exists(p) else os.path.join(SRC, name)


def process(row):
    site, zname = row["site_id"], row["fluxnet_product_name"]
    z = zipfile.ZipFile(zip_path(zname))
    names = z.namelist()
    hh = [n for n in names if "_FLUXMET_HH_" in n][0]
    header = z.open(hh).readline().decode().strip().split(",")
    use = [c for c in COLS if c in header]
    df = pd.read_csv(z.open(hh), usecols=use, na_values=[-9999, "-9999"],
                     dtype={"TIMESTAMP_START": str, "TIMESTAMP_END": str})
    for c in COLS:
        if c not in df:
            df[c] = np.nan
    df = df[COLS]
    df.insert(0, "SITE_ID", site)
    os.makedirs(os.path.join(OUT, "data_HH"), exist_ok=True)
    df.to_csv(os.path.join(OUT, "data_HH", f"{site}_HH_GPP_NEE.csv.gz"), index=False, na_rep="NA")

    # metadata (BIF site info + HH variable info) and the zip's own README/licence text
    md = os.path.join(OUT, "metadata", site)
    os.makedirs(md, exist_ok=True)
    for n in names:
        if "_BIF_" in n or "_BIFVARINFO_HH_" in n:
            with open(os.path.join(md, n), "wb") as fh:
                fh.write(z.read(n))

    # time step + yearly coverage
    t = pd.to_datetime(df.TIMESTAMP_START, format="%Y%m%d%H%M")
    step = int(t.diff().dt.total_seconds().div(60).mode().iloc[0])
    nee = "NEE_VUT_REF" if df.NEE_VUT_REF.notna().any() else "NEE_CUT_REF"
    qc = df[f"{nee}_QC"]
    yr = t.dt.year
    cov = pd.DataFrame({
        "SITE_ID": site, "YEAR": yr,
        "measured": (qc == 0), "good": qc.isin([0, 1]),
        "nee_present": df[nee].notna(), "gpp_nt": df.GPP_NT_VUT_REF.notna() | df.GPP_NT_CUT_REF.notna(),
        "gpp_dt": df.GPP_DT_VUT_REF.notna() | df.GPP_DT_CUT_REF.notna(),
    }).groupby(["SITE_ID", "YEAR"]).agg(
        N_RECORDS=("measured", "size"),
        FRAC_NEE_MEASURED_QC0=("measured", "mean"),
        FRAC_NEE_MEAS_OR_GOODGAPFILL_QC01=("good", "mean"),
        FRAC_NEE_NOT_MISSING=("nee_present", "mean"),
        FRAC_GPP_NT_NOT_MISSING=("gpp_nt", "mean"),
        FRAC_GPP_DT_NOT_MISSING=("gpp_dt", "mean"),
    ).reset_index()
    cov["NEE_VARIANT_USED"] = nee

    info = {
        "SITE_ID": site, "TIME_STEP_MIN": step,
        "FIRST_TIMESTAMP": df.TIMESTAMP_START.iloc[0], "LAST_TIMESTAMP": df.TIMESTAMP_END.iloc[-1],
        "HAS_VUT": df.NEE_VUT_REF.notna().any(), "HAS_CUT": df.NEE_CUT_REF.notna().any(),
        "N_RECORDS": len(df),
    }
    print(site, step, len(df), flush=True)
    return info, cov


def bif_value(site, var):
    md = os.path.join(OUT, "metadata", site)
    f = [n for n in os.listdir(md) if "_BIF_" in n and "VARINFO" not in n][0]
    b = pd.read_csv(os.path.join(md, f), dtype=str)
    v = b.loc[b.VARIABLE == var, "DATAVALUE"]
    return v.iloc[0] if len(v) else ""


if __name__ == "__main__":
    sites = select_sites()
    with Pool(8) as pool:
        res = pool.map(process, [r for _, r in sites.iterrows()])
    info = pd.DataFrame([r[0] for r in res])
    cov = pd.concat([r[1] for r in res])
    cov.to_csv(os.path.join(OUT, "data_coverage_by_site_year.csv"), index=False, float_format="%.3f")

    out = sites.rename(columns={
        "site_id": "SITE_ID", "site_name": "SITE_NAME", "igbp": "IGBP",
        "location_lat": "LAT", "location_long": "LON", "network": "NETWORK_AFFILIATION",
        "product_source_network": "PRODUCT_SOURCE_NETWORK", "first_year": "FIRST_YEAR",
        "last_year": "LAST_YEAR", "team_member_name": "TEAM_MEMBERS",
        "team_member_role": "TEAM_ROLES", "team_member_email": "TEAM_EMAILS",
        "fluxnet_product_name": "SOURCE_ZIP", "product_citation": "CITATION",
        "product_id": "PID", "oneflux_code_version": "ONEFLUX_VERSION",
    })
    out = out.merge(info, on="SITE_ID")
    out["ELEV_M"] = [bif_value(s, "LOCATION_ELEV") for s in out.SITE_ID]
    out["COUNTRY"] = out.SITE_ID.str[:2]
    out = out[["SITE_ID", "SITE_NAME", "COUNTRY", "IGBP", "LAT", "LON", "ELEV_M",
               "FIRST_YEAR", "LAST_YEAR", "FIRST_TIMESTAMP", "LAST_TIMESTAMP", "TIME_STEP_MIN",
               "N_RECORDS", "HAS_VUT", "HAS_CUT", "PRODUCT_SOURCE_NETWORK", "NETWORK_AFFILIATION",
               "ONEFLUX_VERSION", "SOURCE_ZIP", "PID", "CITATION",
               "TEAM_MEMBERS", "TEAM_ROLES", "TEAM_EMAILS"]]
    out.to_csv(os.path.join(OUT, "sites.csv"), index=False)

    with open(os.path.join(OUT, "citations_per_site.txt"), "w") as fh:
        for _, r in out.iterrows():
            fh.write(f"[{r.SITE_ID}] {r.CITATION}\n\n")

    # licence text (identical in every zip)
    z = zipfile.ZipFile(zip_path(sites.fluxnet_product_name.iloc[0]))
    with open(os.path.join(OUT, "DATA_POLICY_LICENSE_AND_INSTRUCTIONS.txt"), "wb") as fh:
        fh.write(z.read("DATA_POLICY_LICENSE_AND_INSTRUCTIONS.txt"))
    with open(os.path.join(OUT, "FLUXNET_PRODUCT_README.txt"), "wb") as fh:
        fh.write(z.read("README.txt"))
    print("done:", len(out), "sites")
