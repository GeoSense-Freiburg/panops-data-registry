#!/usr/bin/env python3
"""
Paper dataset (deadtrees v4.1), step 01 - mortality metrics per site-year and buffer.

Same formulas as the V10 pipeline (scripts/model/run_deadwood_v2-2_metrics.R, renamed as in
step_10, relative disturbance as in step_11), applied to deadtrees_maps_v4.1:
  tree_cover_mean_pct      mean tree cover in the buffer (%)                  [for the tc>=30 filter]
  absolute_mortality       mean standing deadwood cover (%)
  relative_mortality       deadwood / tree cover x 100, NA if tree cover < 10 %, capped at 100
  new_deadwood_gain_pp     mean of max(dw_t - dw_t-1, 0)                      (deadwood gain)
  new_deadwood_gain_pp_thresh   same, only pixels with a gain >= 20 pp
  relative_tree_loss_pct   mean of max(tc_t-1 - tc_t, 0) / tc_t-1 x 100, NA if tc_t-1 < 10 %
  relative_tree_loss_pct_thresh same, only pixels with a loss >= 20 pp
  relative_disturbance     (dw + tree_loss) / (tc + tree_loss) x 100, capped at 100
Changes versus V10, by decision (memory: paper_dataset_v41):
  - source is v4.1 (fixed prediction dates and cube corners), not v2-2
  - 2016 is dropped (data creator: unreliable), so 2017 is the first year and year-over-year
    metrics start in 2018
  - pixels with covered == 0 in a year are excluded; a delta uses pixels covered in both years;
    a site-year-buffer with < 90 % of its pixels covered is set to NA (only the 2025 slice of
    CD-Ygb, JP-Khw, MX-PMm, US-Wjs is affected in v4.1)
Buffers are circles around the cube centre (= the tower), radius 100..500 m, as in V10.
Output: derived_tables/paper_v41/mortality_v41_multibuffer.csv
"""
import glob, os
from concurrent.futures import ThreadPoolExecutor
import numpy as np, pandas as pd, zarr

ROOT = os.environ.get("PAPER_ROOT", ".")
# map version (env DEADTREE_VERSION): v4.1 (default, the paper) or v5 (delivered 2026-10-06, parallel paper_v5 set)
VERSION = os.environ.get("DEADTREE_VERSION", "v4.1")
SUB, TAG = {"v4.1": ("paper_v41", "v41"), "v5": ("paper_v5", "v5")}[VERSION]
ZDIR = f"{ROOT}/deadtree/deadtrees_maps_{VERSION}"
OUT = f"{ROOT}/derived_tables/{SUB}/mortality_{TAG}_multibuffer.csv"
os.makedirs(os.path.dirname(OUT), exist_ok=True)
BUFFERS = [100, 200, 300, 400, 500]
TC_MIN, THRESH_PP, MIN_COVER, FIRST_YEAR = 10.0, 20.0, 0.90, 2017
SCALE = 100.0 / 255.0


def mean_masked(a, m):
    return float(a[m].mean()) if m.any() else np.nan


def site_rows(path):
    sid = os.path.basename(path).replace("_inference.zarr", "")
    z = zarr.open(path, mode="r")
    years = [int(str(t)) for t in z["time"][:]]
    keep = [i for i, y in enumerate(years) if y >= FIRST_YEAR]
    tc = z["forest"][:].astype(np.float32) * SCALE
    dw = z["deadwood"][:].astype(np.float32) * SCALE
    cov = z["covered"][:].astype(bool)
    x, y = np.asarray(z["x"][:], float), np.asarray(z["y"][:], float)
    x0, y0 = (x.min() + x.max()) / 2, (y.min() + y.max()) / 2
    d2 = (y[:, None] - y0) ** 2 + (x[None, :] - x0) ** 2
    masks = {r: d2 <= r * r for r in BUFFERS}
    rows = []
    for k, i in enumerate(keep):
        row = {"SITE_ID": sid, "YEAR": years[i]}
        prev = keep[k - 1] if k > 0 else None           # previous kept year (2016 is never used)
        for r, bm in masks.items():
            s = f"_{r}m"
            m_now = bm & cov[i]
            frac = m_now.sum() / bm.sum()
            row["qc_covered_frac" + s] = round(float(frac), 4)
            ok = frac >= MIN_COVER
            tcm, dwm = (mean_masked(tc[i], m_now), mean_masked(dw[i], m_now)) if ok else (np.nan, np.nan)
            row["tree_cover_mean_pct" + s] = tcm
            row["absolute_mortality" + s] = dwm
            row["relative_mortality" + s] = min(dwm / tcm * 100, 100.0) if ok and tcm >= TC_MIN else np.nan
            gain = gain_t = rtl = rtl_t = rdist = np.nan
            if prev is not None and years[i] - years[prev] == 1:
                m_both = bm & cov[i] & cov[prev]
                if ok and m_both.sum() / bm.sum() >= MIN_COVER:
                    ddw = dw[i] - dw[prev]; dtc = tc[prev] - tc[i]
                    gain = mean_masked(np.maximum(ddw, 0), m_both)
                    gain_t = mean_masked(np.where(ddw >= THRESH_PP, ddw, 0), m_both)
                    loss = mean_masked(np.maximum(dtc, 0), m_both)
                    loss_t = mean_masked(np.where(dtc >= THRESH_PP, dtc, 0), m_both)
                    tc_prev = mean_masked(tc[prev], m_both)
                    if tc_prev >= TC_MIN:
                        rtl, rtl_t = loss / tc_prev * 100, loss_t / tc_prev * 100
                    den = tcm + loss
                    rdist = min((dwm + loss) / den * 100, 100.0) if den > 0 else np.nan
            row["new_deadwood_gain_pp" + s] = gain
            row["new_deadwood_gain_pp_thresh" + s] = gain_t
            row["relative_tree_loss_pct" + s] = rtl
            row["relative_tree_loss_pct_thresh" + s] = rtl_t
            row["relative_disturbance" + s] = rdist
        rows.append(row)
    return rows


def main():
    paths = sorted(glob.glob(f"{ZDIR}/*_inference.zarr"))
    with ThreadPoolExecutor(8) as ex:
        allrows = [r for rs in ex.map(site_rows, paths) for r in rs]
    df = pd.DataFrame(allrows).sort_values(["SITE_ID", "YEAR"])
    df.to_csv(OUT, index=False)
    print(f"sites {df.SITE_ID.nunique()} | site-years {len(df)} | years {df.YEAR.min()}-{df.YEAR.max()}")
    for c in ["absolute_mortality_500m", "relative_mortality_500m", "new_deadwood_gain_pp_500m",
              "relative_tree_loss_pct_500m", "relative_disturbance_500m", "tree_cover_mean_pct_500m"]:
        v = df[c]
        print(f"  {c:32s} non-NA {v.notna().sum():5d}  median {v.median():7.2f}  p95 {v.quantile(.95):7.2f}")
    print("site-years with <90 % buffer coverage at 500 m:", int((df['qc_covered_frac_500m'] < MIN_COVER).sum()),
          df.loc[df['qc_covered_frac_500m'] < MIN_COVER, ['SITE_ID', 'YEAR', 'qc_covered_frac_500m']].values.tolist())
    print("STEP01_DONE")


if __name__ == "__main__":
    main()
