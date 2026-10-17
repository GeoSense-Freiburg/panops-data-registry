# How each EFP is calculated, and Miguel's circularity concern

**Date:** 2026-09-17
**Question addressed:** FORTRACK meeting action item #1 (Miguel, 2026-09-14) — "several EFPs are designed to be relatively independent of the climate variables used in their calculation. Using monthly climate variables to predict annual EFPs could therefore introduce conceptual/circularity issues. Recommendation: reconsider the predictor set for each EFP individually, excluding variables that were directly used to derive/normalize that EFP." This report documents exactly how each EFP is computed in the codebase, as groundwork for deciding whether/how to act on that recommendation.

---

## Where this happens in the code

`notebooks/14_calc_EFPs.ipynb` computes all EFPs, one site-year at a time, via the function `EFPcalc_year()` (cell 12), fed by `read_fluxmet_hh()` (cell 10) which reads and standardises the raw half-hourly FLUXNET `FLUXMET_HH` files.

### Raw inputs (from `read_fluxmet_hh()`)

Standard FLUXNET half-hourly columns are renamed/derived once per site-year:

| derived column | source |
|---|---|
| `GPP_NT`, `RECO_NT`, `NEE` | `GPP_NT_VUT_REF`, `RECO_NT_VUT_REF`, `NEE_VUT_REF` (standard nighttime-partitioning flux products) |
| `LE`, `H` | `LE_F_MDS`, `H_F_MDS` (energy fluxes) |
| `TA`, `VPD` (→ `VPD_kPa`) | `TA_F`, `VPD_F / 10` |
| `SW_IN` (→ `PPFD_IN_FROM_SWIN`) | `SW_IN_F`, converted via `× 2.11` |
| `P`, `WS`, `USTAR`, `CO2` | `P_F`, `WS_F`, `USTAR`, `CO2_F_MDS` |

---

## Per-EFP calculation

### GPPsat

Fit a Michaelis-Menten light-response curve (`bigleaf::light.response()`, inputs `NEE`, `Reco = RECO_NT`, `PPFD` derived from `SW_IN`) independently on every consecutive 5-day window across the growing season. Take the asymptote parameter from each window's fit; **GPPsat = the 90th percentile of those per-window asymptotes across the year.** Growing-season filtering is itself threshold-based on `GPP_NT` and temperature.

→ **Bakes in `SW_IN` continuously** (as the light-response curve's x-axis) **and `TA`** (via the growing-season filter, and via `RECO_NT`, which is itself a temperature-driven partitioning product computed upstream of this notebook).

### NEPmax

Subset to daytime records (`PPFD > 200 × 2.11`, i.e. an `SW_IN` threshold), take `−NEE` at those records, then **NEPmax = the 99th percentile**.

→ Uses `SW_IN` only as a **threshold/filter**, not as a continuous input — a much weaker climate dependence than GPPsat.

### ETmax

Convert `LE` to `ET` via `bigleaf::LE.to.ET(LE, TA)` — this needs `TA` because the latent heat of vaporization is temperature-dependent — restricted to daytime + turbulent conditions (`SW_IN > 200`, `USTAR > 0.2`), then **ETmax = the 95th percentile of ET**.

→ **`TA` is directly, functionally embedded** in the ET conversion itself, not just used as a filter.

### WUE / uWUE

`bigleaf::WUE.metrics()` on the same daytime/turbulent subset, using `GPP_NT`, `NEE`, `LE`, `VPD_kPa`, `TA`. uWUE specifically exists to *remove* VPD's influence on raw WUE (uWUE ≈ GPP×√VPD / ET), but this only partially decouples it — both metrics still pass `TA` and `VPD` through the underlying ET/LE conversion.

→ **`VPD` and `TA` are directly embedded** in both.

---

## Why this matters for the predictor set

The climate predictor block used in every V10 model (`lag1_*` columns — monthly summaries of `TA`, `VPD`, `SW_IN`, `P`) is strictly **the 12 months *before* the response year** — `get_predictor_cols()` (used identically across every `run_v10_*.R` script) only ever selects `^lag1_` / `^lag2_` prefixed columns, never a same-year/contemporaneous climate block. So there is no literal "using the same half-hourly data point twice": the EFP is computed from *this* year's flux data, the predictor is *last* year's climate summary.

That doesn't fully dissolve Miguel's concern, though, because:

- Temperature, radiation, and VPD are all strongly **autocorrelated year-to-year** at a given site — last year's TA is a decent proxy for this year's TA.
- So predicting **GPPsat** from `lag1_SW_IN_*` / `lag1_TA_*`, or **ETmax / WUE / uWUE** from `lag1_TA_*` / `lag1_VPD_*`, is predicting an EFP using a lagged proxy for the *same physical quantities* that were used to derive that EFP in the first place. Not a literal data leak, but the model could be partly "rediscovering" the EFP's own definition rather than learning something new about ecosystem function from climate.
- **NEPmax is the least exposed** (SW_IN only sets a threshold, never enters a fitted curve) — which may be part of why it's one of the two EFPs (with GPPsat) showing the most robust disturbance improvement across every test this project has run, though for different reasons in each case (GPPsat has the *strongest* climate coupling of any EFP here, so this isn't a simple "less circular = more robust" story either).

### Summary table

| EFP | climate variables embedded in its own calculation | how |
|---|---|---|
| GPPsat | `SW_IN` (continuous), `TA` (filter + upstream Reco partitioning) | light-response curve fit |
| NEPmax | `SW_IN` (threshold only) | daytime filter |
| ETmax | `TA` (continuous) | LE→ET conversion |
| WUE | `TA`, `VPD` (continuous) | GPP/ET via bigleaf |
| uWUE | `TA`, `VPD` (continuous, partially normalised) | GPP×√VPD/ET via bigleaf |

---

## Concrete next step (not yet implemented)

Matching what Miguel actually proposed: for each EFP, drop the climate predictor(s) most implicated in its own calculation and check whether the disturbance effect (RMSE gain, SHAP) holds up:

- **ETmax** models without `lag1_TA_*`
- **WUE / uWUE** models without `lag1_TA_*` and `lag1_VPD_*`
- **GPPsat** models without `lag1_SW_IN_*` and `lag1_TA_*`

This is meeting action item #1, still open. Worth prioritizing alongside item #2 (René's suggestion to replace monthly climate histories with static long-term descriptors), since both bear on the same underlying predictor-design question.

---

## Provenance

- EFP calculation code: `notebooks/14_calc_EFPs.ipynb`, function `EFPcalc_year()` (cell 12), fed by `read_fluxmet_hh()` (cell 10)
- Predictor selection: `get_predictor_cols()`, identical across every `scripts/run_v10_*.R` script — climate block is `grep('^lag1_', ...)` (12m window) or `grep('^(lag1_|lag2_)', ...)` (24m window), never same-year
- Meeting notes: `meeting's note/2026-09-14-fortrack-monthly-meeting.md`, action item #1
