# 0002 — Ecosystem respiration: not modelled, quick within-site check instead

**Date:** 2026-10-21
**Status:** Active
**Trigger:** Talie Musavi's comment that ecosystem respiration hardly changes after mortality and that NEP changes most
(`meeting's note/2026-10-21-talie-musavi-recommendations.md`).

## Decisions

### D1: Respiration is not one of the modelled properties
The paper models four ecosystem functional properties: GPPsat, NEPmax, ETmax and WUE. Basal respiration (Rb, Rbmax),
computed with the other properties in `notebooks/14_calc_EFPs.ipynb`, was left out because its yearly estimate fails in
most years:

| | Site-years | Sites |
|---|---|---|
| GPPsat, NEPmax, ETmax, WUE (paper set) | 361 of 361 | 101 of 101 |
| Rb / Rbmax in the paper site-years | 55 of 361 (15 %) | 36 of 101 |
| All FLUXNET site-years since 2017, before tree-cover / mortality filters: GPPsat vs Rb | 1,357 vs 269 | 274 vs 154 |

A model on 55 site-years, with very few high-mortality years among them, would not be informative.

### D2: Quick test with FLUXNET respiration instead of a full model
Script: `scripts/paper_v41/53_reco_mortality_check.R`; outputs in `plots/paper_v5_nothresh/reco_check/`.

- Growing-season means of ecosystem respiration (`RECO_NT_VUT_REF`), GPP (`GPP_NT_VUT_REF`) and NEP (−`NEE_VUT_REF`) from the
  FLUXNET daily (`FLUXMET_DD`) files; same 101 sites, 361 site-years and growing seasons as the paper
  (`derived_tables/paper_v41/nepmax90_gs_by_site_year.csv`).
- Mixed model: flux ~ relative mortality of the site-year (per +10 %) + (1 | site), so the slope is mostly a within-site
  change; site-block bootstrap (1,000 draws) of the slope.

| Growing-season mean (g C m⁻² d⁻¹) | Change per +10 % relative mortality | 95 % bootstrap interval | p (bootstrap) |
|---|---|---|---|
| Respiration (RECO) | +0.01 | −0.31 to +0.23 | 0.97 |
| GPP | −0.25 | −0.55 to −0.06 | 0.004 |
| NEP | −0.28 | −0.41 to −0.14 | < 0.001 |

Result: within sites, respiration does not change with mortality while gross and net productivity decrease — in line with
Clark et al. (2024, For. Ecol. Manage. 553:121589) and Renninger et al. (2014, JGR Biogeosciences 119:630–644).

**Caveat:** expressed as % of each site's mean, the GPP and NEP slopes keep their sign but are not significant (wide
intervals; the % scale is unstable for NEP because its site means are small, 1.8 g C m⁻² d⁻¹ on average). The absolute
effect comes mainly from productive sites with mortality, so the draft reports the absolute slopes.

### D3: Where it is used
One sentence in the Discussion (respiration unchanged, productivity down → reduced uptake rather than respiration from dead
trees) and one in Methods (draft commit 3e69b10). A full respiration model (RECO as a fifth property, C+T vs C+T+M) is kept
for a possible reviewer request.
