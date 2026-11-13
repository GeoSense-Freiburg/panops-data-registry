# Moreno-Martínez et al. 2026: leaf-trait moment maps (1 km)

Global maps of the community-weighted mean, SD, skewness and excess kurtosis of
three leaf traits, from GBIF occurrences, TRY species means and MODIS plant
functional type cover.

- **Paper:** Moreno-Martínez, Á., Muñoz-Marí, J., Adsuara, J. E., Dechant, B.,
  Kattge, J., et al. (2026). Leveraging remote sensing and crowd-sourced
  biodiversity data for enhanced plant functional trait mapping. *Nature
  Communications*. https://doi.org/10.1038/s41467-026-72111-6
- **Data:** Zenodo record 18832368, https://doi.org/10.5281/zenodo.18832368,
  published 2026-03-02.
- **License:** CC-BY-4.0. Cite the paper and the Zenodo record when using these files.
- **Added:** 2026-11-13, unzipped from the Zenodo archives without other changes.

## Contents (`moments_1km/`)

| Folder | Files | Bands |
| --- | --- | --- |
| `SLA/`, `LNC/`, `LPC/` | 18 GeoTIFF tiles each (`<trait>_1.tif` … `<trait>_18.tif`) | 1 Mean, 2 Std log, 3 Skewness, 4 Excess Kurtosis (band names as delivered; see the scale notes below) |
| `QA_SLA/`, `QA_LNC/`, `QA_LPC/` | 18 GeoTIFF tiles each | 1 Grid cell size (selected quadrat), 2 Missing Species (abundance share without trait data) |

- Traits: specific leaf area (mm² mg⁻¹), leaf nitrogen per mass (mg g⁻¹), leaf
  phosphorus per mass (mg g⁻¹).
- All four moments are on the natural-log scale. Band 1 holds the ln-scale mean
  (SLA median 2.65 = ln 14 mm² mg⁻¹), although the paper describes back-transformed
  means; exp(band 1) gives the geometric mean in original units.
- Band 4 is named "Excess Kurtosis", but its values never fall below about 1.5 and
  center on 3 (5th–95th percentile 1.5–23 for SLA), which is raw kurtosis.
  Subtract 3 for excess kurtosis. Checked 2026-11-13 on tile 5 of each trait.
- Grid: EPSG:4326, 0.008983° (about 1 km), float32, NaN where there is no value.
- The paper validates only the means (against sPlotOpen); the higher moments are
  described as proof-of-concept products.

## Zenodo archive checksums (md5)

| Archive | md5 |
| --- | --- |
| SLA.zip | 5d7d2c6593aeb57e9d8361af776576d4 |
| LNC.zip | 2f6dac19a88785fb2c94f555c83425f8 |
| LPC.zip | aa1345cf7f932329b97766ba8bee68b0 |
| QA_SLA.zip | 515a46d16d8e00b4d38a105d4a9e6edb |
| QA_LNC.zip | b0ba53128bd40e9b055dc89b9637f83c |
| QA_LPC.zip | 25111e3fa1c0d6b5eecb3d350beda5ac |

## Use

```bash
dvc import <registry> data/trait_maps/moreno-martinez_2026/moments_1km -o data/raw/moreno-martinez_2026
```
