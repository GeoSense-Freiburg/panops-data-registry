# How to cite these data

These data are FLUXNET products (ONEFlux v1.3) shared under **CC-BY-4.0**. You can use,
share and modify them for any purpose, but **attribution is required with every use**. The
authoritative text is in `DATA_POLICY_LICENSE_AND_INSTRUCTIONS.txt`. This file summarises
what applies to the 37 sites here (source networks ICOS and FLX only).

## 1. Required: one data citation per site used

Cite each site's FLUXNET product in the **reference list**, not only in the supplement, so that
citation tracking works. The citation must include its PID (a handle URL).

- Plain text, one per site: `citations_per_site.txt`
- BibTeX, one entry per site: `site_citations.bib` (keys like `DE_Tha_FLUXNET_v13`)
- Also in `sites.csv`, in the `CITATION` and `PID` columns

Example:

> Janssens, I., Bresseleers, S., De Meulder, T., et al. (2026). Fluxnet Archive Product from
> Brasschaat, 1999–2024, FLUXNET, https://hdl.handle.net/11676/LFEir6YGDlkpjoTnCuB1qWeS

Cite only the sites you actually use in the final analysis.

## 2. Required: acknowledgement text

Include this sentence in the Acknowledgements:

> FLUXNET data products were produced and harmonized by eddy covariance regional networks and
> data processing centers, including AmeriFlux, ChinaFlux, European Fluxes Database, ICOS,
> JapanFlux, KoFlux, OzFlux, SAEON, and TERN. These products also include a modified version
> of ERA5 hourly data provided by the Copernicus Climate Change Service.

No other network-specific sentences are needed, because none of these sites is AMF, CNF, JPF,
OZF or TERN. ICOS also has citation and acknowledgement guidance at
https://www.icos-cp.eu/how-to-cite.

## 3. Required if your paper has a Data Availability section

> The FLUXNET data products used in this work were downloaded from the ICOS Carbon Portal
> (https://data.icos-cp.eu/portal/) via the FLUXNET Shuttle on 12 May 2026.

(DE-RuW was downloaded on 29 September 2026. You can give both dates, or simply cite the
per-site PIDs, which pin the exact versions.)

## 4. Recommended references

Processing pipeline (strongly recommended by FLUXNET):

- Pastorello, G., Trotta, C., Canfora, E. et al. (2020). The FLUXNET2015 dataset and the ONEFlux
  processing pipeline for eddy covariance data. *Scientific Data* 7, 225.
  https://doi.org/10.1038/s41597-020-0534-3

Flux partitioning methods, if you use GPP_NT / GPP_DT:

- Reichstein, M., Falge, E., Baldocchi, D. et al. (2005). On the separation of net ecosystem
  exchange into assimilation and ecosystem respiration: review and improved algorithm.
  *Global Change Biology* 11, 1424–1439. https://doi.org/10.1111/j.1365-2486.2005.001002.x
  (night-time method, GPP_NT)
- Lasslop, G., Reichstein, M., Papale, D. et al. (2010). Separation of net ecosystem exchange into
  assimilation and respiration using a light response curve approach: critical issues and global
  evaluation. *Global Change Biology* 16, 187–208. https://doi.org/10.1111/j.1365-2486.2009.02041.x
  (daytime method, GPP_DT)

```bibtex
@article{Pastorello2020,
  author  = {Pastorello, Gilberto and Trotta, Carlo and Canfora, Eleonora and others},
  title   = {The {FLUXNET2015} dataset and the {ONEFlux} processing pipeline for eddy covariance data},
  journal = {Scientific Data}, volume = {7}, pages = {225}, year = {2020},
  doi     = {10.1038/s41597-020-0534-3}
}
@article{Reichstein2005,
  author  = {Reichstein, Markus and Falge, Eva and Baldocchi, Dennis and others},
  title   = {On the separation of net ecosystem exchange into assimilation and ecosystem respiration: review and improved algorithm},
  journal = {Global Change Biology}, volume = {11}, number = {9}, pages = {1424--1439}, year = {2005},
  doi     = {10.1111/j.1365-2486.2005.001002.x}
}
@article{Lasslop2010,
  author  = {Lasslop, Gitta and Reichstein, Markus and Papale, Dario and others},
  title   = {Separation of net ecosystem exchange into assimilation and respiration using a light response curve approach: critical issues and global evaluation},
  journal = {Global Change Biology}, volume = {16}, number = {1}, pages = {187--208}, year = {2010},
  doi     = {10.1111/j.1365-2486.2009.02041.x}
}
```

## 5. Encouraged: contact the site teams

Co-authorship or collaboration with site PIs is not required under CC-BY-4.0, but FLUXNET
strongly encourages it. PIs often catch site-specific problems such as management events,
disturbances or instrument changes. Their names, roles and e-mails are in `sites.csv`
(`TEAM_MEMBERS`, `TEAM_ROLES`, `TEAM_EMAILS`).
