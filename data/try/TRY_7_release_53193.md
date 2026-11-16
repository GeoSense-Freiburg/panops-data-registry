# TRY 7.0 data release, request 53193

Raw (not gap-filled) TRY plant-trait records, as delivered by TRY on 2026-11-09.

- **Archive:** `TRY_7_release_53193/try7_53193_09112026225022.zip` (md5 fe17186662f7d358cf4430e678db80b4).
- **Contents:** `53193.txt` (15.8 GB, tab-separated long format, one row per TRY data point:
  `SpeciesName`, `AccSpeciesName`, `ObservationID`, `ObsDataID`, `TraitID`, `TraitName`,
  `DataID`, `OrigValueStr`, `StdValue`, `UnitName`, `ErrorRisk`, `OrigObsDataID`, …),
  `TRY_7.0_Data_Release_Notes.pdf` and `TRY_Intellectual_Property_Guidelines.pdf`.
- **Size:** 36,964,300 rows, 78,912 accepted species.
- **Traits (31, the TRY 6 trait set, TraitID: rows):** 3106 plant height (997,353), 21 stem
  diameter (792,273), 47 LDMC (350,496), 26 seed mass (318,731), 3117 SLA (303,255), 55 leaf
  dry mass, 14 leaf N per mass (204,222), 46 leaf thickness, 145 leaf width, 3114 leaf area,
  4 stem specific density, 50 leaf N per area, 3120 leaf water content, 163 leaf fresh mass,
  13 leaf C, 144 leaf length, 15 leaf P per mass (75,021), 146 leaf C/N, 95 seed germination
  rate, 78 leaf δ15N, 27 seed length, 6 rooting depth, 237 diaspore length, 138 seed number,
  281 conduit diameter, 169 conduit density, 1080 SRL, 614 fine-root SRL, 289 wood fiber
  length, 282 vessel element length, 297 wood ray density (605).
- **Use:** follow the TRY intellectual-property guidelines in the archive. Standard
  filtering: drop duplicates (`OrigObsDataID` set) and outliers (`ErrorRisk` ≥ 4).
- **First use:** measured-only species means of SLA, leaf N and leaf P for the
  trait-distribution paper's benchmark against Moreno-Martínez et al. 2026 (2026-11-16).
