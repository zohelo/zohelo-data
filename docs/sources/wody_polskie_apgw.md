# Wody Polskie water-management planning data (`wody_polskie_apgw`)

**Built (Nov 2026, register PL-ENV-013, #3313).** Route: the spatial database of the update of the river basin
management plans (aPGW) that Państwowe Gospodarstwo Wodne Wody Polskie publish on dane.gov.pl, dataset 599 "Baza danych
przestrzennych aktualizacji planów gospodarowania wodami" (https://dane.gov.pl/pl/dataset/599, CC BY 4.0, update
frequency "Nie dotyczy"): one Esri File Geodatabase ZIP per plan cycle. On 27 Nov 2026 the dataset held one,
`Geobaza_2aPGW_ver_20230915.gdb.zip` (resource 53330 of 20 Dec 2023: the 2nd update of the plans, database version 15 Sep
2023; 307,606,332 bytes, ETag `"6582d525-1255b33c"`, 59 layers), plus the plan's WMS and WFS view services (resources
30914, 53332; no file, not used). The download took 12 minutes from a GitHub runner (Dev probe 45921328581). The previous
plan cycle is not on dane.gov.pl, so history starts with the 2023 edition. Flood hazard and risk zones are not part of
this database (view-only WMS, vectors by written request) and are not loaded.

`ingestion/to_landing/wody_polskie_apgw/` reads the dataset's resource list once per run from the portal's public API
(`/1.4/datasets/599/resources`), HEADs every geodatabase it lists and downloads it through the shared bulk_file pattern
only when nothing is landed for it or the newest receipt's URL, ETag or Last-Modified differ (a weekly run is one API
request and one HEAD). Entity = the plan cycle's geodatabase, `geobaza_<n>apgw` from the file name (`geobaza_2apgw`);
version = Last-Modified; restart unit: one file. A new plan update is a new resource and a new entity; two resources for
one entity fail the run.

Bronze `bronze.wody_polskie_apgw_attributes` (`ingestion/to_bronze/wody_polskie_apgw/load.py`): DuckDB spatial (GDAL
OpenFileGDB through `/vsizip/`) lists the layers and fields and reads the attributes of the 11 loaded layers; the
geometry is not stored (it stays in Landing and the Archive), because the planning layers publish their area as an
attribute (`POW_JCWP`, `POWIERZCHN`, `AREA`, km²). One row per feature x field, values as published text, with the
layer, the feature's position in the layer scan, the geodatabase folder name and provenance. Loaded: the surface water
body (JCWP) area layers of lakes, transitional, coastal and reservoir bodies, `Charakterystyka_JCWP`, `Status_JCWP`, the
groundwater body (JCWPd) layer and `Charakterystyka_JCWPd`, `Obszary_dorzeczy`, `Regiony_wodne` and
`Regionalne_Zarządy_Gospodarki_Wodnej`. Listed in the run's annotation, not loaded: the other 48 layers (programmes of
measures, exemptions, environmental objectives, pressures and status details, protected areas, catchments, relation
and dictionary tables, main rivers, ecoregions, and the river JCWP lines, whose attributes `Charakterystyka_JCWP` also
holds; DuckDB cannot read their geometry: "Unsupported geometry type in WKB"). A layer that is neither loaded nor listed,
a loaded layer missing or with other fields, or an empty loaded layer rejects the file
(`bronze.wody_polskie_apgw_rejected_files`).

Silver (one row per plan cycle and official code; `'-909'` and `'-907'` in status columns are the publisher's "not
assessed" / "not applicable" markers, kept as published text):

- `silver.wody_polskie_apgw_surface_water_bodies`: plan_cycle x JCWP code (4,240 in the 2nd update). Category from the
  code prefix (RW river, LW lake, TW transitional, CW coastal; the 45 reservoir bodies are RW and flagged
  `is_reservoir`), designation (natural / heavily modified / artificial), published area (km²) of the lake,
  transitional, coastal and reservoir bodies, river basin district as published and its code (matched by name to the
  same geodatabase's `Obszary_dorzeczy`), water region, RZGW, RDOŚ, the published voivodeship, powiat and gmina lists
  with TERYT codes (`voivodeship_codes` parsed from the brackets, guarded by the part count), status and risk
  assessments.
- `silver.wody_polskie_apgw_groundwater_bodies`: plan_cycle x JCWPd code (174): number, EU code, published area (km²),
  the river basin district code taken from the code (`GW` + district + number, only when the code is exactly that) and
  the published names of its districts, water regions, RZGW and balance areas from `Charakterystyka_JCWPd`.
- `silver.wody_polskie_apgw_planning_units`: plan_cycle x unit kind x code: 9 river basin districts (RBD_MS_CD
  1000-9000), 23 water regions, 11 RZGW areas, with EU codes, names, parents and the published area (km²).

Gold: block `water_bodies` of `poc_build_wody_polskie_apgw`, reference-data counts of the plan's register (not
observations), annual with `date_key` = 1 January of the geodatabase's edition year (2023-01-01; never the download
time): surface water bodies in total and per category (river, lake, transitional, coastal), groundwater bodies, and
the sum of the published areas (lake, transitional, coastal and groundwater bodies; river bodies are lines). Poland in
total and by river basin district (`breakdown_json` `river_basin_district` = RBD_MS_CD); the surface counts also per
voivodeship named in the body's published list (a body in two voivodeships counts in both, so voivodeships do not sum
to Poland). Left out: per-voivodeship areas and groundwater counts (no published voivodeship for JCWPd; a geometry
intersection with PRG is not done), status and risk counts, programmes of measures, protected areas and catchments.
No KPI (register `kpi: false`).

Dev trial 45931064709 (27 Nov 2026): 1 file, 307,606,332 bytes (download 24 minutes); 174,704 Bronze rows from 11
layers, 0 rejected, 48 layers listed; second Landing run downloaded nothing (HEAD current) and second Bronze run wrote
nothing. Silver 4,240 JCWP (river 3,161 incl. 45 reservoirs, lake 1,068, transitional 7, coastal 4), 174 JCWPd, 43
planning units; 43 tests, 0 failures. Markers `-909` / `-907` (e.g. overall status `-909` 1,073 times) kept as text.
Gold 112 rows, 0 duplicates by series_key x date_key, every area matched. Poland, 2023: groundwater bodies 311,762.37
km²; lakes 2,397.92 km² (largest Śniardwy, 110.45 km²), transitional 2,409.88 km², coastal 629.13 km²; surface water
bodies by district: Wisła 2,251, Odra 1,722, Pregoła 187, Niemen 60, Łaba 8, Dunaj 5, Świeża 4, Dniestr 2, Banówka 1.

Schedule: weekly, Tuesday 07:15 UTC (O46; one API request and one HEAD, no download when nothing changed; plans change in
six-year cycles). Refresh full (D32). Archive: newest version plus 3 days (0.31 GB a version). Terms: dane.gov.pl
dataset 599 states CC BY 4.0 (`license_name`); Wody Polskie's data-sharing page allows free commercial and
non-commercial use "pod warunkiem wskazania źródła oraz czasu wytworzenia i pozyskania informacji"; licence key
`cc_by_4_0_wody_polskie_apgw`, status open-conditional, attribution "Źródło: Państwowe Gospodarstwo Wodne Wody Polskie,
Baza danych przestrzennych aPGW (2023), dane.gov.pl" with the date produced (15 Sep 2023) and the date obtained.
