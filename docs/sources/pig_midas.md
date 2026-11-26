# PIG-PIB MIDAS, mineral deposits and mining areas (`pig_midas`)

**Built (26 Nov 2026, register PL-ENV-015, part of #2870).** Route: MIDAS's public WFS
(`https://cbdgmapa.pgi.gov.pl/arcgis/services/midas/MapServer/WFSServer`, ArcGIS, WFS 2.0): one GetFeature per feature
type in `GEOJSON` (EPSG:4326, every attribute), declared as six files of the shared `bulk_file` pattern (no extractor
code). The SHP packages of the download manager (`dm.pgi.gov.pl`) are delivered by e-mail only and are not used. The
WFS pages (`count`/`startIndex`, probe 45818533713), but one request per layer returns the whole layer and is lighter
for the server than 3-10 pages; the Dev trial compares each layer with `resultType=hits`. Entity = one feature type
(`deposits`, `mining_areas`, `mining_terrains`, and the archival selections `deposits_removed`,
`mining_areas_archival`, `mining_terrains_archival`); the WFS gives no version, so version = download time, and a copy
lands only when its SHA-256 changed; restart unit one layer.

- Bronze `bronze.pig_midas_features` (`ingestion/to_bronze/pig_midas/load.py`, DuckDB JSON): one row per feature,
  every property as text, the geometry as GeoJSON text. A response that is not a FeatureCollection in EPSG:4326, has a
  property set other than its layer's declared layout, or repeats an OBJECTID is rejected into
  `bronze.pig_midas_rejected_files`.
- Silver (geometry left in Bronze; centroid and a point on the surface kept): `silver.pig_midas_deposits` (layer x
  OBJECTID; a deposit number has one contour per field and mineral), `silver.pig_midas_mining_areas` and
  `silver.pig_midas_mining_terrains` (layer x ID_konturu; register number, legal dates, decisions, legal area in m²).
  The archival layers are in the same tables (`layer` `*_archival` / `deposits_removed`).
- Gold block `gold.build_pig_midas` (`warehouse/macros/sources/pig_midas.sql`): documented deposits (distinct deposit
  numbers, total and by mineral as published) and current mining areas (contours, legal area in m²), Poland and
  voivodeships, at the download date (frequency D). The WFS has no territory attribute: a contour counts in the
  voivodeship of the PRG gmina (`silver.prg_units`) whose boundary holds the point on its surface; contours at sea
  count for Poland only. Archival layers and mining terrains stay out of Gold. No KPI (register `kpi: false`).
- Schedule: monthly, the 17th at 04:55 UTC (O46). Refresh full (D32).
- Licence: open-conditional, PIG-PIB rules for re-use of public sector information
  (`https://www.pgi.gov.pl/rejestry-i-bazy-danych-geologicznych/zasady-korzystania-z-isp.html`), named by the MIDAS
  page `https://www.pgi.gov.pl/midas/zrodla-danych.html`; attribution "Źródło: Państwowy Instytut Geologiczny - PIB,
  System Gospodarki i Ochrony Bogactw Mineralnych Polski MIDAS" with the date of the data and of the download, and a
  note on the processing.
- Left out: the yearly balance of mineral resources (resources and extraction in tonnes, the official reference
  state; GUS BDL and the PIG-PIB publication), concessions, powiat and gmina placement, a grouping of the 60 MIDAS
  minerals into energy / metallic / chemical / rock raw materials.
