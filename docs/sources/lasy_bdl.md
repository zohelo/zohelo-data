# Lasy Państwowe Bank Danych o Lasach, forest districts and RDLP (`lasy_bdl`)

**Built (26 Nov 2026, register PL-ENV-018, #3301).** Route: the Forest Data Bank's public GeoServer WFS 2.0
(`https://wfs.bdl.lasy.gov.pl/geoserver/BDL/ows`, listed on `https://www9.bdl.lasy.gov.pl/portal/uslugi-ogc`; no
robots.txt, no published limits, CountDefault 1,000,000): one GetFeature per feature type in `application/json`
(GeoJSON in the native EPSG:2180, every attribute), declared as two files of the shared `bulk_file` pattern (no
extractor code; `requests` percent-encodes the type names in UTF-8). Entity = one feature type: `districts`
(`BDL:Nadleśnictwa`, 429 features, about 50 MB) and `regions` (`BDL:RDLP`, 17 features, about 11 MB); the WFS gives no
version (`a_year` is the data year, 2026 on every feature), so version = download time, and a copy lands only when its
SHA-256 changed; restart unit one layer. GeoServer stamps each answer with the request time (`"timeStamp"`), so
`reduce: lasy_bdl` (`ingestion/to_landing/lasy_bdl/`) removes that one member before Landing; everything else lands
byte for byte, and an answer without it fails the download.

- Bronze `bronze.lasy_bdl_features` (`ingestion/to_bronze/lasy_bdl/load.py`, DuckDB JSON and spatial): one row per
  feature, every property as text, the GeoServer feature id. The geometry is **not stored** in Bronze (it stays in
  Landing and the Archive): the loader computes from it the planar area in EPSG:2180 (m²; the layers carry no area
  attribute) and the centroid and a point on the surface (EPSG:4326 degrees). A response that is not a
  FeatureCollection in EPSG:2180, has a property set other than its layer's declared layout, repeats a feature id or
  holds a geometry that is not a polygon is rejected into `bronze.lasy_bdl_rejected_files`.
- Silver: `silver.lasy_bdl_districts` (grain district_code = RDLP code-district code; forest address, name, data year,
  area, centroid and point) and `silver.lasy_bdl_regions` (grain region_code; name as published in upper case, data
  year, area, centroid and point). Names are organisational units, not persons. The area is that of the unit's
  territorial range (zasięg terytorialny), all land in it, not the forest area; planar EPSG:2180 areas differ from
  ellipsoidal areas by a fraction of a per cent.
- Gold block `gold.build_lasy_bdl` (`warehouse/macros/sources/lasy_bdl.sql`): count of forest districts and the summed
  area of their territorial ranges (m²) for Poland, the 16 voivodeships and each RDLP (breakdown `rdlp` under Poland),
  and the area of each district (breakdown `district`), at the download date (frequency D). This is the register
  state on that date. The WFS has no territory attribute: a district counts, with its whole area, in the voivodeship
  of the PRG gmina (`silver.prg_units`) whose boundary holds the point on its surface (districts cross voivodeship
  borders; the area is not split). No KPI (register `kpi: false`).
- Schedule: monthly, the 21st at 04:05 UTC (O46). Refresh full (D32).
- Licence: `unresolved`, internal only (licence gate). Regulamin of the Forest Data Bank
  (`https://www.bdl.lasy.gov.pl/portal/regulamin`): access is public and free of charge, re-use by others than
  administrative bodies "is subject to the rules on the re-use of public information", the user must "inform about the
  source, the time of creating and accessing public sector information"; commercial reuse and modification are not
  stated. Attribution: "Źródło: Bank Danych o Lasach, Lasy Państwowe" with the date of creation and of download.
- Left out: forest ranges (`BDL:Leśnictwa`, 5,259 features, 128 MB a download, four forest addresses repeated; only
  name and address, so a count per district is all it would add), the 17 per-RDLP stand layers (`RDLP_<x>_wydzielenia`,
  36,000 to 244,000 stands each: species, age, site type; a candidate for a later entity-table source), the OGC API
  Features, WMS and WMTS services, and forest area or cover per gmina (GUS BDL).
