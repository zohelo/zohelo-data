# PIG-PIB CBDG boreholes (`pig_boreholes`)

**Built (26 Nov 2026, register PL-ENV-014, Task #3300).** Route: the public WFS of the Central Geological Database
(CBDG), subsystem boreholes, that PIG-PIB lists on dane.gov.pl dataset 677
(`https://cbdgmapa.pgi.gov.pl/arcgis/services/cbdg_otwory/MapServer/WFSServer`, ArcGIS, WFS 2.0.0), its only feature
type `cbdg_otwory:Otwory_wiertnicze_CBDG` (336,758 points on 26 Nov 2026). The download manager (`dm.pgi.gov.pl`) mails
its links and, like `baza.pgi.gov.pl` and `www.pgi.gov.pl`, answers runners with a bot-protection page: never used.
The server refuses `SORTBY` but returns `count`/`startIndex` pages in OBJECTID order, and the same page read twice was
byte-identical (Dev probes 45898339964, 45898682643). `ingestion/to_landing/pig_boreholes/` reads the layer as pages of
25,000 features in `GEOJSON` (EPSG:4326), one request at a time with a pause, checks them (FeatureCollection, OBJECTIDs
strictly ascending across the pages, their number = `resultType=hits`) and only then uploads them as the parts of one
version, with a receipt after each part; nothing is written when every page is unchanged. Entity = the layer
(`boreholes`); the WFS gives no version, so version = download time; restart unit the layer.

- Bronze `bronze.pig_boreholes_features` (`ingestion/to_bronze/pig_boreholes/load.py`, DuckDB JSON): one row per
  feature, one Parquet file per page, every property as text, the point's longitude and latitude. A page that is not a
  FeatureCollection in EPSG:4326, has a property set other than the declared layout, a feature without a Point, or an
  OBJECTID twice is rejected into `bronze.pig_boreholes_rejected_files`; a newest download without all its parts is
  not loaded. Bronze keeps only the newest download (latest-only prune); a page unchanged between downloads keeps the
  version it was first loaded with.
- Silver `silver.pig_boreholes_boreholes`: one row per borehole, grain `id_cbdg` (OBJECTID unique too); depth,
  stratigraphy, drilling purpose and year, elevation, coordinates, location basis, locality and the gmina, powiat and
  voivodeship names as published (Polish, with the WFS's English labels), archive, inventory number, core store,
  geophysics, analyses and studies; the producer's literal `null` is NULL. `snapshot_date` = download date: the
  register state on that date.
- Gold block `gold.build_pig_boreholes` (`warehouse/macros/sources/pig_boreholes.sql`): boreholes in CBDG
  (`pig_boreholes:boreholes:boreholes:cbdg`), total and by drilling purpose (`Cel_wiercenia` as published, `_U` where
  none), for Poland, the voivodeships and the powiats, at the download date (frequency D). A count of what the register
  holds on that date, not of boreholes drilled in a period (no measure by drilling year). A borehole is placed by its
  point in the PRG gmina (`silver.prg_units`) whose boundary covers it, the powiat and voivodeship from the gmina's
  TERYT code; a point on the border of two powiats (voivodeships) is left out of that level (none on 26 Nov 2026),
  and points outside every gmina (502, at sea or across the border) count for Poland only, the register's total.
  The WFS's name fields are a cross-check only (the voivodeship name agreed for 335,443 placed boreholes, differed
  for 7, was empty for 806; Dev trial 45899934080). Borehole names stay out of Gold. No KPI (register `kpi: false`).
- Schedule: monthly, the 24th at 04:15 UTC (O46; about 15 requests). Refresh full (D32): one layer, no changed-since.
- Licence: open-conditional, CC BY 4.0 (dane.gov.pl dataset 677), with PIG-PIB's rules for re-use
  (`https://www.pgi.gov.pl/rejestry-i-bazy-danych-geologicznych/zasady-korzystania-z-isp.html`): give the source, the
  date of the data and of the download, and the processing. Attribution "Źródło: Państwowy Instytut Geologiczny - PIB,
  Centralna Baza Danych Geologicznych" with the download date. Consent-bound information (geophysical measurements)
  is not in the WFS and is not used.
- Left out: counts by gmina, depth class or drilling year; the hydrogeological and geophysical CBDG subsystems.
