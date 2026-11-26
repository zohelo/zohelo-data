# GUGiK PRG (national register of boundaries) (`gugik_prg`)

**Route (implemented):** the national package
`https://opendata.geoportal.gov.pl/prg/granice/00_jednostki_administracyjne.zip` (~377 MB,
shapefiles, geographic ETRS89 coordinates per the `.prj`): A00 country (1), A01 voivodeships (16), A02 powiats (380), A03
gminas (2,479), A05 cadastral units (3,227), A06 cadastral precincts (54,264), counts of
October 2026. Every layer carries the JPT attribute set (`JPT_KOD_JE` = TERYT code,
`JPT_NAZWA_`, `JPT_POWIER` area, validity `WAZNY_OD/DO`, version `WERSJA_OD/DO`, `REGON` for
A01–A03). The server sends no Last-Modified or ETag: every run downloads (~7 minutes from
GitHub Actions) and writes Landing only when the SHA-256 changed. GUGiK states the data is
free for any purpose (Regulation of the Council of Ministers of 16 July 2021, Dz.U. 2021
poz. 1373); updated yearly (state 1 January) with corrections. WFS
(`https://mapy.geoportal.gov.pl/wss/service/PZGIK/PRG/WFS/AdministrativeBoundaries`, GML,
1,000 features per request) is the alternative, not used.

Bronze `bronze.prg_boundaries`: one row per feature of every layer, attributes as text,
geometry as WKB in the source CRS with the `.prj` text (60,367 features, ~434 MB Parquet); any reprojection is a Silver rule.

## Address points (register PL-ENV-007, #3106)

**Route (implemented):** the national address package
`https://opendata.geoportal.gov.pl/prg/adresy/PunktyAdresowe/POLSKA.zip` (~565 MB, SHP; Last-Modified and ETag sent,
rebuilt by GUGiK most days), a second entity `punkty_adresowe` of this source (`version: last_modified`, snapshot,
changed since last by SHA-256). Its members are Deflate64, which Python's `zipfile` cannot test, so it lands as exact
bytes (`format: raw`, `.bin`) and the Bronze decoder unpacks it with 7z, CRC checked. Four layers: localities
(68,388 points), squares (8,646), streets (297,933 lines) and **address points** (8,647,096 points, 26 Nov 2026,
Dev probes 45810275130 and 45811745316); only the address points are loaded. Monthly with the boundaries (O46). The
GML package `PRG-punkty_adresowe.zip` (~805 MB) carries the same points and is not used.

**Entity, version, restart unit:** entity = the national package; version = Last-Modified; the restart unit is the
package: `ingestion/to_bronze/gugik_prg/entities.py` (called by `bronze.load` after the boundaries) unpacks the address
layer (~7.4 GB on the runner's disk), reads it with DuckDB spatial in 8 GB memory and replaces the one file of the
entity in `bronze.prg_address_points` (attributes as text, x and y as printed, `.prj` text) and in
`silver.prg_address_points` (`warehouse/entity/prg_address_points.sql`). A rerun with both files current writes
nothing; a package with other attributes, another row count than its `.shx`, a repeated `ID_IIP`, a null key, a
gmina code that is not 7 digits or a point outside Poland is rejected into `bronze.prg_rejected_files`; Silver keeps
the previous package (Bronze's copy of it is then superseded and removed by the latest-only prune after the load,
#845).

**Silver** `silver.prg_address_points` (entity table, not a dbt model): grain `address_id` (`ID_IIP`: namespace, local
id and version stamp), TERC of the gmina (`TERYT_GMI`), SIMC (`ID_SIMC`), ULIC (`ID_ULIC`, null without a street),
locality and street names, house number, postal code (only `NN-NNN`, the printed text kept), date assigned, x and y in
EPSG:2180 as published (not reprojected). Row policy: #822 ceilings apply to full-rebuild dbt models; a table of
~8.6 M rows is an entity table replaced per package (design #875, as GLEIF and the VAT white list), so Silver keeps
every point. No persons: the layer holds addresses, not residents. Dev trial 45827301180 (package of 26 Nov 2026):
8,647,096 points in Bronze (376 MB Parquet) and Silver (361 MB), 0 repeated ids, 0 null keys; the load (unpack, read,
Silver, upload) took about 3 minutes at 3.3 GB peak memory plus 1.4 GB for 7z, the download about 8 minutes. No-value
markers: the postal code prints the text `null` 751 times (26 more are not `NN-NNN`), one ULIC code prints `null`;
both stay as published, `postal_code` is null there.

**Gold** `gold.prg_address_counts` (dbt): address points per gmina (the point's TERC code), powiat, voivodeship and
Poland; the build stage `build_prg` carries them as block `punkty_adresowe`, measure
`gugik_prg:punkty_adresowe:address_points`, daily at the package's Last-Modified date (entity snapshot, latest only),
KPI `address_points` (draft).
