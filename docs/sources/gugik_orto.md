# GUGiK orthophoto sheet index (`gugik_orto`)

**Built (26 Nov 2026, register PL-ENV-004, #3100 #3101 #3102).** The orthophotos themselves (GeoTIFF sheets,
terabytes) are never downloaded. Route: the sheet index, WFS 2.0.0
https://mapy.geoportal.gov.pl/wss/service/PZGIK/ORTO/WFS/Skorowidze (GML 3.2 only, paging, no key; robots.txt: HTTP
404; capabilities: fees and access constraints "Brak ograniczeń"). One feature type per data year,
`gugik:SkorowidzOrtofomapy<YYYY>`: 49 years from 1957 to 2026, 1,337,733 features on 26 Nov 2026 (resultType=hits;
1997 and 2003 to 2025 hold most of them, the largest year 2017 with 147,937). One feature = one sheet edition: sheet id
(godło), data year and date, pixel size, colour (RGB, CIR, B/W), source (digital or analogue photos), CRS of the sheet
(PL-1992, PL-2000 zones), archive scale, submission number, partially filled flag, download link, PZGiK date, file
size, and the footprint polygon (EPSG:2180).

Entity = one year (`orto_<YYYY>`), version = download time (the service gives none), restart unit one year:
`ingestion/to_landing/gugik_orto/` asks every year's hits count, lands missing years first (newest first), then years
whose count changed, and re-reads the current and previous year every 28 days; older years only when their count
changes. A year is read whole in pages of 5,000 sorted by the download link (pages without a sort order are not stable:
the same page twice held other features, Dev probe 45813550746; sorted, it came back identical, 45814802349), checked
(numberReturned, the hits total, no feature id twice) and landed as `<version>__<ingested>__part-<nnn>.gml` with one
receipt per page; a year whose content equals the newest copy (SHA-256 without the answer's `timeStamp`) writes
nothing. The server is slow (about 15 minutes for the 49 hits requests, about 30 s per sorted page), so the first
load spreads over several runs within their budget.

Bronze `bronze.gugik_orto_sheets` (entity table, design #875): every feature as published, the 13 properties as text,
the footprint's first posList and ring count, the page's SHA-256; a page that is not the year's feature collection or
an unknown property rejects the year. Silver `silver.gugik_orto_sheets` (entity table): one row per feature
(`feature_id` from the gml:id, unique) = sheet x product x edition, typed, with the footprint centroid (EPSG:2180;
the GML lists northing before easting) and planar area in km² computed from the polygon.

Gold `build_gugik_orto` (block `sheets`, A, `warehouse/macros/sources/gugik_orto.sql`): sheet editions by data year in
total and by pixel class (≤ 0.05 m, 0.05–0.10, 0.10–0.25, > 0.25 m, `_U` without a pixel size) and the share of each
class (%), for Poland (every feature) and each voivodeship (centroid inside its PRG boundary, `silver.prg_units`;
outside every voivodeship or without a centroid: Poland only). Covered area is not given: editions overlap, so a sum of
footprints overstates it. No KPI (register `kpi: false`).

Licence: free use, "Ortofotomapa jest dostępna bezpłatnie do pobrania i możliwa do dowolnego wykorzystania"
(https://www.geoportal.gov.pl/pl/dane/ortofotomapa-orto/), attribution "Źródło: GUGiK, Ortofotomapa"
(`free_use_gugik_orto`). Schedule: weekly, Friday 02:50 UTC (O46).

Dev trial TRIAL_PLACEHOLDER

Left out: the imagery, the WMS/WMTS views and the true-ortho layer; an edit to an old year that keeps its count is
seen only when the count changes.
