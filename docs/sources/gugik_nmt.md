# GUGiK elevation sheet indexes: NMT, NMPT, LIDAR (`gugik_nmt`)

**Built (26 Nov 2026, register PL-ENV-005, #3103 #3104 #3105).** The terrain and surface models (grids) and the LIDAR
point clouds (LAS/LAZ, terabytes) are never downloaded. Route: the sheet indexes, six WFS 2.0.0 services linked from
the producer's pages (GML 3.2 only, paging, no key; robots.txt: HTTP 404), one per product and vertical datum, under
`https://mapy.geoportal.gov.pl/wss/service/PZGIK/<service>/WFS/Skorowidze`:

| Entity prefix | Service | Feature types | Features (26 Nov 2026) |
|---|---|---|---|
| `nmt_evrf2007` | NumerycznyModelTerenuEVRF2007 | SkorowidzNMT2018 … 2026 | 400,038 |
| `nmt_kron86` | NumerycznyModelTerenuKRON86 | SkorowidzNMT2000, 2004 … 2019 | 277,889 |
| `nmpt_evrf2007` | NumerycznyModelPokryciaTerenuEVRF2007 | SkorowidzNMPT2019 … 2026 | 81,265 |
| `nmpt_kron86` | NumerycznyModelPokryciaTerenuKRON86 | SkorowidzNMPT2010 … 2019 | 131,001 |
| `lidar_evrf2007` | DanePomiaroweLidarEVRF2007 | SkorowidzDanychPomiarowychLIDAR2018 … 2026 | 447,892 |
| `lidar_kron86` | DanePomiaroweLidarKRON86 | SkorowidzDanychPomiarowychLIDAR2010 … 2019 | 315,017 |

One feature = one sheet edition in one file format: sheet id (godło), data year and date, assortment (NMT, NMPT,
chmura punktow), file format (11 values: ARC/INFO ASCII GRID, ASCII XYZ GRID, LAS, LAZ, TIN and contour formats …), spatial
characteristic (grid size `0.15 m` to `50.0 m`, or point density `4 p/m2` to `50 p/m2`; empty for 31,874 older NMT
sheets), mean height and position errors, CRS, vertical datum
(`PL-EVRF2007-NH`, `PL-KRON86-NH`), archive scale, submission number, partially filled flag, download link, data
source, PZGiK date and the footprint polygon (EPSG:2180).

Entity = one service's year (`<product>_<datum>_<YYYY>`), version = download time (the services give none), restart
unit one service's year: `ingestion/to_landing/gugik_nmt/` asks every year's hits count, lands missing years first,
then years whose count changed, and re-reads the current and previous year every 28 days; older years only when their
count changes. A year is read whole in pages of 5,000 sorted by the download link (unsorted pages are not stable, Dev
probe 45813550746 on the orthophoto index; sorted pages came back identical, 45814802349), checked (numberReturned,
the hits total, no feature id twice) and landed as `<version>__<ingested>__part-<nnn>.gml` with one receipt per page;
a year whose content equals the newest copy (SHA-256 without the answer's `timeStamp`) writes nothing. The services are
slow (about 30 s per sorted page), so the first load spreads over several runs within their budget.

Bronze `bronze.gugik_nmt_sheets` (entity table, design #875): every feature as published, the 16 properties as text,
the footprint's first posList and ring count, the page's SHA-256; a page that is not the year's feature collection or
an unknown property rejects the year. Silver `silver.gugik_nmt_sheets` (entity table): one row per feature (entity x
`feature_id` from the gml:id, unique) with product and service datum, typed, with the footprint centroid (EPSG:2180;
the GML lists northing before easting) and planar area in km² computed from the polygon.

Gold `build_gugik_nmt` (block `sheets`, A, `warehouse/macros/sources/gugik_nmt.sql`): one measure per service (NMT and
NMPT are different surfaces and the two datums are separate indexes, never summed): sheet editions (distinct sheet x
date x spatial characteristic, so the file formats of one edition count once) by data year in total and by spatial
characteristic as published, and the share of each characteristic (%), for Poland (every feature) and each voivodeship
(centroid inside a PRG gmina of it, `silver.prg_units`; outside every gmina or without a centroid: Poland only).
Covered area is not given: editions overlap, so a sum of footprints overstates it. No KPI (register `kpi: false`).

Licence: free use, "Dane NMT są dostępne bezpłatnie i możliwe do dowolnego wykorzystania"
(https://www.geoportal.gov.pl/pl/dane/numeryczny-model-terenu-nmt/), attribution "Źródło: GUGiK, NMT"
(`free_use_gugik_nmt`); the NMPT page says the same of NMPT ("Dane NMPT są dostępne bezpłatnie i możliwe do dowolnego
wykorzystania") and the LIDAR page of the point clouds ("Pliki chmury punktów w postaci LAS (LAZ) są dostępne
bezpłatnie i możliwe do dowolnego wykorzystania"), read by Dev trial 45873287115. Schedule: weekly, Saturday 02:50 UTC (O46).

Dev trial 45873287115 (code of d59de5b, scratch `zz_dev_gugik_nmt`, removed): Landing 63 service years, 368 pages,
3.83 GB in 99 minutes; a second pass landed nothing (63 current; two years read again: unchanged). Bronze and Silver
1,653,102 rows each (63 files; Bronze 127 MB, Silver 31 MB Parquet), second load wrote nothing, 0 rejected; entity x
feature_id unique, keys not null, every feature a one-ring Polygon. Gold 3,658 rows (6 services, 17 areas,
2000-2026), 0 duplicates by series x date, Poland totals equal the Silver edition count of every service year; 16,032
sheet editions lie in no gmina and count for Poland only.

Left out: the grids and point clouds themselves, the WMS/WMTS views (shaded relief, sheet grids) and the 5 m grid
sheet index (`SheetsGrid5mEVRF2007`, WMS only); an edit to an old year that keeps its count is seen only when the count
changes.
