# GUGiK KI EGiB, parcel and building register counts (`gugik_egib`)

**Built (26 Nov 2026, register PL-ENV-006, #3299).** Route: the aggregate WFS of the national integration of the
county land and building registers (`https://mapy.geoportal.gov.pl/wss/service/PZGIK/EGIB/WFS/UslugaZbiorcza`,
MapServer, WFS 2.0.0), which pulls the county EGiB WFS services daily. No feature and no geometry is read: only
`resultType=hits` counts (`numberMatched`). The feature types have no county attribute, so a powiat's count is a
`fes:PropertyIsLike` filter `<powiat TERYT>%` on the identifier, which starts with the cadastral unit code
(`ID_DZIALKI` for `ms:dzialki`, `ID_BUDYNKU` for `ms:budynki`, e.g. `146502_8.0615.1`). `NUMER_JEDNOSTKI` is not used:
it is empty for Warsaw (Dev probe 45898092158: `1465%` gives 0 by `NUMER_JEDNOSTKI`, 287,930 by `ID_DZIALKI`), and for
voivodeship 02 it gives 2,057,867 against 1,957,958 by `ID_DZIALKI`.

- Round (one snapshot of one feature type): the unfiltered national count, the 16 voivodeship counts (`<WW>%`) and the
  380 powiat counts, 397 requests, one at a time, 1 s apart, the platform User-Agent. The powiat list is the 380
  powiats of TERYT TERC (1 January 2026), as GUGiK's BDOT10k package index lists them (Dev probe 45897200062); it is a
  constant in `ingestion/to_landing/gugik_egib/`, so Landing depends on no warehouse table. A code outside the list is
  not lost: it is in the national and voivodeship counts and Gold reports it (`outside_powiat_list`).
- Cost at the server (Dev probes 45897200062, 45898092158, 26 Nov 2026): a parcel request takes about 40 s (37-53 s,
  the unfiltered one too), a building request 4-14 s. A parcel round takes about 4.5 hours, so a round is landed in
  17 parts (`PL` and each voivodeship with its powiats), each its own entity `<feature type>_<round date>_<part>`
  (one JSON Lines file: per request the parameters, URL, time and the answer's exact text with its SHA-256). A run
  first finishes the open round of each feature type, then starts a new one when the newest complete round is 6 days
  old or older (buildings first). Restart unit: one part. Round date = the day the round started.
- Bronze `bronze.gugik_egib_hits` (`ingestion/to_bronze/gugik_egib/load.py`): one row per request, numberMatched,
  numberReturned and timeStamp as text. A part with another line layout, an answer whose bytes do not match its
  SHA-256, an answer without numberMatched, or a code outside its part is rejected into
  `bronze.gugik_egib_rejected_files`. Every round is kept (accumulating).
- Silver `silver.gugik_egib_counts`: snapshot date x feature type x scope (PL, voivodeship, powiat) x TERYT code,
  complete rounds only (a round still being read waits for its remaining parts).
- Gold block models `gold.build_gugik_egib` (blocks `parcels`, `buildings`; `warehouse/macros/sources/gugik_egib.sql`),
  frequency D, date = the round date: `register_count` per powiat, voivodeship and Poland (voivodeship and Poland are
  the sum of their powiats; a powiat answered with 0, i.e. its service supplied nothing that day, is no observation and
  the sums without it are flagged `definition_differs`); `service_total` (Poland, every record of the service);
  `outside_powiat_list` (Poland: service total minus the powiat sum; voivodeship: its prefix count minus its powiats).
  **These are register counts as the county services supplied them on that day, not a census**: completeness
  differs by county. No KPI (register `kpi: false`).
- Schedule: weekly, Wednesday 01:25 UTC (O46), at night for the server. Refresh full (D32): about 800 rows a week.
- Licence: unresolved, internal (licence gate). The EGiB page
  `https://www.geoportal.gov.pl/pl/dane/ewidencja-gruntow-i-budynkow-egib/` states no licence and no reuse sentence;
  the WFS capabilities say Fees and AccessConstraints "none". Attribution if used: "Źródło: GUGiK, Krajowa Integracja
  Ewidencji Gruntów" with the download date.
- Dev trial 45920510130 (27 Nov 2026, scratch `zz_dev_gugik_egib`, removed): the building round, 17 parts and 397
  requests in about 47 minutes, 18 files and 626,595 bytes with the first parcel part; a second pass wrote nothing;
  inspect and verify green; Bronze 398 rows, second load appended nothing, 0 rejected; Silver 397 rows, unique grain,
  no null keys, completeness test empty; Gold 413 rows, 0 duplicates by series_key x date_key, 0 unmatched codes.
  Buildings: national 17,666,481 = the sum of the 380 powiats, 0 outside the list, every voivodeship's prefix
  count = the sum of its powiats; powiats 2479 and 3213 answered 0 (left out, Poland and voivodeships 24 and 32
  flagged). Parcels (national 38,847,375): voivodeship 02 = 1,957,958 = the sum of its 30 powiats (Dev trial
  45907973386); the full parcel round (about 4.5 hours) was not run in a trial.
- Left out: geometries and attributes (the geoportal "Pobierz dane" packages), the county WFS services themselves
  (the fallback route, not needed), gmina counts, land-use classes and areas.
