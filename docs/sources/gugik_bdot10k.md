# GUGiK BDOT10k, topographic objects database: package catalogue (`gugik_bdot10k`)

**Built (26 Nov 2026, register PL-ENV-003, #3097).** BDOT10k itself is heavy vector data (about 60 million objects;
GML/SHP/GPKG packages of ~90 MB a powiat and ~1.3 GB a voivodeship) and is **not downloaded**. Smallest valuable
selection: the catalogue of the powiat download packages. Route: GUGiK's WFS
`https://mapy.geoportal.gov.pl/wss/service/PZGIK/BDOT/WFS/PobieranieBDOT10k`, feature type `BDOT10k_powiaty` (the index
behind the "Dane do pobrania" layer of the BDOT10k page), read in one request, WFS 1.0.0, without geometry
(`PROPERTYNAME=TERYT,NAZWA_POWIATU,Data_aktualizacji,URL_GML`, ~144 KB, 380 features). WFS 1.0.0 because the 2.0.0
answer carries a `timeStamp` and would differ on every read; two 1.0.0 reads of an unchanged index are byte-identical.
Declared as one file of the shared `bulk_file` pattern (no extractor code; `probe: get`, the server sends no
Last-Modified). Entity = the index, version = download time, written only when the SHA-256 changed; restart unit one
file.

Bronze `bronze.bdot10k_packages` (`ingestion/to_bronze/gugik_bdot10k/load.py`): one row per feature, every value as
text, the feature's position and provenance; an answer that is not a `wfs:FeatureCollection` of `ms:BDOT10k_powiaty`
with exactly the four declared children (an exception report, a changed layout) is rejected into
`bronze.bdot10k_rejected_files`. Silver `silver.bdot10k_powiat_packages`: one row per package, grain `package_id` (the
powiat TERYT code; the file is `<package_id>_GML.zip`), powiat name, voivodeship code, update date and year
(`Data_aktualizacji`), GML URL, application schema year from the URL (`schemat2021`) and `snapshot_date` (the date the
index copy was read). The build fails on a TERYT that is not 4 digits, a date that is not a date, or a URL that does
not name the same TERYT.

Gold block `gold.build_gugik_bdot10k` (registered in `warehouse/macros/sources/gugik_bdot10k.sql`): each powiat package
counts once in Poland and once in its voivodeship, by the year of its current edition; date = `snapshot_date`
(frequency D). Measures `gugik_bdot10k:powiat_packages:packages:count` (breakdown `edition_year`, `_T` all years) and
`…:packages:share` (% of the area's packages). A package re-issued later moves to the newer year, so the rows describe
the age of the current editions on that date, not the number of packages issued in a year. KPI
`bdot10k_powiat_packages` (draft). Weekly check (Thursday, O46).

Licence: the BDOT10k page states "Baza BDOT10k jest dostępna bezpłatnie i możliwa do dowolnego wykorzystania" and
"uwolnienie danych BDOT10k powoduje, że dzisiaj każdy może w łatwy sposób wykorzystywać te dane do dowolnych celów"
(re-read 26 Nov 2026, Dev probe 45810649041); no licence name, attribution "Źródło: GUGiK, BDOT10k".

Left out: the packages themselves; the voivodeship packages (not in the WFS; the WMS
`…/PZGIK/BDOT/WMS/PobieranieBDOT10k` GetFeatureInfo lists them); package sizes (the index has none; a HEAD per package
would give `Content-Length` and `Last-Modified`); the 2011-schema, SHP and GPKG twins of each package; and the BDOT10k
statistics WFS (`…/PZGIK/BDOT10k/WFS/StatystykiBudynkow`, `StatystykiSieciKomunikacyjnej`, `StatystykiSieciWodnej`,
`StatystykiObiektowInnych`: object counts per class for every powiat and voivodeship, e.g. 2,229,037 buildings in
mazowieckie; no reference date; GetFeature returns features only with a `BBOX`), the natural next selection.
