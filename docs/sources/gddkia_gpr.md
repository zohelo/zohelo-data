# GDDKiA traffic census GPR (`gddkia_gpr`)

**Built (Nov 2026, register PL-ENV-022, #2999).** GDDKiA's Generalny Pomiar Ruchu (road traffic census, every five
years) has one gov.pl page per edition: https://www.gov.pl/web/gddkia/generalny-pomiar-ruchu-2025 (XLSX) and
https://www.gov.pl/web/gddkia/generalny-pomiar-ruchu-20202021 (XLS), attachments at `https://www.gov.pl/attachment/<uuid>`
(a replaced file gets a new uuid). Landed (`ingestion/to_landing/gddkia_gpr/`): table 2, average annual daily traffic
(SDRR) per measured section and vehicle class, of national roads (dk) and voivodeship roads (dw) of both editions, and
the 2020/21 dk erratum (21 sections). A file is identified by the file name the server sends (the 2020/21 links are
labelled only "XLS"); each declared name pattern must match exactly one link, else Landing fails. Entity = one file
(`gpr<edition>_<dk|dw>_sdrr[_errata]`), version = Last-Modified, a copy lands only when the SHA-256 changed; restart
unit one file. Bronze `bronze.gddkia_gpr_cells`: one row per workbook cell (XLSX through `xlsx_cells`, XLS through
xlrd). Silver `silver.gddkia_gpr_sdrr`: edition × road class × section × vehicle class, columns found by header text,
`-` = not measured (no value), the erratum replaces its sections. Gold block `sdrr`
(`warehouse/macros/sources/gddkia_gpr.sql`): length-weighted SDRR of motor vehicles and of trucks and the measured
length, per road class, Poland and voivodeships (the files' "Nr woj." = TERYT code), date = 1 January of the edition
year (2020 for 2020/21); KPI `average_daily_traffic_national_roads` (draft). Checked twice a year. Licence
`unresolved` (gov.pl states no terms for the attachments; owner question in the register), so nothing reaches a
published file. Not loaded: table 1 (section list, coordinates), SDRDR, SDRL, traffic character, SHP, maps,
syntheses, editions before 2020/21 (archive site, not reachable from runners).
