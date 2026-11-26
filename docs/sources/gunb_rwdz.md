# GUNB RWDZ: building-permit register (`gunb_rwdz`)

**Built (Nov 2026, register PL-ENV-033, #2984).** Route: the producer's bulk files on
https://wyszukiwarka.gunb.gov.pl/pobranie.html, one ZIP per voivodeship at a fixed URL
(`pliki_pobranie/wynik_<voivodeship>.zip`, 7-47 MB, rebuilt every night about 22:35 UTC, Last-Modified sent), through
the shared `bulk_file` pattern; preferred over the GUGiK WFS on the same data (GML pages, paging not transaction-safe).
Each ZIP holds one CSV: `;`-separated, UTF-8 with BOM, LF, `"` quotes, a fixed 31-column header (`cecha` and `ulica`
twice), every application with its decision since 2016 for that voivodeship, one line per plot of the works.

Entity: one voivodeship file (16). Version: Last-Modified; an unchanged file (same SHA-256) is skipped. Entity tables
(design #875, as GLEIF): `ingestion/to_bronze/gunb_rwdz/entities.py` decodes the newest copy of each file into
`bronze.gunb_rwdz_records` (one row per line) and applies `warehouse/entity/gunb_rwdz_records.sql` into
`silver.gunb_rwdz_records`, replacing that voivodeship's file in both. A file with another header, an unreadable line, a
voivodeship other than the file's, a TERYT code that is not 7 digits, an unreadable or missing date or an unknown kind of
investment or object category is rejected into `bronze.gunb_rwdz_rejected_files` and the tables keep the previous file.

Personal data: Bronze keeps only `numer_urzad`, `numer_gunb`, `nazwa_organu` (a public office), the application and
decision dates, `numer_decyzji_urzedu`, `wojewodztwo`, `terc`, `rodzaj_inwestycji`, `kategoria` and `kubatura`. The
authority's address, the investor's name, town, street, house number, the free-text name of the works, the designer's
name, first name and licence number and every plot and precinct number are never read into a table; parser messages
are cut before the quoted line. The raw files in Landing and the Archive hold them as GUNB publishes them (Archive:
newest file plus 14 days).

Silver `silver.gunb_rwdz_records`, the register's entity table: one row per register record (`record_id` =
`numer_gunb`) of each file, with `source_lines` and, where the lines of one record disagree (a few records per file),
the column left null and named in `conflicting_columns` (no line is picked). Gold `gold.build_gunb_rwdz`, block
`permits`: measure `gunb_rwdz:permits:decisions` (unit `decisions`), register records by month of decision from 2020-01
to the month before the oldest file version, Poland, 16 voivodeships (file) and powiats (`TERC` of the record), by
`investment_kind` (`jednorodzinny`, `inny`, `_U`) and `_T`. Records without a TERYT code (155,516 of 2,875,574)
count for Poland and the voivodeship only; six published codes that are no TERYT powiat (0100, 0251, 0396, 0737,
0860, 0934) stay `_X` with the code kept. KPI `building_permit_decisions` (draft).

Dev trial 43804193304 (16 Nov 2026): 16 files, 328,946,600 bytes; Bronze 5,170,755 lines, Silver 2,875,574 records
(no register number appears in two files; 65 records with conflicting lines), second Landing and loader runs changed
nothing; Gold 96,571 rows, 1,200 series, 2020-01 to 2026-10, 0 duplicates by series × month. Poland: 22,088 decisions
in 2020-01, 14,018 in 2025-01, 201,231 in 2025 (112,430 single-family houses).

Left out: the two national files of notifications (`wynik_zgloszenia_2016_2021.zip`, `wynik_zgloszenia_2022_up.zip`;
other header, about 1 M lines since 2022); a count of applications by month of filing (the files hold applications
with a decision only, so recent filing months would rise as decisions follow); months before 2020 in Gold (in Silver);
gmina-level counts; an entity table in Gold (the Silver table is it). Terms: none stated on pobranie.html, the home page,
the WFS capabilities or the GUGiK announcement: licence `unresolved`, internal. Refresh incremental by file (D32),
weekly, Monday 04:20 UTC. Restart unit: one voivodeship file.
