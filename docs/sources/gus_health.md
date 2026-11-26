# GUS health and health care tables (`gus_health`)

**Built (26 Nov 2026, register PLS-004 health family, #2451).** stat.gov.pl publishes each edition of "Zdrowie i
ochrona zdrowia w <year> r." on its own page with a tables file: a ZIP of chapter workbooks for 2012-2015, one XLSX
workbook from 2016 (link texts "... tablice", "Tablice w formacie XLSX", "Aneks tabelaryczny w formacie XLSX"; in 2012
only the edition title). Route: the health subject page links the newest edition, every edition page links the
others (`ingestion/common/gus_publications.py`, editions from 2012). Entity = one edition (`zdrowie_<year>`), version
= download time (no Last-Modified) when the SHA-256 changed; the newest edition is downloaded again on every run;
restart unit: one edition. Bronze `bronze.gus_health_cells` (every cell of every workbook, with the ZIP member).

Silver `silver.gus_health_tables`: doctors and nurses on 31 December ("Pracownicy medyczni według województw"),
general hospitals, beds, beds per 10,000 population and inpatients ("Szpitale ogólne według województw"), Poland and
voivodeships, found by title and column header (`warehouse/macros/gus_tables.sql`), never by cell position; GUS
symbols kept as `cell_status` (`-` without a value, never 0); the sheet title and footnotes kept as printed. Doctors
and nurses up to 2020 are separate measures (`*_pre2021`): GUS marks the 2021 edition's personnel data as not
comparable with earlier editions. Each edition gives its own reference year; the previous year that an edition
reprints (sometimes revised, marked `*`) is not taken. Gold block `health_care_resources`, registered in
`warehouse/macros/sources/gus_health.sql`; the source is added to the KPIs `doctors_working`, `nurses_working` and
`hospital_beds` (O31). Monthly check, first Tuesday 08:15 UTC. Licence: GUS terms (source given; commercial reuse
not addressed; no named licence, D23); the tables name the Ministry of Health and the Ministry of the Interior as
data sources.

Left out: the other tables of the publication (licensed personnel, specialists, outpatient care, emergency care,
pharmacies, health accounts), inpatients per 10,000 population, beds for children; they are in Bronze.
