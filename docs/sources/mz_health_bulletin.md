# MZ health statistics bulletin (`mz_health_bulletin`)

**Built (register PLS-017, #2957).** Centrum e-Zdrowia publishes the Ministry of Health's "Biuletyn Statystyczny" on
https://ezdrowie.gov.pl/portal/home/badania-i-dane/biuletyn-statystyczny: every edition is a plain HTML attachment
link (no script or API), PDF for 2000-2025 (2000, 2004 and 2005 as RAR) and, from the 2019 edition, a tables workbook
"Biuletyn Statystyczny <year> tablice" (XLSX, about 0.3 MB, 56-61 sheets, one table per sheet) under
`/downloadFile/<id>` (2022-) or `/pobierz/biuletyn-statystyczny-<year>-tablice` (2019-2021). Route: the page's links
(`ingestion/to_landing/mz_health_bulletin/`); entity = one edition (`tablice_<edition year>`), version = download
time (no Last-Modified), written only when the SHA-256 changed; links not landed yet are downloaded, the newest
edition again on every run; restart unit: one file. The PDF editions are not landed: 2000-2018 exist only as PDF and
need PDF table extraction (a separate Task); 2019-2025 duplicate the XLSX tables.

Bronze `bronze.mz_health_bulletin_cells`: every non-empty cell of every sheet (`ingestion/common/xlsx_cells.py`), so
every table is kept. Silver `silver.mz_health_bulletin_sheets` records every sheet of every edition with its status
and reason: `modelled`, `not_modelled` (other tables) or `rejected` (a modelled table whose layout changed: no
header row, a column outside the closed list, not exactly Poland and the 16 voivodeships, no reference year); a
rejected sheet stays out of Silver and fails the status test, so schema drift between editions stops the build
instead of thinning a series. Silver `silver.mz_health_bulletin_tables` models four tables, found by title and header
row, never by sheet number (2020 numbers its sheets differently): health workers licensed to practise (doctors, dentists,
nurses, midwives), health workers employed in health care establishments (doctors, dentists, pharmacists without
pharmacies, laboratory diagnosticians, nurses, midwives, physiotherapists, paramedics, dietitians; persons by primary
workplace, MZ-88), general hospitals and beds, general hospital activity (MZ-29), each as count and rate per 10,000
population as published (D33). The reference year comes from the table titles (edition year minus one in every
edition so far; the 2019 edition misprints one title year, the majority of the kind's titles wins). Symbols `-`,
`.`, `x` are kept as status and never become zero. A reference year printed in two editions keeps both rows; Gold
takes the latest edition. Source gap: the 2023 edition has no table of licensed doctors and dentists (2022). Source
error: the 2021 edition's Tablica 2.2.4 (employed physiotherapists, paramedics, dietitians, titled 31.12.2020)
repeats the 2019 edition's 31.12.2018 table cell for cell, rates included (they fit the 2018 population). A sheet
that repeats an older edition's sheet with another reference year is marked `repeated` (value null, `repeated_from`
names it), so these three professions have no 2020 values in Gold.

Not yet modelled (in Bronze only): population, morbidity (venereal diseases, cancer, tuberculosis, infectious
diseases), licensed staff by age and sex, licensed pharmacists and laboratory diagnosticians (several years per
table; in the 2021 edition under the general title, recorded as `not_modelled`), staff time series and specialists,
ambulatory care staff, hospital staff, long-term care, blood donation, hospital beds and patients by ward, day care, diagnostic equipment, dialysis, obstetrics, finances of public health
care units; the national ranks ("miejsce w kraju") of the modelled tables.

Gold block `health_bulletin` (annual), registered in `warehouse/macros/sources/mz_health_bulletin.sql`; draft KPIs
`doctors_working`, `nurses_working`, `hospital_beds`, `hospital_inpatients`. Monthly check (9th). Licence:
`unresolved` (the portal footer reads "© 2026 Centrum e-Zdrowia. Wszystkie prawa zastrzeżone."; no reuse terms):
internal only.
