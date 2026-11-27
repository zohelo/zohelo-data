# SDDS advance release calendar (`sdds_calendar`)

Register TP-012 (NBP statistics publications) and TP-013 (Ministry of Finance budget and debt publications), Tasks
#3347 and #3348, route approved by #3346. nbp.pl answers runners with a bot-protection page (never worked around) and
no gov.pl page names an MF calendar; GUS, the SDDS national coordinator, publishes the official advance release
calendar of both as one workbook on https://stat.gov.pl/banki-i-bazy-danych/sdds/kalendarz-upowszechniania-danych
(robots.txt allows all). The Polish copy is read: the English copy is updated separately and its periods differ in at
least one cell. Code: `ingestion/to_landing/sdds_calendar/`, `ingestion/to_bronze/sdds_calendar/`.

Route: one GET of the page, then one GET of the workbook (about 27 KB), 2 s apart. The file name carries the calendar
year and the update date (`sdds_kalendarz_upowszechniania_danych_w_2026_r_25_11_2026.xlsx`), so the link is read on
every run. Entity = `calendar_<year>`; version = download time (no Last-Modified or ETag, Dev probe 45963679231); a copy
lands only when its SHA-256 changed. No page link, or two links for one year, fails the run.

Layout (2026 workbook of 25 Nov 2026): sheet "Spis tablic" (title "SDDS Kalendarz Upowszechniania Danych w 2026 r."),
then one sheet per table: Sektor podstawowy, Sektor podatkowo-budżetowy, Sektor finansowy, Sektor zagraniczny, Ludność.
Header row "Kategorie | Ostatnia aktualizacja | 01 2026 … 12 2026" (the fiscal table runs to "2 2027"); a category row
holds its last update (an Excel date) and per month "-", "dziennie" or one or two "DD.MM.YYYY (period)" releases, the
period "MM YYYY", "Qn YYYY" or "n kw. YYYY". Bronze `bronze.sdds_calendar_cells` keeps every cell
(`ingestion/common/xlsx_cells.py`) after a guard by sheet name: any other sheet, header, text or period rejects the
workbook into `bronze.sdds_calendar_rejected_files`.

Silver `silver.sdds_calendar_releases`: one row per release, grain workbook × category × reference period × release
date, publisher by category (closed list; a new category fails the not_null test until a person maps it). The file's
slips are kept and flagged in `check_status`, never corrected: in the 2026 workbook official reserves for 04 2026 dated
07.04.2026 (`before_period_end`), the international investment position and external debt for 4 kw. 2024 dated
31.03.2025 in the March 2026 column (`outside_column`), and a "Q3 2026" release among the monthly reserves
(`mixed_period_kind`).

Gold: entity table `gold.sdds_release` (NBP and MF releases, keyed by category × reference period × release date; each
release year from that year's workbook when held, else the newest one listing it) and build stage
`build_sdds_calendar`, block `releases`: expected releases per month of the release date (frequency M, Poland), in
total and by publisher, only `ok` rows; months not over at download are `provisional`. kpi false. Schedule: weekly,
Wednesday 06:40 UTC (O46). Licence: GUS copyright terms (https://stat.gov.pl/copyright), source required, commercial
reuse not stated (`gus_copyright_sdds`).

Left out: the GUS categories of the workbook (in Silver, not in Gold: the GUS kalendarium of `release_calendars`
covers GUS releases), daily categories (interest rates, exchange rates, WIG: no dated cells), MPC meeting dates (not
in this calendar; they stay in the manual seed `release_calendar.csv`), links from a release to KPIs and the topics
release bonus (`release_kpi` rows; the topics layer reads `silver.releases`), and calendars of years before the first
landed copy (the page lists only the current year).
