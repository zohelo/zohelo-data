# OTOMOTO market report (`otomoto`)

**Built (26 Nov 2026, register CD-034, #3264; route decision D41).** Raport Rynkowy OTOMOTO is the monthly market
report of OTOMOTO Insights (OTOMOTO, Grupa OLX). The report page https://www.otomoto.pl/news/raport-rynkowy-otomoto
links each month's edition, a public Infogram dashboard (August 2026:
https://infogram.com/0826otomotoinsights-1hxj48mj9m8o52v) whose HTML carries the whole report as JSON
(`window.infographicData`: 12 tabs, 27 charts, each chart with its data table in `chartData`).

Route (D41; `ingestion/to_landing/otomoto/`, shared bulk_file pattern): one request per edition on infogram.com, 5 s
apart; robots.txt of infogram.com disallows only `/app/` and `/oembed`. www.otomoto.pl refuses the runner (HTTP 403
on robots.txt, Dev probe 43291903826) and is never read, so the address of each edition is a seed row of
`ingestion/sources/otomoto.yaml` (`entity: edition_YYYY_MM`, `edition: "YYYY-MM"`), added from a desk reading of the
report page. `landing.inspect` and `landing.extract` warn "edition expected but not in the seed list" once a month
ended 45 days ago without a row; nothing is lost by a late row, because each edition repeats the months of the
current and the previous year. Entity = one edition; version = download time (no Last-Modified; the weak ETag changes
on every request). The page differs on every request outside the JSON (a per-visitor cookie id, a time stamp on the
preview image; Dev probe 45813576806), so Landing keeps the JSON object of the statement only, byte for byte
(`reduce: otomoto`); a copy lands only when its SHA-256 changed (an edition is edited after release: August 2026 was
created on 9 Sep and changed on 17 Nov). Restart unit: one edition.

Bronze `bronze.otomoto_cells` (`ingestion/to_bronze/otomoto/load.py`): one row per non-empty chart cell (tab, chart
element id, sheet, row, column, value as text) and one per short text (at most 120 characters) of a tab that holds a
chart (headings, chart titles); commentary and the contact tab (it names OTOMOTO staff) are not stored. The report's
title names the edition (`08_26_OTOMOTO_INSIGHTS`) and must match the seed row, else the file is rejected into
`bronze.otomoto_rejected_files`.

Silver `silver.otomoto_series` (measure x segment x month): the segment is the tab heading "Ogłoszenia na OTOMOTO
(<segment>)": Samochody używane, Samochody nowe, Motocykle, Maszyny ciężkie.

- `active_listings`: the chart titled "Średnia dzienna liczba aktywnych ogłoszeń" (average daily active listings),
  header "rok | 2025 | 2026 | r/r", months as `sty` ... `gru`; the r/r column is a derived change and is not read.
- `users_index`, `buyers_index`: the chart titled "Statystyki ruchu (jako % okresu bazowego 01.2025)", header
  "Miesiąc | Miesięczna liczba użytkowników | Miesięczna liczba kupujących", months as YYYY-MM, January 2025 = 100.
  New cars and motorcycles have both, heavy machinery users only, used cars no traffic chart.

The newest edition that holds a month wins (the edition is the version); a value that is not a number keeps `value`
null (`not_numeric`), never 0; empty cells are no value. The completeness test checks the newest edition's month in
every segment, no gap in any series, and the index base of 100 in January 2025. Gold block `insights`
(`warehouse/macros/sources/otomoto.sql`): measures `otomoto:insights:listings:active_listings`,
`otomoto:insights:jan2025_100:users_index` and `...:buyers_index`, segment in `breakdown_json`, Poland, frequency M,
values and units as published (units `listings` and `index_2025m01_100`, D33). No KPI (register `kpi: false`).

Left out (next steps, same JSON): CEPiK registrations by kind (used, imported, new) and owner (private, company)
(the table the register first read as listings; registrations are loaded from CEPiK, PL-ENV-024), median and
average asking prices (including a map by voivodeship), price-band, fuel and body shares, listing duration by seller
type, traffic totals (visits, daily users), user profile and search filters, and the macroeconomic tab (GUS and NBP
data). The editions before August 2026: their addresses are on the refused report page only.

Monthly schedule (27th, 07:35 UTC; one edition a month).

Licence: `unresolved` (no terms of reuse found). The edition says "Wszystkie wizualizacje należą do Grupy OLX i są
przygotowywane przez zespół analityków biznesowych" and "Wszelkie informacje i dane zawarte w raporcie mają charakter
informacyjno-edukacyjny"; the annual PDF carries "© OTOMOTO INSIGHTS". The source stays internal (licence gate);
publication waits for the owner's question to Grupa OLX under CD-033. Working attribution: "Źródło: OTOMOTO Insights,
Raport Rynkowy OTOMOTO".
