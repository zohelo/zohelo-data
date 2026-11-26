# Official release calendars (`release_calendars`, topics)

**Implemented (10 Nov 2026, register TP-010, TP-011, TP-014; NBP TP-012 and MF TP-013 parked; topics layer
O32).** Dev probes 42784902820, 42785648233, 42786020672, 42789309243. Eurostat: the documented ICS address
(`cache/RELEASE_CALENDAR/calendar_EN.ics`) answers 404; the euro-indicators calendar page loads its events from
`https://ec.europa.eu/eurostat/o/calendars/eventsJson?start=…&end=…&isEuroindicator=true` (JSON with title,
reference period, dataset codes), landed for 14 days back to 90 days ahead. GUS: no machine-readable calendar
(month and year calendars are HTML and PDF); the `kalendarium` day pages (`stat.gov.pl/kalendarium/DD-MM-YYYY.html`,
"Publikacje GUS" list: title and link), 7 days back to 45 ahead, 1 s apart. ECB: statistical calendar and
Governing Council meeting calendar (HTML; robots.txt Crawl-delay 5, respected). NBP (nbp.pl) answers automated
clients with a bot-protection page and www.gov.pl (MF) refuses connections from GitHub runners: both parked; the
seed `release_calendar.csv` stays the manual fallback. Files are built per run by
`ingestion/to_landing/release_calendars/`, landed through `bulk_file`; Bronze `bronze.release_calendar_items`
(newest copy of each file, latest-only); Silver `silver.releases` with `kpi_keys` (seed `release_kpi.csv` by
title, Eurostat also by dataset code against `kpi_v2_map`).
