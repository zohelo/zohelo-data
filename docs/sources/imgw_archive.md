# IMGW-PIB observation archive (`imgw_archive`)

**Built (13 Nov 2026, register PL-ENV-009, #2959).** Route: the public directory archive
https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/ (Apache index, no robots.txt), bulk ZIP files, no key.
Scope loaded: the daily synoptic station files `dane_meteorologiczne/dobowe/synop/` from 1966 (five-year folders until
2000, one ZIP per station and year 2001 to last year, one ZIP per month in the current year; 57-63 stations a year).
Entity = one ZIP (`synop_daily_<period>_<station>`, `synop_daily_<year>_<month>`), version = Last-Modified, restart unit
one ZIP: `ingestion/to_landing/imgw_archive/` lists the 35 index pages, lands missing files first (newest periods
first) and files the index shows as modified later (IMGW replaces corrected files, `lista_zmian.txt`), within the run's
budget. Bronze `bronze.imgw_archive_synop_daily`: the `s_d` member as published (65 fields, Windows-1250, header names
of `s_d_nagłówek.csv`); Silver `silver.imgw_archive_synop_daily` (entity table, design #875): station x day x
parameter for TMAX, TMIN, STD, TMNG (°C), SMDB (mm), PKSN (cm), USL (h), IMGW status kept (blank measured, `8` no
measurement, `9` phenomenon absent; older files write 0 with status 8/9, so a value is kept only for a measured
observation). When a year closes, its monthly files are superseded by the station files and leave Bronze and Silver.
Station dimension `silver.imgw_archive_synop_stations` (IMGW 9-digit code, newest name, first/last day; coordinates
where the API station list has the code; no TERYT, IMGW gives none). Dates are observation days as published (no time
of day in the files).

Gold `build_imgw_archive` (blocks `synop_daily` D and `synop_monthly` M, `warehouse/macros/sources/imgw_archive.sql`):
Poland only (no station-to-voivodeship key from IMGW): daily mean air temperature = mean of the station daily means;
monthly mean air temperature = mean over stations with every day measured; monthly precipitation = mean of the station
monthly sums over stations with every day measured or "phenomenon absent" (0 mm); at least 30 stations. Draft KPIs
`air_temperature`, `precipitation`. Schedule: 9th and 23rd of the month, 06:40 UTC.

Not loaded yet (same archive, other layouts): daily klimat and opad files (one ZIP per month since 2001), the term
(hourly) files, the monthly files (incomplete since mid-2024, `UWAGA!.txt`), the `s_d_t` member (daily means of the
terms, until mid-2024), the hydrological files (daily since 1951, hydrological years) and the actinometric files
(1990-2017). Licence: `unresolved` (IMGW Regulamin §5: free use for private purposes, any purpose only for high-value
data; owner question in the register).
