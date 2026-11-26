# IMGW-PIB public data API (`imgw_api`)

**Built (13 Nov 2026, register PL-ENV-008, #2959).** Route: https://danepubliczne.imgw.pl/api/data/ (no key, no
published limits), current state only. Landed through the shared bulk_file pattern as snapshots (version = download
time, an unchanged response is skipped): `synop` (62 synoptic stations, newest hourly observation, about 14 KB) after
each main synoptic term (00:05, 03:05, ..., 21:05 UTC slots, fetched about 20 minutes after the term; the 21:05 run is the full dataflow) and `meteo` (788 automatic
stations, about 520 KB) at most once a day for the station list (`ingestion/to_landing/imgw_api/`). Every snapshot is
loaded (accumulating Bronze: `bronze.imgw_api_synop`, `bronze.imgw_api_meteo`, values as sent). Silver
`silver.imgw_api_synop`: station (WMO index) x observation hour, the newest snapshot of each, values typed, null stays
null; `silver.imgw_api_stations`: IMGW 9-digit code, name, coordinates, altitude, founding year (no TERYT). Times are
UTC: the API names no zone, and on 13 Nov 2026 at 00:26 UTC it showed hour 0 of 13 Nov (Dev probe 43336937240).

Gold `build_imgw_api` (block `synop_daily`, D, status `provisional`): Poland daily mean air temperature, station day =
mean of the eight main synoptic terms (all eight required), Poland = mean of at least 30 stations; its own source series
next to the verified archive (O31). No row combines the two sources: each is its own series (`source_key`), so a
station-day is counted once in each source's series and never twice in one (the API series is not summed with or
used to fill the archive). Station link: API `id_stacji` (WMO index, e.g. 12375) = `12` + the last three digits of
the archive's 9-digit synoptic code (`api_synop_station_id` in `silver.imgw_archive_synop_stations`); the meteo
endpoint uses the archive's 9-digit codes directly. Poland only (the archive carries the voivodeship series).
Not landed: `/hydro/`, `/warningsmeteo`, `/warningshydro`, `/product`. Licence:
`unresolved` (as `imgw_archive`).
