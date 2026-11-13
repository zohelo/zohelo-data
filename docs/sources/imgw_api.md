# IMGW-PIB public data API (`imgw_api`)

**Built (13 Nov 2026, register PL-ENV-008, #2959).** Route: https://danepubliczne.imgw.pl/api/data/ (no key, no
published limits), current state only. Landed through the shared bulk_file pattern as snapshots (version = download
time, an unchanged response is skipped): `synop` (62 synoptic stations, newest hourly observation, about 14 KB) after
each main synoptic term (00:25, 03:25, ..., 21:25 UTC; the 21:25 run is the full dataflow) and `meteo` (788 automatic
stations, about 520 KB) at most once a day for the station list (`ingestion/to_landing/imgw_api/`). Every snapshot is
loaded (accumulating Bronze: `bronze.imgw_api_synop`, `bronze.imgw_api_meteo`, values as sent). Silver
`silver.imgw_api_synop`: station (WMO index) x observation hour, the newest snapshot of each, values typed, null stays
null; `silver.imgw_api_stations`: IMGW 9-digit code, name, coordinates, altitude, founding year (no TERYT). Times are
UTC: the API names no zone, and on 13 Nov 2026 at 00:26 UTC it showed hour 0 of 13 Nov (Dev probe 43336937240).

Gold `build_imgw_api` (block `synop_daily`, D, status `provisional`): Poland daily mean air temperature, station day =
mean of the eight main synoptic terms (all eight required), Poland = mean of at least 30 stations; its own source series
next to the verified archive (O31). Not landed: `/hydro/`, `/warningsmeteo`, `/warningshydro`, `/product`. Licence:
`unresolved` (as `imgw_archive`).
