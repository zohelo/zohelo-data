# ZTM Gdańsk GTFS timetable (`gtfs_gdansk`)

**Built (13 Nov 2026, register PL-ENV-029, #2977); city GTFS family with `gtfs_poznan`, `gtfs_krakow`,
`gtfs_wroclaw` on the shared module `ingestion/common/gtfs.py`.** Route: the fixed download URL of the CKAN dataset
"tristar" (`https://ckan.multimediagdansk.pl/dataset/c24aa637-3619-4dc2-a171-a23eec8f2172/resource/30e783e4-2bec-4a7d-bb22-ee3e3b26ca96/download/gtfsgoogle.zip`);
robots.txt asks for a crawl delay of 10 s and disallows the CKAN API, so the API is never called and requests are
10 s apart. Static GTFS only; GTFS-Realtime, shapes and stop_times rows are out of scope.

**Why one source per city.** Each city has its own producer, licence, schedule and register entry, and D34 keys a
source's files, seed rows and Gold registration by `source_system`; one source with a city key would put four
licences behind one `source.csv` row. The code is shared: Landing and Bronze in `ingestion/common/gtfs.py`, the
Silver and Gold SQL as macros in `warehouse/macros/sources/gtfs_gdansk.sql`, the generic tests in
`warehouse/tests/generic/gtfs.sql`; each city's files only name them.

**Entity and version.** The feed covers the next 14 days and the producer keeps no archive, so history accumulates
from our own copies: the entity is the feed and its validity window, `gtfs_<first>_<last>` (first and last service
date from calendar.txt and calendar_dates.txt), version = Last-Modified. Every window is the newest version of its
own entity, so Landing, the Archive and Bronze keep each one (latest-only per window); identical bytes land once, a
corrected file for the same window replaces it. Restart unit: one download. Weekly (Monday 03:15 UTC): a 14-day
window leaves a 7-day overlap, so one failed run loses no service day; every copy is about 25 MB of new bytes in the
Archive.

**Bronze** `bronze.gtfs_gdansk_records`: one row per record of agency, routes, stops, trips, calendar,
calendar_dates and feed_info (kept fields as text), and one row per trip of stop_times.txt with its number of
stop_times rows (about 2.3 M stop_times rows are never stored row by row). A ZIP without routes, stops, trips,
stop_times and a calendar file, a missing required column, or a code outside GTFS (weekday flag, exception type,
date, route type) is rejected into `bronze.gtfs_gdansk_rejected_files` with the reason.

**Silver.** `silver.gtfs_gdansk_stops` and `silver.gtfs_gdansk_routes` (feed × id, newest window);
`silver.gtfs_gdansk_service_days` (feed × service_date × route_type: trips, departures, routes). Service days are
calendar.txt weekday patterns plus calendar_dates.txt additions, minus its removals; a feed may carry either file.
**Which snapshot wins:** a day takes the newest snapshot downloaded on or before it (Warsaw date), so a day that has
passed never changes when a later window overlaps it and no day is counted twice; a day before the first download
takes the earliest snapshot.

**Gold.** Block `scheduled_trips` (`poc_build_gtfs_gdansk`, frequency D, unit `trips`, breakdown `route_type`
and a total), area Gdańsk `TERC:2261011` (the city whose authority publishes the feed; lines reach neighbouring
gminas); only days up to the newest download (a later day is still a plan and stays in Silver; it enters Gold once
a download on or after it has been seen). Draft KPI
`scheduled_transit_trips`. Entity tables `gold.transit_stops_gtfs_gdansk` and `gold.transit_routes_gtfs_gdansk`,
keyed `gtfs_gdansk:<feed>:<id>`.

**Licence** `cc_by_ztm_gdansk`: the dataset page names "Creative Commons Attribution" without a version; the
regulamin asks for the source "Zarząd Transportu Miejskiego w Gdańsku" and the dates of production and download.
The full regulamin is to be re-read before publication (register).
