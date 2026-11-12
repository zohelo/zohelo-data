# Wrocław public transport GTFS timetable (`gtfs_wroclaw`)

**Built (13 Nov 2026, register PL-ENV-032, #2980); city GTFS family, design in [gtfs_gdansk.md](gtfs_gdansk.md)
(shared module `ingestion/common/gtfs.py`).** Route: the listing https://open-data.cui.wroclaw.pl/hdb/ft/6/ of the
dataset "Rozkład jazdy transportu publicznego" (newest first); the first `/hdb/download/` link is the newest
snapshot, and the download id changes per snapshot, so the listing is the entry (one request for the page, one for
the file, 10 s apart). The old catalogue under www.wroclaw.pl/open-data is disallowed by robots.txt and not used.

**Newest only, history forward.** Each run lands the newest listed snapshot; earlier snapshots on the listing are
not back-loaded (their windows overlap and the newest one is what was in force when we read it). History
accumulates from the first run on, one validity window per entity, as for Gdańsk. Area Wrocław `TERC:0264011`.
Monday and Thursday 03:55 UTC (a new snapshot per timetable change). Restart unit: one download.

**Licence** `cc0_wroclaw_timetable`: CC0 1.0, named on the dataset's metadata page
https://open-data.cui.wroclaw.pl/hdb/metadane/13/.
