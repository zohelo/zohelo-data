# ZTM Poznań GTFS timetable (`gtfs_poznan`)

**Built (13 Nov 2026, register PL-ENV-030, #2978); city GTFS family, design in [gtfs_gdansk.md](gtfs_gdansk.md)
(shared module `ingestion/common/gtfs.py`).** Route: the fixed URL `https://www.ztm.poznan.pl/pl/dla-deweloperow/getGTFSFile`
(attachment named after its validity period, e.g. `20261112_20261127.zip`; no Last-Modified, so version = download
time and an unchanged file is skipped by its SHA-256). robots.txt disallows only `/wp-admin/`. The portal's list of
earlier files (`/otwarte-dane/gtfsfiles/`) is not back-loaded: history accumulates from the first run on.

Entity `gtfs_<first>_<last>` (validity window), Bronze, Silver, snapshot choice and Gold as for Gdańsk; area Poznań
`TERC:3064011`. Monday and Thursday 03:25 UTC (a new file per timetable period, about two weeks ahead). Restart
unit: one download.

**Licence** `ztm_poznan_open_data`: https://www.ztm.poznan.pl/otwarte-dane/dla-deweloperow/, "Zasady
udostępniania": free reuse on condition of naming the source, the date of the last update and that the data were
processed.
