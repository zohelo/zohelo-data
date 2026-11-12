# ZTP Kraków GTFS timetables (`gtfs_krakow`)

**Built (13 Nov 2026, register PL-ENV-031, #2979); city GTFS family, design in [gtfs_gdansk.md](gtfs_gdansk.md)
(shared module `ingestion/common/gtfs.py`).** Route: the three ZIP files listed on https://gtfs.ztp.krakow.pl/,
`GTFS_KRK_A.zip`, `GTFS_KRK_M.zip` and `GTFS_KRK_T.zip`, each a feed of its own (version = Last-Modified), 10 s apart.

Entity `<feed>_<first>_<last>` per feed (validity window); Bronze, Silver, snapshot choice and Gold as for Gdańsk.
The city total sums the three feeds and is loaded only for days all three cover. Area Kraków `TERC:1261011`.
Monday and Thursday 03:45 UTC. Restart unit: one download.

**Licence: unresolved** (licence key `unresolved`): the feed page states no licence and the Kraków open-data
portal terms were not read, so the source is built and stays internal (licence gate, O37).
