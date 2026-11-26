"""ZTP Kraków GTFS (register PL-ENV-031): Landing activities, ingestion/common/gtfs.py.

landing.inspect  feed URLs (HEAD) vs the windows landed; no writes
landing.extract  download each feed once; land it under <feed>_<first>_<last> unless those bytes are held
landing.verify   every receipt of every window has its file (Landing or Archive)
"""

from __future__ import annotations

from ingestion.common import gtfs

ACTIVITIES = gtfs.landing_activities("gtfs_krakow")
