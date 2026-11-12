"""PIG-PIB MIDAS Landing activities: the six WFS layers of the bulk_file declaration, one request at a time.

landing.extract  bulk_file load of every declared layer with a pause between requests (polite access to the
                 ArcGIS WFS, which publishes no limits); every response carries its request time, so every run
                 lands every layer (full_refresh, ingestion/sources/pig_midas.yaml)
landing.inspect, landing.verify  the shared bulk_file pattern (run.py)
"""

from __future__ import annotations

SOURCE = "pig_midas"
PAUSE_SECONDS = 10.0


def _extract(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "load", pause_seconds=PAUSE_SECONDS)


ACTIVITIES = {"landing.extract": _extract}
