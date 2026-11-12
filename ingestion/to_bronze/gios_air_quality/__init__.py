"""GIOŚ air quality Landing → Bronze → Silver activities.

bronze.load       load.py → bronze.gios_air_stations, bronze.gios_air_sensors, bronze.gios_air_measurements (one
                  Parquet file per Landing file), entities.py → silver.gios_air_hourly (a file per measurement day)
bronze.verify     ingestion/common/lineage.py --source gios_air_quality
landing.archive   ingestion/common/archive.py --source gios_air_quality (move to Archive)
archive.prune     ingestion/common/retention.py --source gios_air_quality (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "gios_air_quality"


def _load(p) -> int:
    from ingestion.to_bronze.gios_air_quality import load

    return load.load(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.common import lineage

    return lineage.main(["--source", SOURCE])


def _archive(p) -> int:
    from ingestion.common import archive

    return archive.main(["--source", SOURCE])


def _prune(p) -> int:
    from ingestion.common import retention

    return retention.main(["--source", SOURCE])


ACTIVITIES = {"bronze.load": _load, "bronze.verify": _verify, "landing.archive": _archive, "archive.prune": _prune}
