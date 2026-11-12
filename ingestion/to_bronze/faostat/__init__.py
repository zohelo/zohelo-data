"""FAOSTAT Landing → Bronze activities.

bronze.load       load.py → bronze.faostat_observations (one Parquet file per Landing file)
bronze.verify     ingestion/common/lineage.py --source faostat
landing.archive   ingestion/common/archive.py --source faostat (move to Archive)
archive.prune     ingestion/common/retention.py --source faostat (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "faostat"


def _load(p) -> int:
    from ingestion.to_bronze.faostat import load

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
