"""OECD Landing → Bronze activities.

bronze.load       load.py → bronze.oecd_observations (one Parquet file per Landing file)
bronze.verify     ingestion/common/lineage.py --source oecd
landing.archive   ingestion/common/archive.py --source oecd (move to Archive)
archive.prune     ingestion/common/retention.py --source oecd (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "oecd"


def _load(p) -> int:
    from ingestion.to_bronze.oecd import load

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
