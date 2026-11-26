"""ULC airport traffic statistics: Landing → Bronze activities (register PL-ENV-026).

bronze.load       load.py → bronze.ulc_airports_rows (one row per printed table row, PDF text layer)
bronze.verify     ingestion/common/lineage.py --source ulc_airports
landing.archive   ingestion/common/archive.py --source ulc_airports (move to Archive)
archive.prune     ingestion/common/retention.py --source ulc_airports (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "ulc_airports"


def _load(p) -> int:
    from ingestion.to_bronze.ulc_airports import load

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
