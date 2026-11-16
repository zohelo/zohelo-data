"""TED notices (Poland) Landing → Bronze activities.

bronze.load       load.py → bronze.ted_notices (one Parquet file per Landing file, one row per notice)
bronze.verify     ingestion/common/lineage.py --source ted_notices
landing.archive   ingestion/common/archive.py --source ted_notices (move to Archive)
archive.prune     ingestion/common/retention.py --source ted_notices (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "ted_notices"


def _load(p) -> int:
    from ingestion.to_bronze.ted_notices import load

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
