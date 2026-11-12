"""UNdata Landing → Bronze activities.

bronze.load       load.py → bronze.undata_syb_values (one Parquet file per Landing file)
bronze.verify     ingestion/common/lineage.py --source undata
landing.archive   ingestion/common/archive.py --source undata (move to Archive)
archive.prune     ingestion/common/retention.py --source undata (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "undata"


def _load(p) -> int:
    from ingestion.to_bronze.undata import load

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
