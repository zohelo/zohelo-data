"""KPRM work list (PLG-034) Landing → Bronze activities.

bronze.load       load.py → bronze.kprm_work_list_drafts (one Parquet file per Landing file)
bronze.verify     ingestion/common/lineage.py --source kprm_work_list
landing.archive   ingestion/common/archive.py --source kprm_work_list (move to Archive)
archive.prune     ingestion/common/retention.py --source kprm_work_list (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "kprm_work_list"


def _load(p) -> int:
    from ingestion.to_bronze.kprm_work_list import load

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
