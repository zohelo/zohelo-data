"""RSPO school lists: Landing → Bronze activities (register PLS-021).

Landing is ingestion/to_landing/rspo/ (the yearly lists of dane.gov.pl dataset 839, through the bulk_file pattern).

bronze.load       load.py → bronze.rspo_institutions (one Parquet file per Landing file, allowlisted columns);
                  rejections → bronze.rspo_rejected_files
bronze.verify     ingestion/common/lineage.py --source rspo
landing.archive   ingestion/common/archive.py --source rspo (move to Archive)
archive.prune     ingestion/common/retention.py --source rspo (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "rspo"


def _load(p) -> int:
    from ingestion.to_bronze.rspo import load

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
