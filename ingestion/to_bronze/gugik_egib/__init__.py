"""GUGiK KI EGiB Landing → Bronze activities (register PL-ENV-006).

Landing is ingestion/to_landing/gugik_egib/ (one JSON Lines file of hits answers per feature type and run date).

bronze.load       load.py → bronze.gugik_egib_hits (one Parquet file per Landing file, every snapshot kept);
                  rejections → bronze.gugik_egib_rejected_files
bronze.verify     ingestion/common/lineage.py --source gugik_egib
landing.archive   ingestion/common/archive.py --source gugik_egib (move to Archive)
archive.prune     ingestion/common/retention.py --source gugik_egib (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "gugik_egib"


def _load(p) -> int:
    from ingestion.to_bronze.gugik_egib import load

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
