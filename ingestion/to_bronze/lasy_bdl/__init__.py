"""Lasy Państwowe BDL Landing → Bronze activities (register PL-ENV-018).

Landing is the shared bulk_file pattern (one WFS GetFeature response per layer: districts, regions).

bronze.load       load.py → bronze.lasy_bdl_features (one Parquet file per Landing file);
                  rejections → bronze.lasy_bdl_rejected_files
bronze.verify     ingestion/common/lineage.py --source lasy_bdl
landing.archive   ingestion/common/archive.py --source lasy_bdl (move to Archive)
archive.prune     ingestion/common/retention.py --source lasy_bdl (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "lasy_bdl"


def _load(p) -> int:
    from ingestion.to_bronze.lasy_bdl import load

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
