"""PIG-PIB MIDAS Landing → Bronze activities (register PL-ENV-015).

Landing is the shared bulk_file pattern (one WFS GetFeature response per layer).

bronze.load       load.py → bronze.pig_midas_features (one Parquet file per Landing file);
                  rejections → bronze.pig_midas_rejected_files
bronze.verify     ingestion/common/lineage.py --source pig_midas
landing.archive   ingestion/common/archive.py --source pig_midas (move to Archive)
archive.prune     ingestion/common/retention.py --source pig_midas (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "pig_midas"


def _load(p) -> int:
    from ingestion.to_bronze.pig_midas import load

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
