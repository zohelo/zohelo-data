"""GUGiK BDOT10k Landing → Bronze activities (register PL-ENV-003).

Landing is the shared bulk_file pattern (the WFS index of the powiat download packages).

bronze.load       load.py → bronze.bdot10k_packages (one Parquet file per Landing file);
                  rejections → bronze.bdot10k_rejected_files
bronze.verify     ingestion/common/lineage.py --source gugik_bdot10k
landing.archive   ingestion/common/archive.py --source gugik_bdot10k (move to Archive)
archive.prune     ingestion/common/retention.py --source gugik_bdot10k (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "gugik_bdot10k"


def _load(p) -> int:
    from ingestion.to_bronze.gugik_bdot10k import load

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
