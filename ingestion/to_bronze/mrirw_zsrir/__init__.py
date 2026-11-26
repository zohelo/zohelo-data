"""MRiRW ZSRIR market bulletins: Landing → Bronze activities (register PLS-032).

bronze.load       load.py → bronze.mrirw_zsrir_cells (one Parquet file per Landing file);
                  rejections → bronze.mrirw_zsrir_rejected_files
bronze.verify     ingestion/common/lineage.py --source mrirw_zsrir
landing.archive   ingestion/common/archive.py --source mrirw_zsrir (move to Archive)
archive.prune     ingestion/common/retention.py --source mrirw_zsrir (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "mrirw_zsrir"


def _load(p) -> int:
    from ingestion.to_bronze.mrirw_zsrir import load

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
