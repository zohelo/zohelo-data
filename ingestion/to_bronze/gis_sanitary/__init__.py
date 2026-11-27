"""GIS sanitary report (PLS-018) Landing → Bronze activities.

bronze.load       load.py → bronze.gis_sanitary_cells (one Parquet file per Landing file;
                  rejections → bronze.gis_sanitary_rejected_files)
bronze.verify     ingestion/common/lineage.py --source gis_sanitary
landing.archive   ingestion/common/archive.py --source gis_sanitary (move to Archive)
archive.prune     ingestion/common/retention.py --source gis_sanitary (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "gis_sanitary"


def _load(p) -> int:
    from ingestion.to_bronze.gis_sanitary import load

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
