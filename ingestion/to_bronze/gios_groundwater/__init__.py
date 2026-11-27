"""GIOŚ groundwater monitoring results: Landing → Bronze activities (register PL-ENV-012, #2870).

Landing is ingestion/to_landing/gios_groundwater/ (the result files of dane.gov.pl dataset 1405, bulk_file pattern).

bronze.load       load.py → bronze.gios_groundwater_records (one Parquet file per Landing file); rejections →
                  bronze.gios_groundwater_rejected_files
bronze.verify     ingestion/common/lineage.py --source gios_groundwater
landing.archive   ingestion/common/archive.py --source gios_groundwater (move to Archive)
archive.prune     ingestion/common/retention.py --source gios_groundwater (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "gios_groundwater"


def _load(p) -> int:
    from ingestion.to_bronze.gios_groundwater import load

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
