"""GPW EBI Landing → Bronze activities.

bronze.load       the newest landed copy of every day → bronze.gpw_ebi_reports (one Parquet file per Landing file,
                  one row per report; shared decoder in ingestion/common/report_lists.py)
bronze.verify     ingestion/common/lineage.py --source gpw_ebi
landing.archive   ingestion/common/archive.py --source gpw_ebi (move to Archive)
archive.prune     ingestion/common/retention.py --source gpw_ebi (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "gpw_ebi"
TABLE = "gpw_ebi_reports"


def _load(p) -> int:
    from ingestion.common import report_lists as lists

    return lists.bronze_load(SOURCE, TABLE)


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
