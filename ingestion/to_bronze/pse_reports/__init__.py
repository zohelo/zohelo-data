"""PSE Reports API v2 Landing → Bronze activities.

bronze.load       load.py → bronze.pse_reports_rce_pln and bronze.pse_reports_his_wlk_cal (one Parquet file per
                  Landing file, one row per 15-minute period, values as published text)
bronze.verify     ingestion/common/lineage.py --source pse_reports
landing.archive   ingestion/common/archive.py --source pse_reports (move to Archive)
archive.prune     ingestion/common/retention.py --source pse_reports (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "pse_reports"


def _load(p) -> int:
    from ingestion.to_bronze.pse_reports import load

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
