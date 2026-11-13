"""NFZ pharmacy refunds Landing → Bronze activities.

bronze.load       load.py → bronze.nfz_ra_monthly_provisions (one Parquet file per Landing file, i.e. per page)
bronze.verify     ingestion/common/lineage.py --source nfz_pharmacy_refunds
landing.archive   ingestion/common/archive.py --source nfz_pharmacy_refunds (move to Archive)
archive.prune     ingestion/common/retention.py --source nfz_pharmacy_refunds (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "nfz_pharmacy_refunds"


def _load(p) -> int:
    from ingestion.to_bronze.nfz_pharmacy_refunds import load

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
