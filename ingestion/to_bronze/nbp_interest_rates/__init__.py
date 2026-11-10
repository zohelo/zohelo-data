"""NBP interest rates Landing → Bronze activities.

bronze.load       load.py → bronze.nbp_interest_rate_items (one Parquet file per Landing file)
bronze.verify     ingestion/common/lineage.py --source nbp_interest_rates
landing.archive   ingestion/common/archive.py --source nbp_interest_rates (move to Archive)
archive.prune     ingestion/common/retention.py --source nbp_interest_rates (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "nbp_interest_rates"


def _load(p) -> int:
    from ingestion.to_bronze.nbp_interest_rates import load

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
