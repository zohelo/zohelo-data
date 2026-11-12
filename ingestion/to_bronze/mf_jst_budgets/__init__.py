"""Ministry of Finance local-government budget summaries Landing → Bronze activities.

bronze.load       load.py → bronze.mf_jst_budgets_cells (one Parquet file per Landing file)
bronze.verify     ingestion/common/lineage.py --source mf_jst_budgets
landing.archive   ingestion/common/archive.py --source mf_jst_budgets (move to Archive)
archive.prune     ingestion/common/retention.py --source mf_jst_budgets (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "mf_jst_budgets"


def _load(p) -> int:
    from ingestion.to_bronze.mf_jst_budgets import load

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
