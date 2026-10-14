"""World Bank WDI Landing → Bronze activities.

bronze.load       load.py → bronze.wdi_bulk_observations, bronze.wdi_bulk_metadata
bronze.verify     ingestion/common/lineage.py --source world_bank_wdi
landing.archive   ingestion/common/archive.py --source world_bank_wdi (move to Archive)
archive.prune     ingestion/common/retention.py --source world_bank_wdi (archive_policy.retention)
"""

from __future__ import annotations


def _load(p) -> int:
    from ingestion.to_bronze.world_bank_wdi import load

    return load.load(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.common import lineage

    return lineage.main(["--source", "world_bank_wdi"])


def _archive(p) -> int:
    from ingestion.common import archive

    return archive.main(["--source", "world_bank_wdi"])


def _prune(p) -> int:
    from ingestion.common import retention

    return retention.main(["--source", "world_bank_wdi"])


ACTIVITIES = {"bronze.load": _load, "bronze.verify": _verify, "landing.archive": _archive, "archive.prune": _prune}
