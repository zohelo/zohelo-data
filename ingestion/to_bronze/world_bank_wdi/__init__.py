"""World Bank WDI Landing → Bronze activities (Bronze itself was built by retired code).

bronze.verify     ingestion/common/lineage.py --source world_bank_wdi
landing.archive   ingestion/common/archive.py --source world_bank_wdi
"""
from __future__ import annotations


def _verify(p) -> int:
    from ingestion.common import lineage
    return lineage.main(["--source", "world_bank_wdi"])


def _archive(p) -> int:
    from ingestion.common import archive
    return archive.main(["--source", "world_bank_wdi"])


ACTIVITIES = {"bronze.verify": _verify, "landing.archive": _archive}
