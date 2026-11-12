"""World Bank WGI Landing → Bronze activities.

bronze.load       load.py → bronze.wgi_observations, bronze.wgi_metadata (ingestion/common/databank_bronze.py)
bronze.verify     ingestion/common/lineage.py --source world_bank_wgi
landing.archive   ingestion/common/archive.py --source world_bank_wgi (move to Archive)
archive.prune     ingestion/common/retention.py --source world_bank_wgi (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "world_bank_wgi"


def _load(p) -> int:
    from ingestion.to_bronze.world_bank_wgi import load

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
