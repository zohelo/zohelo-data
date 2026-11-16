"""e-Zamówienia BZP Landing → Bronze activities.

bronze.load       load.py → bronze.uzp_bzp_notices (one Parquet file per Landing file, one row per notice)
bronze.verify     ingestion/common/lineage.py --source uzp_bzp
landing.archive   ingestion/common/archive.py --source uzp_bzp (move to Archive)
archive.prune     ingestion/common/retention.py --source uzp_bzp (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "uzp_bzp"


def _load(p) -> int:
    from ingestion.to_bronze.uzp_bzp import load

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
