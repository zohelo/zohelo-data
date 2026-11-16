"""RPWDL Landing → Bronze activities (register PLS-020).

bronze.load       load.py → bronze.rpwdl_providers, _establishments, _units, _cells, _practices (one Parquet file per
                  Landing file and table; allowlisted columns only)
bronze.verify     ingestion/common/lineage.py --source rpwdl
landing.archive   ingestion/common/archive.py --source rpwdl (move to Archive)
archive.prune     ingestion/common/retention.py --source rpwdl (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "rpwdl"


def _load(p) -> int:
    from ingestion.to_bronze.rpwdl import load

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
