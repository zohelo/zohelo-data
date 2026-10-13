"""NBP Landing → Bronze activities.

bronze.load       load.py → bronze.nbp_observations (one Parquet file per Landing file)
bronze.verify     ingestion/common/lineage.py --source nbp
landing.archive   ingestion/common/archive.py --source nbp (move to Archive)
archive.prune     ingestion/common/retention.py --source nbp (archive_policy.retention)
"""
from __future__ import annotations


def _load(p) -> int:
    from ingestion.to_bronze.nbp import load
    return load.load(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.common import lineage
    return lineage.main(["--source", "nbp"])


def _archive(p) -> int:
    from ingestion.common import archive
    return archive.main(["--source", "nbp"])


def _prune(p) -> int:
    from ingestion.common import retention
    return retention.main(["--source", "nbp"])


ACTIVITIES = {"bronze.load": _load, "bronze.verify": _verify, "landing.archive": _archive,
              "archive.prune": _prune}
