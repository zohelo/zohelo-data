"""GUGiK PRG Landing → Bronze activities.

bronze.load       load.py → bronze.prg_boundaries (one Parquet file per Landing file)
bronze.verify     ingestion/common/lineage.py --source gugik_prg
landing.archive   ingestion/common/archive.py --source gugik_prg (move to Archive)
archive.prune     ingestion/common/retention.py --source gugik_prg (archive_policy.retention)
"""
from __future__ import annotations


def _load(p) -> int:
    from ingestion.to_bronze.gugik_prg import load
    return load.load(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.common import lineage
    return lineage.main(["--source", "gugik_prg"])


def _archive(p) -> int:
    from ingestion.common import archive
    return archive.main(["--source", "gugik_prg"])


def _prune(p) -> int:
    from ingestion.common import retention
    return retention.main(["--source", "gugik_prg"])


ACTIVITIES = {"bronze.load": _load, "bronze.verify": _verify, "landing.archive": _archive,
              "archive.prune": _prune}
