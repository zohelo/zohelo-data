"""GUS TERYT Landing → Bronze activities.

bronze.load       load.py → bronze.teryt_records (one Parquet file per Landing file)
bronze.verify     ingestion/common/lineage.py --source gus_teryt
landing.archive   ingestion/common/archive.py --source gus_teryt (move to Archive)
archive.prune     ingestion/common/retention.py --source gus_teryt (archive_policy.retention)
"""
from __future__ import annotations


def _load(p) -> int:
    from ingestion.to_bronze.gus_teryt import load
    return load.load(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.common import lineage
    return lineage.main(["--source", "gus_teryt"])


def _archive(p) -> int:
    from ingestion.common import archive
    return archive.main(["--source", "gus_teryt"])


def _prune(p) -> int:
    from ingestion.common import retention
    return retention.main(["--source", "gus_teryt"])


ACTIVITIES = {"bronze.load": _load, "bronze.verify": _verify, "landing.archive": _archive,
              "archive.prune": _prune}
