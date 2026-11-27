"""UOKiK decisions Landing → Bronze activities.

bronze.load       load.py → bronze.uokik_decisions_index (one Parquet file per Landing file, one row per decision)
bronze.verify     ingestion/common/lineage.py --source uokik_decisions
landing.archive   ingestion/common/archive.py --source uokik_decisions (move to Archive)
archive.prune     ingestion/common/retention.py --source uokik_decisions (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "uokik_decisions"


def _load(p) -> int:
    from ingestion.to_bronze.uokik_decisions import load

    return load.load()


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
