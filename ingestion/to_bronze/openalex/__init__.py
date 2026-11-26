"""OpenAlex Landing → Bronze activities (register CD-028).

bronze.load       load.py → bronze.openalex_works_counts, bronze.openalex_institutions (a Parquet file per Landing file)
bronze.verify     ingestion/common/lineage.py --source openalex
landing.archive   ingestion/common/archive.py --source openalex (move to Archive)
archive.prune     ingestion/common/retention.py --source openalex (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "openalex"


def _load(p) -> int:
    from ingestion.to_bronze.openalex import load

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
