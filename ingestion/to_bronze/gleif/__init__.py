"""GLEIF Landing → Bronze activities.

bronze.load       load.py → bronze.gleif_lei_records, gleif_relationship_records, gleif_reporting_exceptions
bronze.verify     ingestion/common/lineage.py --source gleif
landing.archive   ingestion/common/archive.py --source gleif (move to Archive)
archive.prune     ingestion/common/retention.py --source gleif (archive_policy.retention)
"""

from __future__ import annotations


def _load(p) -> int:
    from ingestion.to_bronze.gleif import load

    return load.load(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.common import lineage

    return lineage.main(["--source", "gleif"])


def _archive(p) -> int:
    from ingestion.common import archive

    return archive.main(["--source", "gleif"])


def _prune(p) -> int:
    from ingestion.common import retention

    return retention.main(["--source", "gleif"])


ACTIVITIES = {"bronze.load": _load, "bronze.verify": _verify, "landing.archive": _archive, "archive.prune": _prune}
