"""Our World in Data (CD-031) Landing → Bronze activities.

bronze.load       load.py → bronze.owid_values (chart CSV files), bronze.owid_charts (metadata.json files)
bronze.verify     ingestion/common/lineage.py --source owid
landing.archive   ingestion/common/archive.py --source owid (move to Archive)
archive.prune     ingestion/common/retention.py --source owid (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "owid"


def _load(p) -> int:
    from ingestion.to_bronze.owid import load

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
