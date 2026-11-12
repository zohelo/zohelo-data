"""UNDP HDR Landing → Bronze activities.

bronze.load       load.py → bronze.undp_hdr_values (one Parquet file per Landing file)
bronze.verify     ingestion/common/lineage.py --source undp_hdr
landing.archive   ingestion/common/archive.py --source undp_hdr (move to Archive)
archive.prune     ingestion/common/retention.py --source undp_hdr (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "undp_hdr"


def _load(p) -> int:
    from ingestion.to_bronze.undp_hdr import load

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
