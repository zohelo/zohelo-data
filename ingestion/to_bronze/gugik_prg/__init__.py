"""GUGiK PRG Landing → Bronze activities.

bronze.load       load.py → bronze.prg_boundaries, then addresses.py → bronze.prg_address_points
                  (one Parquet file per Landing file each)
bronze.verify     ingestion/common/lineage.py --source gugik_prg
landing.archive   ingestion/common/archive.py --source gugik_prg (move to Archive)
archive.prune     ingestion/common/retention.py --source gugik_prg (archive_policy.retention)
"""

from __future__ import annotations


def _load(p) -> int:
    from ingestion.to_bronze.gugik_prg import addresses, load

    try:
        boundaries = load.load(p.budget_minutes)
    finally:  # the address package loads even when the boundary package fails (that error still fails the step)
        points = addresses.load(p.budget_minutes)
    return max(boundaries, points)


def _verify(p) -> int:
    from ingestion.common import lineage

    return lineage.main(["--source", "gugik_prg"])


def _archive(p) -> int:
    from ingestion.common import archive

    return archive.main(["--source", "gugik_prg"])


def _prune(p) -> int:
    from ingestion.common import retention

    return retention.main(["--source", "gugik_prg"])


ACTIVITIES = {"bronze.load": _load, "bronze.verify": _verify, "landing.archive": _archive, "archive.prune": _prune}
