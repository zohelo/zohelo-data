"""IMGW observation archive Landing → Bronze → Silver activities (design #875).

bronze.load       entities.py: newest daily synop ZIPs → bronze.imgw_archive_synop_daily and
                  silver.imgw_archive_synop_daily (one file per entity, replaced whole)
bronze.verify     entities.verify: newest Landing version of every entity in Bronze and Silver
landing.archive   ingestion/common/archive.py --source imgw_archive (move to Archive)
archive.prune     ingestion/common/retention.py --source imgw_archive (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "imgw_archive"


def _load(p) -> int:
    from ingestion.to_bronze.imgw_archive import entities

    return entities.main(["--minutes", f"{max(p.budget_minutes - 15, 5):.0f}"])


def _verify(p) -> int:
    from ingestion.to_bronze.imgw_archive import entities

    return entities.verify()


def _archive(p) -> int:
    from ingestion.common import archive

    return archive.main(["--source", SOURCE])


def _prune(p) -> int:
    from ingestion.common import retention

    return retention.main(["--source", SOURCE])


ACTIVITIES = {"bronze.load": _load, "bronze.verify": _verify, "landing.archive": _archive, "archive.prune": _prune}
