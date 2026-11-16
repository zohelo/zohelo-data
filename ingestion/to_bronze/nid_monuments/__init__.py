"""NID register of immovable monuments: Landing → Bronze activities (register PLS-026).

bronze.load       load.py → bronze.nid_monuments_items (one row per item of each PDF list, newest copy per list);
                  rejections → bronze.nid_monuments_rejected_files
bronze.verify     ingestion/common/lineage.py --source nid_monuments
landing.archive   ingestion/common/archive.py --source nid_monuments (move to Archive)
archive.prune     ingestion/common/retention.py --source nid_monuments (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "nid_monuments"


def _load(p) -> int:
    from ingestion.to_bronze.nid_monuments import load

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
