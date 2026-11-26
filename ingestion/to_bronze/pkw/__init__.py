"""PKW election results: Landing → Bronze activities (register PLG-029).

Landing is the shared bulk_file pattern (ingestion/sources/pkw.yaml).

bronze.load       load.py → bronze.pkw_cells (one row per non-empty cell of each newest Landing file)
bronze.verify     ingestion/common/lineage.py --source pkw
landing.archive   ingestion/common/archive.py --source pkw (move to Archive)
archive.prune     ingestion/common/retention.py --source pkw (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "pkw"


def _load(p) -> int:
    from ingestion.to_bronze.pkw import load

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
