"""NIZP PZH-PIB EPIMELD annual bulletins: Landing → Bronze activities (register PLS-019).

bronze.load       load.py → bronze.pzh_epimeld_rows (one row per printed row of the voivodeship tables, PDF text layer)
bronze.verify     ingestion/common/lineage.py --source pzh_epimeld
landing.archive   ingestion/common/archive.py --source pzh_epimeld (move to Archive)
archive.prune     ingestion/common/retention.py --source pzh_epimeld (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "pzh_epimeld"


def _load(p) -> int:
    from ingestion.to_bronze.pzh_epimeld import load

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
