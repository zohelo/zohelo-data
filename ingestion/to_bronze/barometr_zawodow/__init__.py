"""Barometr Zawodów: Landing → Bronze activities (register PLS-030).

bronze.load       load.py → bronze.barometr_zawodow_occupations (one row per occupation printed on a poster page)
bronze.verify     ingestion/common/lineage.py --source barometr_zawodow
landing.archive   ingestion/common/archive.py --source barometr_zawodow (move to Archive)
archive.prune     ingestion/common/retention.py --source barometr_zawodow (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "barometr_zawodow"


def _load(p) -> int:
    from ingestion.to_bronze.barometr_zawodow import load

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
