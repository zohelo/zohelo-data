"""GUGiK elevation sheet indexes Landing → Bronze → Silver activities (design #875).

bronze.load       entities.py: newest complete version of each service's year → bronze.gugik_nmt_sheets and
                  silver.gugik_nmt_sheets (one file per service and year, replaced whole)
bronze.verify     entities.verify: newest Landing version of every service's year in Bronze and Silver
landing.archive   ingestion/common/archive.py --source gugik_nmt (move to Archive)
archive.prune     ingestion/common/retention.py --source gugik_nmt (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "gugik_nmt"


def _load(p) -> int:
    from ingestion.to_bronze.gugik_nmt import entities

    return entities.main(["--minutes", f"{max(p.budget_minutes - 15, 5):.0f}"])


def _verify(p) -> int:
    from ingestion.to_bronze.gugik_nmt import entities

    return entities.verify()


def _archive(p) -> int:
    from ingestion.common import archive

    return archive.main(["--source", SOURCE])


def _prune(p) -> int:
    from ingestion.common import retention

    return retention.main(["--source", SOURCE])


ACTIVITIES = {"bronze.load": _load, "bronze.verify": _verify, "landing.archive": _archive, "archive.prune": _prune}
