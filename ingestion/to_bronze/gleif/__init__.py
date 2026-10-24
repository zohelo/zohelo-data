"""GLEIF Landing → Bronze → Silver activities (design #875, as Eurostat).

bronze.load       entities.py: per declared file (lei2, rr, repex), newest Landing version →
                  its Bronze table and its Silver table (warehouse/entity/gleif_*.sql)
bronze.verify     entities.verify: newest Landing version of each file in Bronze and Silver
landing.archive   ingestion/common/archive.py --source gleif (move to Archive)
archive.prune     ingestion/common/retention.py --source gleif (archive_policy.retention)
"""

from __future__ import annotations


def _load(p) -> int:
    from ingestion.to_bronze.gleif import entities

    return entities.main(["--minutes", f"{max(p.budget_minutes - 10, 5):.0f}"])


def _verify(p) -> int:
    from ingestion.to_bronze.gleif import entities

    return entities.verify()


def _archive(p) -> int:
    from ingestion.common import archive

    return archive.main(["--source", "gleif"])


def _prune(p) -> int:
    from ingestion.common import retention

    return retention.main(["--source", "gleif"])


ACTIVITIES = {"bronze.load": _load, "bronze.verify": _verify, "landing.archive": _archive, "archive.prune": _prune}
