"""GUS DBW Landing → Bronze → Silver activities (design #875, as Eurostat).

bronze.load       entities.py: per bulk file, newest Landing version → bronze.dbw_observations and
                  silver.dbw_observations (warehouse/entity/dbw_observations.sql)
bronze.verify     entities.verify: Landing → Bronze → Silver completeness from table metadata
landing.archive   ingestion/common/archive.py --source gus_dbw (move to Archive)
archive.prune     ingestion/common/retention.py --source gus_dbw (archive_policy.retention)
"""

from __future__ import annotations


def _load(p) -> int:
    from ingestion.to_bronze.gus_dbw import entities

    return entities.main(["--minutes", f"{max(p.budget_minutes - 10, 5):.0f}"])


def _verify(p) -> int:
    from ingestion.to_bronze.gus_dbw import entities

    return entities.verify()


def _archive(p) -> int:
    from ingestion.common import archive

    return archive.main(["--source", "gus_dbw"])


def _prune(p) -> int:
    from ingestion.common import retention

    return retention.main(["--source", "gus_dbw"])


ACTIVITIES = {"bronze.load": _load, "bronze.verify": _verify, "landing.archive": _archive, "archive.prune": _prune}
