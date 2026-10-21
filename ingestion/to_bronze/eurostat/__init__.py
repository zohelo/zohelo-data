"""Eurostat Landing → Bronze → Silver activities (design #875).

bronze.load       entities.py: per dataset, newest Landing version → bronze.eurostat_observations and
                  silver.eurostat_observations (warehouse/entity/eurostat_observations.sql); before it,
                  the bulk structure metadata → bronze.eurostat_structure_items (structure.py)
bronze.verify     entities.verify: Landing → Bronze → Silver completeness from table metadata
landing.archive   ingestion/common/archive.py --source eurostat (move to Archive)
archive.prune     ingestion/common/retention.py --source eurostat (archive_policy.retention)
"""

from __future__ import annotations


def _load(p) -> int:
    import time

    from ingestion.common.report import annotate
    from ingestion.to_bronze.eurostat import entities, structure

    started = time.monotonic()
    annotate("notice", "eurostat structure Bronze", structure.load(min(30.0, p.budget_minutes / 4)))
    left = p.budget_minutes - (time.monotonic() - started) / 60
    return entities.main(["--minutes", f"{max(left - 10, 5):.0f}"])


def _verify(p) -> int:
    from ingestion.to_bronze.eurostat import entities

    return entities.verify()


def _archive(p) -> int:
    from ingestion.common import archive

    return archive.main(["--source", "eurostat"])


def _prune(p) -> int:
    from ingestion.common import retention

    return retention.main(["--source", "eurostat"])


ACTIVITIES = {"bronze.load": _load, "bronze.verify": _verify, "landing.archive": _archive, "archive.prune": _prune}
