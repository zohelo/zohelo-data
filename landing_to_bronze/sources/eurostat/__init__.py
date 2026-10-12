"""Eurostat Landing → Bronze activities.

bronze.load       load.py: batches of batch.py, decode.py per file → bronze.eurostat_full_observations
bronze.verify     landing_to_bronze/common/lineage.py --source eurostat
landing.archive   landing_to_bronze/common/archive.py --source eurostat
"""
from __future__ import annotations


def _load(p) -> int:
    from landing_to_bronze.sources.eurostat import load
    return load.load(p.budget_minutes)


def _verify(p) -> int:
    from landing_to_bronze.common import lineage
    return lineage.main(["--source", "eurostat"])


def _archive(p) -> int:
    from landing_to_bronze.common import archive
    return archive.main(["--source", "eurostat"])


ACTIVITIES = {"bronze.load": _load, "bronze.verify": _verify, "landing.archive": _archive}
