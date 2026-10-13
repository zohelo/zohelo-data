"""GUS BDL Landing → Bronze activities.

bronze.load       load.py: batches of batch.py, decode.py per file → bronze.bdl_subgroup_observations
"""
from __future__ import annotations


def _load(p) -> int:
    from ingestion.to_bronze.gus_bdl import load
    return load.load(p.budget_minutes)


ACTIVITIES = {"bronze.load": _load}
