"""GUS BDL Landing → Bronze activities.

bronze.load       load.py: batches of batch.py, decode.py per file → bronze.bdl_subgroup_observations
bronze.verify     ingestion/common/lineage.py --source gus_bdl (latest-only check and duplicate reporting)
"""

from __future__ import annotations


def _load(p) -> int:
    from ingestion.to_bronze.gus_bdl import load

    return load.load(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.common import lineage

    return lineage.main(["--source", "gus_bdl"])


ACTIVITIES = {"bronze.load": _load, "bronze.verify": _verify}
