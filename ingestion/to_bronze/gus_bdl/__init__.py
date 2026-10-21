"""GUS BDL Landing → Bronze → Silver activities (design #875).

bronze.load       entities.py: per subgroup, every part of the newest complete download →
                  bronze.bdl_observations and silver.bdl_observations (warehouse/entity/bdl_observations.sql);
                  then the landed API metadata → bronze.bdl_metadata_variables / _units (metadata.py)
bronze.verify     entities.verify: Landing → Bronze → Silver completeness from table metadata
landing.archive   ingestion/common/archive.py --source gus_bdl (move to Archive; proof: entities.proven)
"""

from __future__ import annotations


def _load(p) -> int:
    from ingestion.common.report import annotate
    from ingestion.to_bronze.gus_bdl import entities, metadata

    annotate("notice", "gus_bdl metadata Bronze", metadata.load())
    return entities.main(["--minutes", f"{max(p.budget_minutes - 10, 5):.0f}"])


def _verify(p) -> int:
    from ingestion.to_bronze.gus_bdl import entities

    return entities.verify()


def _archive(p) -> int:
    from ingestion.common import archive

    return archive.main(["--source", "gus_bdl"])


ACTIVITIES = {"bronze.load": _load, "bronze.verify": _verify, "landing.archive": _archive}
