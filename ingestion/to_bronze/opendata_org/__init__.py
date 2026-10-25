"""OpenData.org Landing → Bronze → Silver activities (design #875).

bronze.load       entities.py: per member file of the newest snapshot ZIP → bronze.opendata_records
                  and silver.opendata_organizations / _people / _locations (warehouse/entity/)
bronze.verify     entities.verify: members of the newest snapshot in Bronze and Silver
landing.archive   ingestion/common/archive.py --source opendata_org (move to Archive)
archive.prune     ingestion/common/retention.py --source opendata_org (archive_policy.retention)
"""

from __future__ import annotations


def _load(p) -> int:
    from ingestion.to_bronze.opendata_org import entities

    return entities.main(["--minutes", f"{max(p.budget_minutes - 20, 5):.0f}"])


def _verify(p) -> int:
    from ingestion.to_bronze.opendata_org import entities

    return entities.verify()


def _archive(p) -> int:
    from ingestion.common import archive

    return archive.main(["--source", "opendata_org"])


def _prune(p) -> int:
    from ingestion.common import retention

    return retention.main(["--source", "opendata_org"])


ACTIVITIES = {"bronze.load": _load, "bronze.verify": _verify, "landing.archive": _archive, "archive.prune": _prune}
