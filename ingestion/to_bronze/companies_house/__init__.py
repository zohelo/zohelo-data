"""Companies House Landing → Bronze → Silver activities (design #875, as GLEIF and the MF white list).

bronze.load       entities.py: newest snapshot file → bronze.companies_house_companies (replaced),
                  silver.companies_house_poland_origin (replaced) and silver.companies_house_snapshot_counts
                  (one file per snapshot date, kept)
bronze.verify     entities.verify: the newest Landing file is in Bronze and both Silver tables, or rejected
landing.archive   ingestion/common/archive.py --source companies_house (move to Archive)
archive.prune     ingestion/common/retention.py --source companies_house (archive_policy.retention)
"""

from __future__ import annotations


def _load(p) -> int:
    from ingestion.to_bronze.companies_house import entities

    return entities.main()


def _verify(p) -> int:
    from ingestion.to_bronze.companies_house import entities

    return entities.verify()


def _archive(p) -> int:
    from ingestion.common import archive

    return archive.main(["--source", "companies_house"])


def _prune(p) -> int:
    from ingestion.common import retention

    return retention.main(["--source", "companies_house"])


ACTIVITIES = {"bronze.load": _load, "bronze.verify": _verify, "landing.archive": _archive, "archive.prune": _prune}
