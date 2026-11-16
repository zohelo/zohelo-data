"""GUNB RWDZ (register PL-ENV-033) Landing → Bronze → Silver activities (design #875, as GLEIF).

Landing is the shared bulk_file pattern (the producer's files of https://wyszukiwarka.gunb.gov.pl/pobranie.html).

bronze.load       entities.py: per declared file (one voivodeship), newest Landing version →
                  bronze.gunb_rwdz_records (allowed columns only, no personal data) and
                  silver.gunb_rwdz_records (warehouse/entity/gunb_rwdz_records.sql)
bronze.verify     entities.verify: newest Landing version of each file in Bronze and Silver
landing.archive   ingestion/common/archive.py --source gunb_rwdz (move to Archive)
archive.prune     ingestion/common/retention.py --source gunb_rwdz (archive_policy.retention)
"""

from __future__ import annotations


def _load(p) -> int:
    from ingestion.to_bronze.gunb_rwdz import entities

    return entities.main(["--minutes", f"{max(p.budget_minutes - 10, 5):.0f}"])


def _verify(p) -> int:
    from ingestion.to_bronze.gunb_rwdz import entities

    return entities.verify()


def _archive(p) -> int:
    from ingestion.common import archive

    return archive.main(["--source", "gunb_rwdz"])


def _prune(p) -> int:
    from ingestion.common import retention

    return retention.main(["--source", "gunb_rwdz"])


ACTIVITIES = {"bronze.load": _load, "bronze.verify": _verify, "landing.archive": _archive, "archive.prune": _prune}
