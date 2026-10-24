"""MF VAT white list Landing → Bronze → Silver activities (design #875).

bronze.load       entities.py: newest flat file → bronze.biala_lista_records and
                  silver.biala_lista_taxpayer_hashes / silver.biala_lista_masks
bronze.verify     entities.verify: newest Landing version in Bronze and both Silver tables
landing.archive   ingestion/common/archive.py --source mf_biala_lista (move to Archive)
archive.prune     ingestion/common/retention.py --source mf_biala_lista (archive_policy.retention)
"""

from __future__ import annotations


def _load(p) -> int:
    from ingestion.to_bronze.mf_biala_lista import entities

    return entities.main(["--minutes", f"{max(p.budget_minutes - 10, 5):.0f}"])


def _verify(p) -> int:
    from ingestion.to_bronze.mf_biala_lista import entities

    return entities.verify()


def _archive(p) -> int:
    from ingestion.common import archive

    return archive.main(["--source", "mf_biala_lista"])


def _prune(p) -> int:
    from ingestion.common import retention

    return retention.main(["--source", "mf_biala_lista"])


ACTIVITIES = {"bronze.load": _load, "bronze.verify": _verify, "landing.archive": _archive, "archive.prune": _prune}
