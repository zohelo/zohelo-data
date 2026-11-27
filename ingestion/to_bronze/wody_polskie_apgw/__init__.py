"""Wody Polskie aPGW planning geodatabase: Landing → Bronze activities (register PL-ENV-013, #3313).

Landing is ingestion/to_landing/wody_polskie_apgw/ (the plan-cycle File Geodatabase ZIP of dane.gov.pl dataset 599).

bronze.load       load.py → bronze.wody_polskie_apgw_attributes (one Parquet file per Landing file); rejections →
                  bronze.wody_polskie_apgw_rejected_files
bronze.verify     ingestion/common/lineage.py --source wody_polskie_apgw
landing.archive   ingestion/common/archive.py --source wody_polskie_apgw (move to Archive)
archive.prune     ingestion/common/retention.py --source wody_polskie_apgw (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "wody_polskie_apgw"


def _load(p) -> int:
    from ingestion.to_bronze.wody_polskie_apgw import load

    return load.load(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.common import lineage

    return lineage.main(["--source", SOURCE])


def _archive(p) -> int:
    from ingestion.common import archive

    return archive.main(["--source", SOURCE])


def _prune(p) -> int:
    from ingestion.common import retention

    return retention.main(["--source", SOURCE])


ACTIVITIES = {"bronze.load": _load, "bronze.verify": _verify, "landing.archive": _archive, "archive.prune": _prune}
