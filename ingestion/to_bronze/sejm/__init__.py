"""Sejm API (register PLG-024) Landing → Bronze activities.

bronze.load       load.py → bronze.sejm_terms, sejm_proceedings, sejm_voting_days, sejm_votings,
                  sejm_prints, sejm_mps
                  (one Parquet file per Landing file; rejections in bronze.sejm_rejected_files)
bronze.verify     ingestion/common/lineage.py --source sejm
landing.archive   ingestion/common/archive.py --source sejm (move to Archive)
archive.prune     ingestion/common/retention.py --source sejm (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "sejm"


def _load(p) -> int:
    from ingestion.to_bronze.sejm import load

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
