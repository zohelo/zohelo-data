"""Senate (register PLG-025) Landing → Bronze activities.

bronze.load       load.py → bronze.senat_sittings, senat_day_votes, senat_vote_results, senat_senators
                  (one Parquet file per Landing file; rejections in bronze.senat_rejected_files)
bronze.verify     ingestion/common/lineage.py --source senat
landing.archive   ingestion/common/archive.py --source senat (move to Archive)
archive.prune     ingestion/common/retention.py --source senat (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "senat"


def _load(p) -> int:
    from ingestion.to_bronze.senat import load

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
