"""KNF public warning list (PLG-019) Landing → Bronze activities.

bronze.load       load.py → bronze.knf_warnings_entries (one Parquet file per Landing file, one row per list entry)
bronze.verify     ingestion/common/lineage.py --source knf_warnings
landing.archive   ingestion/common/archive.py --source knf_warnings (move to Archive)
archive.prune     ingestion/common/retention.py --source knf_warnings (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "knf_warnings"


def _load(p) -> int:
    from ingestion.to_bronze.knf_warnings import load

    return load.load()


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
