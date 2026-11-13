"""internet.gov.pl broadband report (PL-ENV-034) Landing → Bronze activities.

bronze.load       load.py → bronze.internet_gov_broadband_cells (one Parquet file per Landing file;
                  rejections → bronze.internet_gov_broadband_rejected_files)
bronze.verify     ingestion/common/lineage.py --source internet_gov_broadband
landing.archive   ingestion/common/archive.py --source internet_gov_broadband (move to Archive)
archive.prune     ingestion/common/retention.py --source internet_gov_broadband (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "internet_gov_broadband"


def _load(p) -> int:
    from ingestion.to_bronze.internet_gov_broadband import load

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
