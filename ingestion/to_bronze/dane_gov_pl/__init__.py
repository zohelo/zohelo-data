"""dane.gov.pl catalogue (register PLG-030) Landing → Bronze activities.

bronze.load       load.py → bronze.dane_gov_pl_datasets, bronze.dane_gov_pl_distributions
                  (one Parquet file per Landing file; rejections in bronze.dane_gov_pl_rejected_files)
bronze.verify     ingestion/common/lineage.py --source dane_gov_pl
landing.archive   ingestion/common/archive.py --source dane_gov_pl (move to Archive)
archive.prune     ingestion/common/retention.py --source dane_gov_pl (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "dane_gov_pl"


def _load(p) -> int:
    from ingestion.to_bronze.dane_gov_pl import load

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
