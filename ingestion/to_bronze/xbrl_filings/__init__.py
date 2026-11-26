"""filings.xbrl.org Landing → Bronze activities.

bronze.load       load.py → bronze.xbrl_filings_filings, bronze.xbrl_filings_facts (one Parquet file per Landing file)
bronze.verify     ingestion/common/lineage.py --source xbrl_filings
landing.archive   ingestion/common/archive.py --source xbrl_filings (move to Archive)
archive.prune     ingestion/common/retention.py --source xbrl_filings (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "xbrl_filings"


def _load(p) -> int:
    from ingestion.to_bronze.xbrl_filings import load

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
