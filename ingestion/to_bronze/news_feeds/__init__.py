"""News feeds Landing → Bronze activities.

bronze.load       load.py → bronze.news_items (one Parquet file per Landing file)
bronze.verify     ingestion/common/lineage.py --source news_feeds
landing.archive   ingestion/common/archive.py --source news_feeds (move to Archive)
archive.prune     ingestion/common/retention.py --source news_feeds (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "news_feeds"


def _load(p) -> int:
    from ingestion.to_bronze.news_feeds import load

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
