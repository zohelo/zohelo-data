"""WIPO IP Statistics Landing → Bronze activities (register GL-032).

Landing is the shared bulk_file pattern (one ZIP per right, found on the IP statistics page).

bronze.load       load.py → bronze.wipo_ip_stats_counts (one Parquet file per Landing file);
                  rejections → bronze.wipo_ip_stats_rejected_files
bronze.verify     ingestion/common/lineage.py --source wipo_ip_stats
landing.archive   ingestion/common/archive.py --source wipo_ip_stats (move to Archive)
archive.prune     ingestion/common/retention.py --source wipo_ip_stats (archive_policy.retention)
"""

from __future__ import annotations

SOURCE = "wipo_ip_stats"


def _load(p) -> int:
    from ingestion.to_bronze.wipo_ip_stats import load

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
