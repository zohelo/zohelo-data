"""ZTP Kraków GTFS (register PL-ENV-031): Landing → Bronze activities, ingestion/common/gtfs.py.

bronze.load       newest copy of every landed window → bronze.gtfs_krakow_records; rejections →
                  bronze.gtfs_krakow_rejected_files
bronze.verify     ingestion/common/lineage.py --source gtfs_krakow
landing.archive   ingestion/common/archive.py --source gtfs_krakow (move to Archive)
archive.prune     ingestion/common/retention.py --source gtfs_krakow (archive_policy.retention)
"""

from __future__ import annotations

from ingestion.common import gtfs

ACTIVITIES = gtfs.bronze_activities("gtfs_krakow")
