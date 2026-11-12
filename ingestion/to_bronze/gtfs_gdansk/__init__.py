"""ZTM Gdańsk GTFS (register PL-ENV-029): Landing → Bronze activities, ingestion/common/gtfs.py.

bronze.load       newest copy of every landed window → bronze.gtfs_gdansk_records; rejections →
                  bronze.gtfs_gdansk_rejected_files
bronze.verify     ingestion/common/lineage.py --source gtfs_gdansk
landing.archive   ingestion/common/archive.py --source gtfs_gdansk (move to Archive)
archive.prune     ingestion/common/retention.py --source gtfs_gdansk (archive_policy.retention)
"""

from __future__ import annotations

from ingestion.common import gtfs

ACTIVITIES = gtfs.bronze_activities("gtfs_gdansk")
