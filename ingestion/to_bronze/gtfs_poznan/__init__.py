"""ZTM Poznań GTFS (register PL-ENV-030): Landing → Bronze activities, ingestion/common/gtfs.py.

bronze.load       newest copy of every landed window → bronze.gtfs_poznan_records; rejections →
                  bronze.gtfs_poznan_rejected_files
bronze.verify     ingestion/common/lineage.py --source gtfs_poznan
landing.archive   ingestion/common/archive.py --source gtfs_poznan (move to Archive)
archive.prune     ingestion/common/retention.py --source gtfs_poznan (archive_policy.retention)
"""

from __future__ import annotations

from ingestion.common import gtfs

ACTIVITIES = gtfs.bronze_activities("gtfs_poznan")
