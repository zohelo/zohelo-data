"""Wrocław public transport GTFS (register PL-ENV-032): Landing → Bronze activities, ingestion/common/gtfs.py.

bronze.load       newest copy of every landed window → bronze.gtfs_wroclaw_records; rejections →
                  bronze.gtfs_wroclaw_rejected_files
bronze.verify     ingestion/common/lineage.py --source gtfs_wroclaw
landing.archive   ingestion/common/archive.py --source gtfs_wroclaw (move to Archive)
archive.prune     ingestion/common/retention.py --source gtfs_wroclaw (archive_policy.retention)
"""

from __future__ import annotations

from ingestion.common import gtfs

ACTIVITIES = gtfs.bronze_activities("gtfs_wroclaw")
