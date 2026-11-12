"""GUS PSR 2020 agricultural census: Landing → Bronze activities (register PLS-006). Landing is the shared bulk_file pattern.

bronze.load       newest copy of every declared workbook → bronze.gus_psr2020_cells
                  (one row per workbook cell, ingestion/common/gus_publications.py);
                  rejections → bronze.gus_psr2020_rejected_files
bronze.verify     ingestion/common/lineage.py --source gus_psr2020
landing.archive   ingestion/common/archive.py --source gus_psr2020 (move to Archive)
archive.prune     ingestion/common/retention.py --source gus_psr2020 (archive_policy.retention)
"""

from __future__ import annotations

from ingestion.common import gus_publications


def _entities() -> list[str]:
    from ingestion.to_landing.patterns import bulk_file

    return [spec["entity"] for spec in bulk_file.declaration("gus_psr2020")]


ACTIVITIES = gus_publications.bronze_activities("gus_psr2020", _entities)
