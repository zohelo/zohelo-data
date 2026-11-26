"""GUS Zdrowie i ochrona zdrowia: Landing → Bronze activities (register PLS-004, health family).

bronze.load       newest copy of every landed edition → bronze.gus_health_cells
                  (one row per workbook cell, ingestion/common/gus_publications.py);
                  rejections → bronze.gus_health_rejected_files
bronze.verify     ingestion/common/lineage.py --source gus_health
landing.archive   ingestion/common/archive.py --source gus_health (move to Archive)
archive.prune     ingestion/common/retention.py --source gus_health (archive_policy.retention)
"""

from __future__ import annotations

from ingestion.common import gus_publications


def _entities() -> list[str]:
    from ingestion.to_landing.gus_health import SERIES

    return sorted(gus_publications.held(SERIES))


ACTIVITIES = gus_publications.bronze_activities("gus_health", _entities)
