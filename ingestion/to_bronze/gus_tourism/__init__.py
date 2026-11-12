"""GUS Turystyka: Landing → Bronze activities (register PLS-035).

bronze.load       newest copy of every landed edition → bronze.gus_tourism_cells
                  (one row per workbook cell, ingestion/common/gus_publications.py);
                  rejections → bronze.gus_tourism_rejected_files
bronze.verify     ingestion/common/lineage.py --source gus_tourism
landing.archive   ingestion/common/archive.py --source gus_tourism (move to Archive)
archive.prune     ingestion/common/retention.py --source gus_tourism (archive_policy.retention)
"""

from __future__ import annotations

from ingestion.common import gus_publications


def _entities() -> list[str]:
    from ingestion.to_landing.gus_tourism import SERIES

    return sorted(gus_publications.held(SERIES))


ACTIVITIES = gus_publications.bronze_activities("gus_tourism", _entities)
