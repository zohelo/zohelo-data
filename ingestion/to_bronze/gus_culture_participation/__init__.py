"""GUS Uczestnictwo ludności w kulturze: Landing → Bronze activities (register PLS-028).

bronze.load       newest copy of every landed edition → bronze.gus_culture_participation_cells
                  (one row per workbook cell, ingestion/common/gus_publications.py);
                  rejections → bronze.gus_culture_participation_rejected_files
bronze.verify     ingestion/common/lineage.py --source gus_culture_participation
landing.archive   ingestion/common/archive.py --source gus_culture_participation (move to Archive)
archive.prune     ingestion/common/retention.py --source gus_culture_participation (archive_policy.retention)
"""

from __future__ import annotations

from ingestion.common import gus_publications


def _entities() -> list[str]:
    from ingestion.to_landing.gus_culture_participation import SERIES

    return sorted(gus_publications.held(SERIES))


ACTIVITIES = gus_publications.bronze_activities("gus_culture_participation", _entities)
