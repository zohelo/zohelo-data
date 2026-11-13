"""KG PSP fire-service intervention statistics: Landing → Bronze activities (register EX-003).

bronze.load       newest copy of every landed standard-tables ZIP → bronze.kgpsp_interventions_cells
                  (one row per non-empty cell of every .xlsx member, ingestion/common/gus_publications.py decode,
                  cells read by ingestion/common/xlsx_cells.py); a file that is not a ZIP of workbooks is rejected
                  → bronze.kgpsp_interventions_rejected_files
bronze.verify     ingestion/common/lineage.py --source kgpsp_interventions
landing.archive   ingestion/common/archive.py --source kgpsp_interventions (move to Archive)
archive.prune     ingestion/common/retention.py --source kgpsp_interventions (archive_policy.retention)
"""

from __future__ import annotations

from ingestion.common import gus_publications

SOURCE = "kgpsp_interventions"


def _entities() -> list[str]:
    from ingestion.to_landing.kgpsp_interventions import held

    return sorted(held())


ACTIVITIES = gus_publications.bronze_activities(SOURCE, _entities)
