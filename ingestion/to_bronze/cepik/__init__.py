"""CEPiK vehicle statistics: Landing → Bronze activities (register PL-ENV-024).

bronze.load       newest copy of every landed workbook → bronze.cepik_cells (one row per non-empty cell, shared
                  XLSX cell shape, ingestion/common/xlsx_cells.py); a file that is not an XLSX workbook is
                  rejected → bronze.cepik_rejected_files
bronze.verify     ingestion/common/lineage.py --source cepik
landing.archive   ingestion/common/archive.py --source cepik (move to Archive)
archive.prune     ingestion/common/retention.py --source cepik (archive_policy.retention)
"""

from __future__ import annotations

from typing import Any

from ingestion.common import gus_publications

SOURCE = "cepik"


def _entities() -> list[str]:
    from ingestion.to_landing import cepik as landing

    return sorted(landing.held())


ACTIVITIES: dict[str, Any] = {
    **gus_publications.bronze_activities(SOURCE, _entities),
}
