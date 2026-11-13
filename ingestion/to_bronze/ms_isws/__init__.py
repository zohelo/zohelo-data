"""ISWS justice statistics: Landing → Bronze activities (register PLG-031).

bronze.load       newest copy of every landed workbook → bronze.ms_isws_cells (one row per non-empty cell of the old
                  Excel (.xls, BIFF) workbook, read with xlrd from requirements.txt by the decoder of
                  ingestion/to_bronze/mrpips_social_assistance/, or of an .xlsx workbook through
                  ingestion/common/xlsx_cells.py); a file that is neither is rejected → bronze.ms_isws_rejected_files
bronze.verify     ingestion/common/lineage.py --source ms_isws
landing.archive   ingestion/common/archive.py --source ms_isws (move to Archive)
archive.prune     ingestion/common/retention.py --source ms_isws (archive_policy.retention)

Error cells keep their Excel text (#N/A, #DIV/0!) and empty cells are not rows, so neither can become 0 in Silver.
"""

from __future__ import annotations

from typing import Any

from ingestion.common import gus_publications

SOURCE = "ms_isws"


def decode(body: bytes, receipt: dict[str, Any]):
    """bulk_bronze decoder: every non-empty cell in the shared cell shape (gus_publications.bronze_columns())."""
    from ingestion.to_bronze.mrpips_social_assistance import decode as xls_decode

    return xls_decode(body, receipt)


def bronze_load(entities: list[str], landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    from ingestion.common import bulk_bronze

    return bulk_bronze.load(
        SOURCE,
        (namespace, f"{SOURCE}_cells"),
        (namespace, f"{SOURCE}_rejected_files"),
        gus_publications.bronze_columns(),
        decode,
        "cells",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=[{"entity": e} for e in sorted(entities)],
    )


def _entities() -> list[str]:
    from ingestion.to_landing import ms_isws as landing

    return sorted(landing.held())


ACTIVITIES = {
    **gus_publications.bronze_activities(SOURCE, _entities),
    "bronze.load": lambda p: bronze_load(_entities()),
}
