"""MZ / Centrum e-Zdrowia statistical bulletin: Landing → Bronze activities (register PLS-017, #2957).

bronze.load       newest copy of every landed edition → bronze.mz_health_bulletin_cells (one row per non-empty cell
                  of every sheet, ingestion/common/xlsx_cells.py, so every table is kept, modelled or not); a file
                  that is not an XLSX workbook → bronze.mz_health_bulletin_rejected_files with the reason
bronze.verify     ingestion/common/lineage.py --source mz_health_bulletin
landing.archive   ingestion/common/archive.py --source mz_health_bulletin (move to Archive)
archive.prune     ingestion/common/retention.py --source mz_health_bulletin (archive_policy.retention)
"""

from __future__ import annotations

from ingestion.common import gus_publications

SOURCE = "mz_health_bulletin"


def bronze_load(entities: list[str], landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    from ingestion.common import bulk_bronze, xlsx_cells

    return bulk_bronze.load(
        SOURCE,
        (namespace, f"{SOURCE}_cells"),
        (namespace, f"{SOURCE}_rejected_files"),
        xlsx_cells.COLUMNS,
        xlsx_cells.decode,
        "cells",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=[{"entity": e} for e in sorted(entities)],
    )


def _entities() -> list[str]:
    from ingestion.to_landing import mz_health_bulletin as landing

    return sorted(landing.held())


ACTIVITIES = {
    **gus_publications.bronze_activities(SOURCE, _entities),
    "bronze.load": lambda p: bronze_load(_entities()),
}
