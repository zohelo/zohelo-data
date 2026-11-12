"""SIO archival education statistics (cie.gov.pl): Landing → Bronze activities (register PLS-022).

bronze.load       newest copy of every landed workbook → bronze.cie_sio_cells (one row per non-empty cell, the
                  shared workbook-cell shape): .xlsx with ingestion/common/xlsx_cells.py, the old binary .xls with
                  xlrd (the decoder of mrpips_social_assistance, which reads both); a file that is neither is
                  rejected → bronze.cie_sio_rejected_files
bronze.verify     ingestion/common/lineage.py --source cie_sio
landing.archive   ingestion/common/archive.py --source cie_sio (move to Archive)
archive.prune     ingestion/common/retention.py --source cie_sio (archive_policy.retention)
"""

from __future__ import annotations

from ingestion.common import gus_publications

SOURCE = "cie_sio"


def bronze_load(entities: list[str], landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    from ingestion.common import bulk_bronze
    from ingestion.to_bronze.mrpips_social_assistance import decode

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
    from ingestion.to_landing import cie_sio as landing

    return sorted(landing.held())


ACTIVITIES = {
    **gus_publications.bronze_activities(SOURCE, _entities),
    "bronze.load": lambda p: bronze_load(_entities()),
}
