#!/usr/bin/env python3
"""knf_pensions bronze.load: newest Landing copy of the PPK workbook → bronze.knf_pensions_cells.

One row per non-empty cell (sheet, row, column, source text, number format), decoded by the shared
ingestion/common/xlsx_cells.py; Silver picks the cells it models. A file that is not an XLSX workbook is
recorded in bronze.knf_pensions_rejected_files, never loaded silently. Loader: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

from ingestion.common import bulk_bronze, xlsx_cells

SOURCE = "knf_pensions"
TABLE = "knf_pensions_cells"
REJECTED = "knf_pensions_rejected_files"


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: one workbook of about 130 kB."""
    from ingestion.to_landing import knf_pensions as landing

    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE),
        (namespace, REJECTED),
        xlsx_cells.COLUMNS,
        xlsx_cells.decode,
        "cells",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=[{"entity": landing.ENTITY}],  # not a bulk_file declaration: the one entity of to_landing/knf_pensions
    )
