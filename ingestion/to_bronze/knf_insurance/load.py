#!/usr/bin/env python3
"""KNF insurance and pension statistics bronze.load: every landed release file → bronze.knf_insurance_cells.

One row per non-empty cell (ingestion/common/xlsx_cells.py: value as stored, sheet, row, column, number format) of
each release workbook (entity ofe_YYYY_MM, bulletin_YYYY_qN_<part>, ike_YYYY, ikze_YYYY; to_landing/knf_insurance).
Each file holds its own period, so the newest copy of every entity is loaded (latest-only per entity).

A workbook whose sheets do not carry the layout its series is known by (LAYOUTS: a sheet name pattern and a text
that must appear on it) is rejected into bronze.knf_insurance_rejected_files with the reason, never guessed.
"""

from __future__ import annotations

import re
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze, xlsx_cells

SOURCE = "knf_insurance"
TABLE = "knf_insurance_cells"
REJECTED = "knf_insurance_rejected_files"
# series → (sheet name pattern, text pattern that must be in a cell of a matching sheet); from the trial's profile.
LAYOUTS: dict[str, tuple[re.Pattern[str], re.Pattern[str]]] = {
    "ofe": (re.compile(r".*"), re.compile(r"OFE|fundusz", re.IGNORECASE)),
    "bulletin": (re.compile(r".*"), re.compile(r"ubezpiecze|składk", re.IGNORECASE)),
    "ike": (re.compile(r".*"), re.compile(r"IKE", re.IGNORECASE)),
    "ikze": (re.compile(r".*"), re.compile(r"IKZE", re.IGNORECASE)),
}


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    series = receipt["entity"].split("_", 1)[0]
    if series not in LAYOUTS:
        raise ValueError(f"unknown series {series!r} of entity {receipt['entity']!r}")
    table = xlsx_cells.decode(body, receipt)
    sheet_pattern, marker = LAYOUTS[series]
    names = sorted(set(table.column("sheet_name").to_pylist()))
    matching = [s for s in names if sheet_pattern.search(s)]
    if not matching:
        raise ValueError(f"{series}: no sheet matches {sheet_pattern.pattern!r}; sheets {names}")
    texts = (
        t
        for s, t in zip(table.column("sheet_name").to_pylist(), table.column("value_text").to_pylist(), strict=True)
        if s in matching
    )
    if not any(marker.search(t) for t in texts):
        raise ValueError(f"{series}: no cell matches {marker.pattern!r} on sheets {matching}")
    return table


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: workbooks of 30-70 kB."""
    from ingestion.to_landing import knf_insurance as landing

    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE),
        (namespace, REJECTED),
        xlsx_cells.COLUMNS,
        decode,
        "cells",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=[{"entity": e} for e in sorted(landing.held_entities(landing_root))],
    )
