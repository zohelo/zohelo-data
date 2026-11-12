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
# Layout per series (from the trial's profile, Dev trial 43285716017): each (sheet name pattern, text pattern) must
# be met by at least one cell of a sheet whose name matches. Bulletin part A also needs the four tables Silver reads.
TITLE = r"^Table A\.[0-9]+\. {} of {} Insurance Companies\s*$"
LAYOUTS: dict[str, list[tuple[str, str]]] = {
    "ofe": [(r"^Aktywa netto$", r"^Stan na dzień"), (r"^Członkowie$", r"^Stan na dzień")],
    "bulletin_a": [
        (r"^Tabl\.A\.", TITLE.format("Premiums", "Life")),
        (r"^Tabl\.A\.", TITLE.format("Premiums", "Non-Life")),
        (r"^Tabl\.A\.", TITLE.format("Claims", "Life")),
        (r"^Tabl\.A\.", TITLE.format("Claims", "Non-Life")),
    ],
    "bulletin": [(r"^Tytuł części", r"^Biuletyn Kwartalny\. Rynek ubezpieczeń")],
    "ike": [(r"^IKE_dane liczbowe$", r"^Informacje liczbowe o IKE$")],
    "ikze": [(r"^IKZE_dane liczbowe$", r"^Informacje liczbowe o IKZE$")],
}


def layout(entity: str) -> str:
    series = entity.split("_", 1)[0]
    return "bulletin_a" if series == "bulletin" and entity.endswith("_a") else series


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    kind = layout(receipt["entity"])
    if kind not in LAYOUTS:
        raise ValueError(f"unknown series of entity {receipt['entity']!r}")
    table = xlsx_cells.decode(body, receipt)
    cells = list(zip(table.column("sheet_name").to_pylist(), table.column("value_text").to_pylist(), strict=True))
    for sheet_pattern, text_pattern in LAYOUTS[kind]:
        sheet_re, text_re = re.compile(sheet_pattern), re.compile(text_pattern)
        if not any(sheet_re.search(s) and text_re.search(" ".join(t.split())) for s, t in cells):
            names = sorted({s for s, _ in cells})[:30]
            raise ValueError(f"{kind}: no sheet {sheet_pattern!r} with a cell {text_pattern!r}; sheets {names}")
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
