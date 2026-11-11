#!/usr/bin/env python3
"""MRPiPS registered unemployment bronze.load: newest copy of each month's ZIP → bronze.mrpips_unemployment_cells.

One row per non-empty cell (ingestion/common/xlsx_cells.py: value as stored, sheet, row, column, number format) of
two workbooks of the monthly ZIP "statystyki strukturalne <month> <year>":

  rate workbook "Stopa bezrobocia rejestrowanego <MM_YYYY | MM.YYYY>.xlsx" (until January 2023 "stopa-pow-<…>.xlsx"):
        registered unemployed (thousands) and registered unemployment rate (%) at the end of the month for Poland,
        the 16 voivodeships and every powiat; columns WOJ. (TERYT voivodeship code), POW. (powiat code, 00 = the
        voivodeship), name, thousands, rate
  report workbook "MRPiPS-01_Tablice wynikowe <…>.xlsx" (earlier "MRiPS01_TABLICE_WYNIKOWE_<…>.xlsx"), sheets TAB 1
        and TAB 2: TAB 1 national inflows, outflows and the end-of-month stock by category and sex; TAB 2 the
        end-of-month stock by category for Poland and the voivodeships, in sections Ogółem / Kobiety

The rate workbook is required and must carry its header row (``WOJ.`` | ``POW.`` in columns A and B); a ZIP
without it, or with a different header, is rejected (bronze.mrpips_unemployment_rejected_files), never guessed.
The PDF report, ``Dane_*.xlsx``, ``Sygnalna``, ``Dane 1990-*.xls``/``.mht`` are not loaded. ZIPs whose rate table is
only in the old Excel format (``stopa pow. MM.YYYY.xls``, January 2019 - August 2020) are rejected with that reason.
"""

from __future__ import annotations

import io
import re
from typing import Any
import zipfile

import pyarrow as pa

from ingestion.common import bulk_bronze, xlsx_cells

SOURCE = "mrpips_unemployment"
TABLE_ID = ("bronze", "mrpips_unemployment_cells")
REJECTED_ID = ("bronze", "mrpips_unemployment_rejected_files")
RATE = re.compile(r"/?(Stopa bezrobocia rejestrowanego[^/]*|stopa[ ._-]*pow[^/]*)\.xlsx$", re.IGNORECASE)
REPORT = re.compile(r"Tablice[ _]wynikowe[^/]*\.xlsx$", re.IGNORECASE)
REPORT_SHEETS = re.compile(r"^TAB [12]$")
# The shared workbook-cell shape (ingestion/common/xlsx_cells.py) plus the workbook and its ZIP member.
COLUMNS = [
    *xlsx_cells.COLUMNS[:1],
    ("workbook", "VARCHAR"),  # rate | report
    ("member", "VARCHAR"),  # file name inside the ZIP, as stored
    *xlsx_cells.COLUMNS[1:],
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def _member(archive: zipfile.ZipFile, pattern: re.Pattern[str]) -> zipfile.ZipInfo | None:
    found = [i for i in archive.infolist() if pattern.search(i.filename)]
    if len(found) > 1:
        raise ValueError(f"more than one member matches {pattern.pattern!r}: {[i.filename for i in found]}")
    return found[0] if found else None


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    try:
        archive = zipfile.ZipFile(io.BytesIO(body))
    except zipfile.BadZipFile as exc:
        raise ValueError(f"not a ZIP: {exc}") from exc
    rate = _member(archive, RATE)
    if rate is None:
        raise ValueError(f"no rate workbook in the ZIP: {[i.filename for i in archive.infolist()]}")
    base = bulk_bronze.provenance(receipt)
    rows: list[dict[str, Any]] = []
    header = False
    for workbook, info, sheets in (("rate", rate, None), ("report", _member(archive, REPORT), REPORT_SHEETS)):
        if info is None:
            continue
        for cell in xlsx_cells.cells(archive.read(info)):
            if sheets is not None and not sheets.search(cell["sheet_name"]):
                continue
            if workbook == "rate" and cell["column_number"] == 1 and cell["value_text"].strip() == "WOJ.":
                header = True
            rows.append(
                {**base, **cell, "workbook": workbook, "member": info.filename, "source_row_number": len(rows) + 1}
            )
    if not header:
        raise ValueError(f"rate workbook {rate.filename!r} has no 'WOJ.' header cell in column A")
    return pa.Table.from_pylist(rows, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: one ZIP of about 3 MB per month, two small workbooks decoded."""
    from ingestion.to_landing import mrpips_unemployment as landing

    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "cells",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=[{"entity": e} for e in sorted(landing.held_entities(landing_root))],
    )
