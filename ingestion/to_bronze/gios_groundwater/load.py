#!/usr/bin/env python3
"""GIOŚ groundwater bronze.load: newest Landing copy of each workbook → bronze.gios_groundwater_cells.

One row per non-empty cell (ingestion/common/xlsx_cells.py: value as stored, sheet, row, column, number format) plus
the workbook kind (from the entity), the link text of the result page (receipt ``detail.link_text``) and the role of
the cell's column, read from the header in row 1:

  header             a cell of row 1 (the header text itself)
  <descriptor>       the point and sample columns, a closed list (DESCRIPTORS): lp, gwb_number_161 … gwb_code_174,
                     eu_point_id_161/172/174, point_id_monitoring, point_no_monbada, point_no_soh, point_no_cbdh,
                     x_puwg1992, y_puwg1992, voivodeship, powiat, gmina, locality, river_basin, rzgw, stratigraphy,
                     aquifer_top_depth, screen_interval, water_table, aquifer_type, point_type, land_use, study_year,
                     sample_date
  final_class        classes workbooks: the last column, "Klasa jakości <year> końcowa"
  monitoring_type    a results column whose header names the monitoring ("Monitoring diagnostyczny"; 2007)
  lab_analysis_no    a results column "Nr analizy laboratoryjnej" (the laboratory's analysis number; organic 2023)
  parameter          results workbooks: every column after the descriptors (the header is the indicator and unit)
  parameter_class    nitrate_classes: every column after the descriptors (the class of that indicator)

Every workbook has one worksheet with cells and its header in row 1 starting with "L.p."; the descriptors come first
and end with "Rok badań" / "Data poboru próbki". A file with another layout — no or several worksheets, an unknown
descriptor header, a classes workbook without its final-class column, a data cell under no header — is rejected
(bronze.gios_groundwater_rejected_files) with the reason, never guessed. Values are not interpreted here.
"""

from __future__ import annotations

import re
from typing import Any
import zipfile

import pyarrow as pa

from ingestion.common import bulk_bronze, xlsx_cells

SOURCE = "gios_groundwater"
TABLE = "gios_groundwater_cells"
REJECTED = "gios_groundwater_rejected_files"
KINDS = {"inorganic", "organic", "classes", "nitrate_inorganic", "nitrate_classes"}
# Descriptor headers (lower case, whitespace collapsed) → column role; the closed list found in every workbook
# 1991-2025 (Dev probe 43286691466). "wody" is the water-table header of the 2014 classes workbook as stored.
DESCRIPTORS = [
    (r"l\.?\s?p\.?", "lp"),
    (r"numer jcwpd \(wg podziału na (161|172|174) części\)", "gwb_number_{0}"),
    (r"kod ue jcwpd \(wg podziału na (161|172|174) części\)", "gwb_code_{0}"),
    (r"identyfikator ue punktu pomiarowego \(wg podziału (?:jcwpd )?na (161|172|174) części\)", "eu_point_id_{0}"),
    (r"(?:numer|identyfikator) punktu pomiarowego (?:wg )?id monitoring", "point_id_monitoring"),
    (r"numer punktu pomiarowego wg monbada", "point_no_monbada"),
    (r"numer punktu pomiarowego wg soh/sobwp", "point_no_soh"),
    (r"numer punktu pomiarowego wg cbdh", "point_no_cbdh"),
    (r"puwg 1992 x", "x_puwg1992"),
    (r"puwg 1992 y", "y_puwg1992"),
    (r"województwo", "voivodeship"),
    (r"powiat", "powiat"),
    (r"gmina", "gmina"),
    (r"miejscowość", "locality"),
    (r"nazwa dorzecza", "river_basin"),
    (r"rzgw", "rzgw"),
    (r"stratygrafia", "stratigraphy"),
    (r"głębokość do stropu warstwy wodonośnej \[m p\.p\.t\.\]", "aquifer_top_depth"),
    (r"przedział ujętej warstwy wodonośnej \[m p\.p\.t\.?\]", "screen_interval"),
    (r"(?:zwierciadło )?wody", "water_table"),
    (r"typ ośrodka wodonośnego", "aquifer_type"),
    (r"rodzaj punktu pomiarowego", "point_type"),
    (r"użytkowanie terenu", "land_use"),
    (r"rok badań", "study_year"),
    (r"data poboru próbki", "sample_date"),
]
DESCRIPTOR_RE = [(re.compile(f"^{p}$"), role) for p, role in DESCRIPTORS]
FINAL_CLASS = re.compile(r"^klasa jakości \d{4} końcowa$")
MONITORING = re.compile(r"monitoring")
LAB_NUMBER = re.compile(r"^nr analizy laboratoryjnej$")  # organic 2023: the laboratory's analysis number
COLUMNS = [
    *xlsx_cells.COLUMNS[:1],
    ("workbook", "VARCHAR"),  # entity kind: inorganic | organic | classes | nitrate_inorganic | nitrate_classes
    ("link_text", "VARCHAR"),  # the result page's link text of the workbook, as published
    ("column_role", "VARCHAR"),  # see the module docstring
    *xlsx_cells.COLUMNS[1:],
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def norm(text: str) -> str:
    return " ".join(text.split()).lower()


def descriptor(text: str) -> str | None:
    for pattern, role in DESCRIPTOR_RE:
        m = pattern.match(norm(text))
        if m:
            return role.format(*m.groups())
    return None


def roles(header: dict[int, str], kind: str) -> dict[int, str]:
    """Column number → role from the header row; ValueError for a layout this loader does not know."""
    columns = sorted(header)
    if not columns or columns[0] != 1 or descriptor(header[1]) != "lp":
        raise ValueError(f"row 1 does not start with 'L.p.' in column A: {header.get(1)!r}")
    out: dict[int, str] = {}
    n = 0
    while n < len(columns) and descriptor(header[columns[n]]):
        out[columns[n]] = descriptor(header[columns[n]])
        n += 1
    seen = list(out.values())
    if len(seen) != len(set(seen)):
        raise ValueError(f"a descriptor header repeats: {sorted(r for r in seen if seen.count(r) > 1)}")
    if "study_year" not in seen or "voivodeship" not in seen:
        raise ValueError(f"descriptor headers without 'Rok badań' or 'Województwo': {seen}")
    if seen[-1] not in ("study_year", "sample_date"):
        raise ValueError(
            f"the descriptors do not end with 'Rok badań' / 'Data poboru próbki': {header[columns[n - 1]]!r}"
        )
    rest = columns[n:]
    if kind == "classes":
        if len(rest) != 1 or not FINAL_CLASS.match(norm(header[rest[0]])):
            raise ValueError(
                f"classes workbook: expected one final 'Klasa jakości <year> końcowa' column after the "
                f"descriptors, found {[header[c][:60] for c in rest]}"
            )
        out[rest[0]] = "final_class"
        return out
    if not rest:
        raise ValueError("no indicator columns after the descriptors")
    for c in rest:
        if kind == "nitrate_classes":
            out[c] = "parameter_class"
        elif MONITORING.search(norm(header[c])) and "[" not in header[c]:
            out[c] = "monitoring_type"
        elif LAB_NUMBER.match(norm(header[c])):
            out[c] = "lab_analysis_no"
        else:
            out[c] = "parameter"
    return out


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    kind = receipt["entity"].rsplit("_", 1)[0]
    if kind not in KINDS:
        raise ValueError(f"unknown workbook kind {kind!r}")
    try:
        cells = list(xlsx_cells.cells(body))
    except (KeyError, zipfile.BadZipFile) as exc:
        raise ValueError(f"not an XLSX workbook: {exc}") from exc
    sheets = sorted({c["sheet_name"] for c in cells})
    if len(sheets) != 1:
        raise ValueError(f"expected one worksheet with cells, found {sheets}")
    header = {c["column_number"]: c["value_text"] for c in cells if c["row_number"] == 1}
    role = roles(header, kind)
    orphans = sorted({c["column_number"] for c in cells if c["row_number"] > 1 and c["column_number"] not in role})
    if orphans:
        raise ValueError(f"data cells under no header in columns {orphans[:10]}")
    base = {
        **bulk_bronze.provenance(receipt),
        "workbook": kind,
        "link_text": (receipt.get("detail") or {}).get("link_text"),
    }
    rows = [
        {
            **base,
            **c,
            "column_role": "header" if c["row_number"] == 1 else role[c["column_number"]],
            "source_row_number": i,
        }
        for i, c in enumerate(cells, 1)
    ]
    return pa.Table.from_pylist(rows, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: about 75 workbooks of 0.1-1.5 MB, decoded one at a time."""
    from ingestion.to_landing import gios_groundwater as landing

    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE),
        (namespace, REJECTED),
        COLUMNS,
        decode,
        "cells",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=[{"entity": e} for e in sorted(landing.held_entities(landing_root))],
    )
