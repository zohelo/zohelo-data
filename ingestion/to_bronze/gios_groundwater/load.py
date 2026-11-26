#!/usr/bin/env python3
"""GIOŚ groundwater bronze.load: newest Landing copy of each result file → bronze.gios_groundwater_records.

A result file (ingestion/to_landing/gios_groundwater/) is one table: row 1 the column names, then one row per water
sample (monitoring point x sampling date). The first columns describe the point and the sample (ATTRIBUTES, matched
by name after collapsing white space, never by position); every column after ``Data poboru próbki`` is one indicator
(name and unit as published, e.g. ``Azotany [mgNO3/l]``). 2019 and 2020 are CSV (UTF-8, comma-separated, decimal
comma in quotes), 2021 on XLSX with one sheet named after the year (read with ingestion/common/xlsx_cells.py: strings
as stored, numbers as the stored text, dates as Excel serial numbers). Layouts seen (Dev probe 45810873298): 72 or 90
columns 2019-2021 (point numbers by MONBADA, SOH/SOBWP and CBDH, groundwater bodies of the 161 and 172 division);
76 or 319 columns 2022-2024 (adds the 174 division and the ID Monitoring point number).

Two kinds of Bronze rows, values as published text:

- ``sample``: one per data row, the attribute columns (an attribute the layout lacks is NULL);
- ``value``: one per non-empty indicator cell: the sheet row, the indicator column name and the cell text
  (numbers, ``<0,00005`` below the limit of quantification, ``n.o.`` not determined; nothing is interpreted here).

Rejected (bronze.gios_groundwater_rejected_files): a column name before ``Data poboru próbki`` that is not in
ATTRIBUTES, a missing required column, an empty or repeated indicator name, a row of another width (CSV), more or
fewer than one sheet with cells (XLSX), a ``Rok badań`` other than the entity's year, or a data row without a
sampling date or without both point numbers (MONBADA and ID Monitoring).
"""

from __future__ import annotations

import csv
import io
import re
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze, xlsx_cells

SOURCE = "gios_groundwater"
TABLE = "gios_groundwater_records"
REJECTED = "gios_groundwater_rejected_files"
SAMPLE_DATE = "Data poboru próbki"
# Published column name (white space collapsed) → Bronze column.
ATTRIBUTES = {
    "L.p.": "row_label",
    "Numer JCWPd (wg podziału na 161 części)": "jcwpd161_number",
    "Kod UE JCWPd (wg podziału na 161 części)": "jcwpd161_code",
    "Identyfikator UE punktu pomiarowego (wg podziału na 161 części)": "point_eu_id_161",
    "Numer JCWPd (wg podziału na 172 części)": "jcwpd172_number",
    "Kod UE JCWPd (wg podziału na 172 części)": "jcwpd172_code",
    "Identyfikator UE punktu pomiarowego (wg podziału JCWPd na 172 części)": "point_eu_id_172",
    "Numer JCWPd (wg podziału na 174 części)": "jcwpd174_number",
    "Kod UE JCWPd (wg podziału na 174 części)": "jcwpd174_code",
    "Identyfikator UE punktu pomiarowego (wg podziału JCWPd na 174 części)": "point_eu_id_174",
    "Numer punktu pomiarowego wg ID Monitoring": "point_id_monitoring",
    "Numer punktu pomiarowego wg MONBADA": "point_monbada",
    "Numer punktu pomiarowego wg SOH/SOBWP": "point_soh",
    "Numer punktu pomiarowego wg CBDH": "point_cbdh",
    "PUWG 1992 X": "puwg1992_x",
    "PUWG 1992 Y": "puwg1992_y",
    "Województwo": "voivodeship",
    "Powiat": "powiat",
    "Gmina": "gmina",
    "Miejscowość": "locality",
    "Nazwa dorzecza": "river_basin",
    "RZGW": "rzgw",
    "Stratygrafia": "stratigraphy",
    "Głębokość do stropu warstwy wodonośnej [m p.p.t.]": "aquifer_top_depth_m",
    "Przedział ujętej warstwy wodonośnej [m p.p.t.]": "screened_interval_m",
    "Zwierciadło wody": "water_table",
    "Typ ośrodka wodonośnego": "aquifer_medium",
    "Rodzaj punktu pomiarowego": "point_kind",
    "Użytkowanie terenu": "land_use",
    "Rok badań": "survey_year",
    SAMPLE_DATE: "sample_date",
}
REQUIRED = (
    "Numer punktu pomiarowego wg MONBADA",
    "PUWG 1992 X",
    "PUWG 1992 Y",
    "Województwo",
    "Rok badań",
    SAMPLE_DATE,
)
FIELDS = list(ATTRIBUTES.values())
COLUMNS = [
    ("entity", "VARCHAR"),
    ("record_kind", "VARCHAR"),
    ("sheet_row", "BIGINT"),
    *[(c, "VARCHAR") for c in FIELDS],
    ("indicator", "VARCHAR"),
    ("value", "VARCHAR"),
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def label(text: str | None) -> str:
    return " ".join((text or "").split())


def table(body: bytes) -> list[tuple[int, list[str]]]:
    """(sheet row number, cells as text) of the file's one table, header first; empty rows left out."""
    if body[:4] == b"PK\x03\x04":
        grid: dict[str, dict[int, dict[int, str]]] = {}
        for c in xlsx_cells.cells(body):
            grid.setdefault(c["sheet_name"], {}).setdefault(c["row_number"], {})[c["column_number"]] = c["value_text"]
        if len(grid) != 1:
            raise ValueError(f"expected one sheet with cells, found {sorted(grid)}")
        rows = next(iter(grid.values()))
        width = max(max(r) for r in rows.values())
        return [(n, [rows[n].get(c, "") for c in range(1, width + 1)]) for n in sorted(rows)]
    text = body.decode("utf-8-sig")
    out = [(n, row) for n, row in enumerate(csv.reader(io.StringIO(text), delimiter=","), start=1) if any(row)]
    widths = {len(row) for _, row in out}
    if len(widths) != 1:
        raise ValueError(f"CSV rows of different widths: {sorted(widths)}")
    return out


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    year = receipt["entity"][:4]
    rows = table(body)
    if not rows:
        raise ValueError("empty file")
    header = [label(h) for h in rows[0][1]]
    if SAMPLE_DATE not in header:
        raise ValueError(f"no column {SAMPLE_DATE!r}: {header[:5]}")
    split = header.index(SAMPLE_DATE) + 1
    unknown = [h for h in header[:split] if h not in ATTRIBUTES]
    if unknown:
        raise ValueError(f"unknown attribute columns {unknown}")
    missing = [h for h in REQUIRED if h not in header[:split]]
    if missing:
        raise ValueError(f"required columns missing {missing}")
    indicators = header[split:]
    if any(not h for h in indicators) or len(set(indicators)) != len(indicators):
        raise ValueError(
            f"empty or repeated indicator names: {[h for h in indicators if not h or indicators.count(h) > 1]}"
        )
    position = {ATTRIBUTES[h]: i for i, h in enumerate(header[:split])}
    out: list[dict[str, Any]] = []
    for sheet_row, cells in rows[1:]:
        sample = {f: (cells[position[f]].strip() or None) if f in position else None for f in FIELDS}
        if sample["survey_year"] is None or re.sub(r"\.0$", "", sample["survey_year"]) != year:
            raise ValueError(f"row {sheet_row}: survey year {sample['survey_year']!r} is not {year}")
        if sample["sample_date"] is None:
            raise ValueError(f"row {sheet_row}: no sampling date")
        if sample["point_monbada"] is None and sample["point_id_monitoring"] is None:
            raise ValueError(f"row {sheet_row}: no MONBADA and no ID Monitoring point number")
        out.append({"record_kind": "sample", "sheet_row": sheet_row, **sample})
        for name, text in zip(indicators, cells[split:], strict=False):
            if text.strip():
                out.append({"record_kind": "value", "sheet_row": sheet_row, "indicator": name, "value": text})
    base = bulk_bronze.provenance(receipt)
    for n, row in enumerate(out, start=1):
        row.update(base, source_row_number=n)
    return pa.Table.from_pylist(out, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: a dozen files of under 1 MB."""
    del budget_minutes
    from ingestion.to_landing import gios_groundwater as landing

    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE),
        (namespace, REJECTED),
        COLUMNS,
        decode,
        "records",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=[{"entity": e} for e in sorted(landing.held(landing_root))],
    )
