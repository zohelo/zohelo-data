#!/usr/bin/env python3
"""OTOMOTO Insights bronze.load: newest Landing copy of every edition → bronze.otomoto_cells.

A Landing copy (ingestion/to_landing/otomoto/) is the JSON object of the edition's Infogram page
(``window.infographicData``). The report is a dashboard of tabs: ``elements.content.content.blockOrder`` lists the
tabs in order, ``blocks[<tab>].entities`` the elements of a tab, ``entities[<id>]`` each element. A chart keeps its
data table in ``props.chartData``: ``data`` is a list of sheets, a sheet a list of rows, a row a list of cells (text,
a number, an object with ``value``, or null), ``sheetnames`` the sheets' names. A text element keeps its lines in
``props.content.blocks[].text``.

Two kinds of Bronze rows, values as published text, each with the tab's place (from 1) and the element's id:

- ``cell``: one per non-empty cell of a chart (sheet place and name, row and column from 1; row 1 is the chart's
  header row). Empty and null cells are not rows: a missing cell is no value, never 0.
- ``text``: one per text element of a tab that holds a chart, its lines joined by a space, whitespace collapsed,
  when it is at most 120 characters (the tab headings and chart titles Silver checks: "Ogłoszenia na OTOMOTO
  (Samochody używane)", "Średnia dzienna liczba aktywnych ogłoszeń"). Longer texts (the commentary) and tabs without a
  chart (the contact tab names OTOMOTO staff) are not stored.

The edition month comes from the report's title (``08_26_OTOMOTO_INSIGHTS`` → 2026-08) and must be the month of the
seed row the copy was landed for. JSON that does not parse, a title that names no edition or another month than the
seed row, a tab list that names a missing tab or element, or a chart whose data is not a list of sheets of rows rejects
the file into bronze.otomoto_rejected_files (never guessed).
"""

from __future__ import annotations

import json
import re
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze
from ingestion.to_landing.patterns import bulk_file

SOURCE = "otomoto"
TABLE = "otomoto_cells"
REJECTED = "otomoto_rejected_files"
TEXT_LIMIT = 120
COLUMNS = [
    ("entity", "VARCHAR"),
    ("edition", "VARCHAR"),  # YYYY-MM, the edition month of the report's title
    ("record_kind", "VARCHAR"),  # cell | text
    ("tab_number", "BIGINT"),  # place of the tab in blockOrder, from 1
    ("element_id", "VARCHAR"),  # id of the chart or text element in the edition
    ("sheet_number", "BIGINT"),  # cell: place of the sheet in the chart's data, from 1
    ("sheet_name", "VARCHAR"),  # cell: the chart's sheetnames entry
    ("row_number", "BIGINT"),  # cell: from 1; row 1 is the header row
    ("column_number", "BIGINT"),  # cell: from 1
    ("value", "VARCHAR"),  # the cell or the text as published
    *bulk_bronze.PROVENANCE,
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)
TITLE = re.compile(r"(\d{2})_(\d{2})_OTOMOTO_INSIGHTS")


def cell_text(cell: Any) -> str | None:
    """A cell as published text: an object's ``value``, strings unchanged, numbers as JSON text; empty is None."""
    if isinstance(cell, dict):
        cell = cell.get("value")
    if cell is None:
        return None
    if isinstance(cell, (bool, list, dict)):
        raise ValueError(f"cell is not text or a number: {str(cell)[:80]}")
    value = cell if isinstance(cell, str) else json.dumps(cell)
    return value if value.strip() else None


def element_text(props: dict[str, Any]) -> str | None:
    """The lines of a text element joined by a space, whitespace collapsed; None when it has none."""
    content = props.get("content")
    lines = content.get("blocks") if isinstance(content, dict) else None
    if not isinstance(lines, list):
        return None
    joined = " ".join(str(line.get("text") or "") for line in lines if isinstance(line, dict))
    joined = re.sub(r"\s+", " ", joined.replace(" ", " ")).strip()
    return joined or None


def edition_of(document: dict[str, Any]) -> str:
    match = TITLE.fullmatch(str(document.get("title", "")).strip())
    if not match:
        raise ValueError(f"title names no edition (MM_YY_OTOMOTO_INSIGHTS): {str(document.get('title'))[:80]}")
    return f"20{match.group(2)}-{match.group(1)}"


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    document = json.loads(body)
    if not isinstance(document, dict):
        raise ValueError("infographicData is not an object")
    edition = edition_of(document)
    seeded = {s["entity"]: str(s.get("edition")) for s in bulk_file.declaration(SOURCE)}
    if seeded.get(receipt["entity"]) != edition:
        raise ValueError(
            f"title names edition {edition}, seed row {receipt['entity']} is {seeded.get(receipt['entity'])}"
        )
    content = document["elements"]["content"]["content"]
    blocks, entities = content["blocks"], content["entities"]
    rows: list[dict[str, Any]] = []
    for tab_number, tab in enumerate(content["blockOrder"], start=1):
        elements = [(key, entities[key]) for key in blocks[tab]["entities"]]
        charts = [
            (k, e["props"]["chartData"])
            for k, e in elements
            if isinstance((e.get("props") or {}).get("chartData"), dict)
        ]
        if not charts:
            continue
        for key, element in elements:
            value = element_text(element.get("props") or {})
            if value and len(value) <= TEXT_LIMIT:
                rows.append({"record_kind": "text", "tab_number": tab_number, "element_id": key, "value": value})
        for key, data in charts:
            sheets, names = data.get("data"), data.get("sheetnames") or []
            if (
                not isinstance(sheets, list)
                or not all(isinstance(s, list) for s in sheets)
                or not isinstance(names, list)
            ):
                raise ValueError(f"chart {key}: data is not a list of sheets, or sheetnames not a list")
            for sheet_number, sheet in enumerate(sheets, start=1):
                name = cell_text(names[sheet_number - 1]) if sheet_number <= len(names) else None
                for row_number, row in enumerate(sheet, start=1):
                    if not isinstance(row, list):
                        raise ValueError(f"chart {key} sheet {sheet_number}: row {row_number} is not a list")
                    for column_number, cell in enumerate(row, start=1):
                        value = cell_text(cell)
                        if value is not None:
                            rows.append(
                                {
                                    "record_kind": "cell",
                                    "tab_number": tab_number,
                                    "element_id": key,
                                    "sheet_number": sheet_number,
                                    "sheet_name": name,
                                    "row_number": row_number,
                                    "column_number": column_number,
                                    "value": value,
                                }
                            )
    if not any(r["record_kind"] == "cell" for r in rows):
        raise ValueError("no chart cells in the edition")
    base = bulk_bronze.provenance(receipt)
    data = {c: [base[c]] * len(rows) for c in ("entity", "raw_sha256", "source_version", "ingested_at")}
    data["edition"] = [edition] * len(rows)
    for column in (
        "record_kind",
        "tab_number",
        "element_id",
        "sheet_number",
        "sheet_name",
        "row_number",
        "column_number",
        "value",
    ):
        data[column] = [r.get(column) for r in rows]
    return pa.Table.from_pydict({n: data[n] for n, _ in COLUMNS}, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: well under 1 MB per edition."""
    del budget_minutes
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE),
        (namespace, REJECTED),
        COLUMNS,
        decode,
        "cells",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
    )
