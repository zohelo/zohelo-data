#!/usr/bin/env python3
"""GIS sanitary report bronze.load: newest Landing copy of every edition → bronze.gis_sanitary_cells.

Each Landing file is one annual report PDF ("Stan sanitarny kraju" 2012-2023, "Raport z działań Państwowej
Inspekcji Sanitarnej" from 2024). Its text layer is read with pypdf (pinned in requirements.txt, so the same bytes
always give the same text): every page in plain mode, and in layout mode only the pages whose plain text names
"krajowe" (the table's header; the layout read is the slow part). tables.py reads the food-samples table from them:
one row per value cell, as printed, with page, row and column position, row label (the year) and column header. A
file that is not a PDF, whose text cannot be read or whose table fails a check is rejected with the reason into
bronze.gis_sanitary_rejected_files; nothing of it is loaded. Shared loader: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

import io
import re
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze
from ingestion.to_bronze.gis_sanitary import tables

SOURCE = "gis_sanitary"
TABLE_ID = ("bronze", "gis_sanitary_cells")
REJECTED_ID = ("bronze", "gis_sanitary_rejected_files")
FIELDS = [
    ("table_title", "VARCHAR"),
    ("page_number", "BIGINT"),
    ("row_number", "BIGINT"),
    ("column_number", "BIGINT"),
    ("row_label", "VARCHAR"),
    ("column_header", "VARCHAR"),
    ("column_code", "VARCHAR"),
    ("value_text", "VARCHAR"),
]
COLUMNS = [("entity", "VARCHAR"), *FIELDS, *bulk_bronze.PROVENANCE]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def report_year(entity: str) -> int:
    """The edition's report year, from the entity name ``ssk_<year>`` or ``pis_<year>``."""
    match = re.fullmatch(r"(?:ssk|pis)_(\d{4})", entity)
    if not match:
        raise ValueError(f"entity {entity!r} is not ssk_<year> or pis_<year>")
    return int(match.group(1))


def page_texts(body: bytes) -> list[str]:
    """Layout text of the pages that can hold the table, '' for the others; a reading error is a rejection."""
    from pypdf import PdfReader
    from pypdf.errors import PyPdfError

    if not body.startswith(b"%PDF-"):
        raise ValueError(f"not a PDF file (starts {body[:16]!r})")
    try:
        reader = PdfReader(io.BytesIO(body))
        return [
            page.extract_text(extraction_mode="layout") if "krajowe" in (page.extract_text() or "").lower() else ""
            for page in reader.pages
        ]
    except PyPdfError as exc:
        raise ValueError(f"PDF text layer not readable: {type(exc).__name__}: {exc}") from exc


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    cells = tables.read_report(page_texts(body), report_year(receipt["entity"]))
    count = len(cells)
    prov = bulk_bronze.provenance(receipt)
    data: dict[str, list[Any]] = {
        name: [prov[name]] * count for name in ("entity", "raw_sha256", "source_version", "ingested_at")
    }
    for name, _ in FIELDS:
        data[name] = [cell[name] for cell in cells]
    return pa.Table.from_pydict({name: data[name] for name, _ in COLUMNS}, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: 14 PDF files, each read in about a minute, and only new files are read."""
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "cells",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
    )
