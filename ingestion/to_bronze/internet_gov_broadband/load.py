#!/usr/bin/env python3
"""internet.gov.pl broadband bronze.load: newest Landing copy of every edition → bronze.internet_gov_broadband_cells.

Each Landing file is the national PDF of one quarter's report. Its text layer is read with pypdf's layout mode
(pinned in requirements.txt, so the same bytes always give the same text) and the three coverage tables are read
by tables.py: one row per value cell, as printed, with table number and title, page, row and column position, row
label, column header and the printed reference date. A file that is not a PDF, whose text cannot be read, or whose
tables fail a check (title, header, row labels, number formats, SUMA, the reference date against the edition's
quarter) is rejected with the reason into bronze.internet_gov_broadband_rejected_files; nothing of it is loaded.
Shared loader: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

import io
import re
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze
from ingestion.to_bronze.internet_gov_broadband import tables

SOURCE = "internet_gov_broadband"
TABLE_ID = ("bronze", "internet_gov_broadband_cells")
REJECTED_ID = ("bronze", "internet_gov_broadband_rejected_files")
FIELDS = [
    ("table_code", "VARCHAR"),
    ("table_number", "VARCHAR"),
    ("table_title", "VARCHAR"),
    ("page_number", "BIGINT"),
    ("row_number", "BIGINT"),
    ("column_number", "BIGINT"),
    ("row_label", "VARCHAR"),
    ("column_header", "VARCHAR"),
    ("column_code", "VARCHAR"),
    ("value_text", "VARCHAR"),
]
COLUMNS = [("entity", "VARCHAR"), ("reference_date_text", "VARCHAR"), *FIELDS, *bulk_bronze.PROVENANCE]
ARROW = bulk_bronze.arrow_schema(COLUMNS)
QUARTER_ENDS = {1: (3, 31), 2: (6, 30), 3: (9, 30), 4: (12, 31)}


def quarter_end(entity: str) -> tuple[int, int, int]:
    """(year, month, day) of the last day of the edition's quarter, from the entity name ``polska_<year>q<n>``."""
    match = re.fullmatch(r"polska_(\d{4})q([1-4])", entity)
    if not match:
        raise ValueError(f"entity {entity!r} is not polska_<year>q<quarter>")
    return (int(match.group(1)), *QUARTER_ENDS[int(match.group(2))])


def page_texts(body: bytes) -> list[str]:
    """The layout-mode text of every page; any PDF reading error is a rejection."""
    from pypdf import PdfReader
    from pypdf.errors import PyPdfError

    if not body.startswith(b"%PDF-"):
        raise ValueError(f"not a PDF file (starts {body[:16]!r})")
    try:
        reader = PdfReader(io.BytesIO(body))
        return [page.extract_text(extraction_mode="layout") for page in reader.pages]
    except PyPdfError as exc:
        raise ValueError(f"PDF text layer not readable: {type(exc).__name__}: {exc}") from exc


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    printed, cells = tables.read_report(page_texts(body), quarter_end(receipt["entity"]))
    count = len(cells)
    prov = bulk_bronze.provenance(receipt)
    data: dict[str, list[Any]] = {
        name: [prov[name]] * count for name in ("entity", "raw_sha256", "source_version", "ingested_at")
    }
    data["reference_date_text"] = [printed] * count
    for name, _ in FIELDS:
        data[name] = [cell[name] for cell in cells]
    return pa.Table.from_pydict({name: data[name] for name, _ in COLUMNS}, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: ten PDF files of about 1.4 MB, read in seconds."""
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
