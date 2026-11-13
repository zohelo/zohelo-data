#!/usr/bin/env python3
"""ZUS social insurance yearbook bronze.load: newest Landing copy of each edition → bronze.zus_yearbook_lines.

The yearbook is a PDF with a text layer. Bronze keeps that text as the source shows it: one row per non-empty text line
of each page, read by pypdf in layout mode (columns stay apart as runs of spaces), with the page number, the line
number on the page and the line as text. Silver finds its tables in these lines by title and column header
(warehouse/models/silver/zus_yearbook/). A file that is not a PDF, is encrypted, or has pages without a text layer
beyond MAX_EMPTY_SHARE (a scanned edition) is rejected (bronze.zus_yearbook_rejected_files), never guessed.
"""

from __future__ import annotations

import io
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "zus_yearbook"
TABLE = "zus_yearbook_lines"
REJECTED = "zus_yearbook_rejected_files"
MAX_EMPTY_SHARE = 0.2  # blank pages and full-page charts exist; a scanned edition has almost no text
COLUMNS = [
    ("entity", "VARCHAR"),
    ("page_number", "BIGINT"),
    ("line_number", "BIGINT"),
    ("line_text", "VARCHAR"),
    *bulk_bronze.PROVENANCE,
]


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    import pypdf
    from pypdf.errors import PdfReadError

    if not body.startswith(b"%PDF"):
        raise ValueError(f"not a PDF file: starts {body[:8]!r}")
    try:
        reader = pypdf.PdfReader(io.BytesIO(body))
        if reader.is_encrypted:
            raise ValueError("encrypted PDF")
        pages, lines, numbers, empty = [], [], [], 0
        for page_number, page in enumerate(reader.pages, start=1):
            text = page.extract_text(extraction_mode="layout") or ""
            kept = [line.rstrip() for line in text.splitlines() if line.strip()]
            empty += not kept
            for line_number, line in enumerate(kept, start=1):
                pages.append(page_number)
                numbers.append(line_number)
                lines.append(line)
    except PdfReadError as exc:
        raise ValueError(f"unreadable PDF: {exc}") from exc
    total = len(reader.pages)
    if not total or empty / total > MAX_EMPTY_SHARE:
        raise ValueError(f"unknown layout: {empty} of {total} pages without a text layer")
    prov = bulk_bronze.provenance(receipt)
    n = len(lines)
    return pa.table(
        {
            "entity": [prov["entity"]] * n,
            "page_number": pages,
            "line_number": numbers,
            "line_text": lines,
            "raw_sha256": [prov["raw_sha256"]] * n,
            "source_version": [prov["source_version"]] * n,
            "ingested_at": [prov["ingested_at"]] * n,
        },
        schema=bulk_bronze.arrow_schema(COLUMNS),
    )


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: a handful of editions of 2-8 MB, one new every three years."""
    from ingestion.to_landing import zus_yearbook as landing

    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE),
        (namespace, REJECTED),
        COLUMNS,
        decode,
        "lines",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=[{"entity": e} for e in sorted(landing.held_entities(landing_root))],
    )
