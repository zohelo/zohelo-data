#!/usr/bin/env python3
"""NID register of immovable monuments, bronze.load: newest Landing copy of every list → bronze.nid_monuments_items.

Each Landing file is one PDF list of a voivodeship (Warsaw separate), written in Word: every page starts with three
header lines (``woj. <voivodeship> – pow. <powiat>`` or, in the Warsaw list, ``WARSZAWA – <district>``; the page
number; ``Stan na <dd.mm.yyyy>``), then localities as headings and the monuments as dash items
(``- <object>, <address>, <dating>, nr rej.: <number> z <date>``), an item's text wrapping onto further lines and
items of an ensemble listed below it. The text layer is read with pypdf (plain extraction, page by page) and cut into
records here, nothing else: one row per dash item, its wrapped lines joined with one space, with the page header,
the date of the list and the last heading above it, all as text. The item text is not split or interpreted
(Silver does that).

How a line is read, in page order across page breaks:
- the three header lines of a page are checked (an unknown header rejects the file) and kept as context;
- a line starting with a dash (``-``, ``–``) starts an item;
- any other line directly below an item line (no blank line between, a page break is not a blank line) continues
  that item, unless the item already ends with a register date or a colon and the line reads like a heading
  (capital letter, no digit, no ``nr rej``);
- otherwise the line is a heading (a locality, a gmina, a city district) and closes the item above it.
A file with no item, a page without the header, or an item longer than 40 lines is rejected
(bronze.nid_monuments_rejected_files), never guessed.
"""

from __future__ import annotations

import io
import re
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "nid_monuments"
TABLE_ID = ("bronze", "nid_monuments_items")
REJECTED_ID = ("bronze", "nid_monuments_rejected_files")
COLUMNS = [
    ("entity", "VARCHAR"),
    ("page_number", "BIGINT"),
    ("item_number", "BIGINT"),
    ("page_header", "VARCHAR"),
    ("state_text", "VARCHAR"),
    ("heading_text", "VARCHAR"),
    ("item_text", "VARCHAR"),
    ("line_count", "BIGINT"),
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)
DASH = re.compile(r"^[-–]\s*")
PAGE_NO = re.compile(r"^\d{1,4}$")
STATE = re.compile(r"^Stan na \d{1,2}\.\d{1,2}\.\d{4}$")
HEADER = re.compile(r"^(woj\. .+ – .+|WARSZAWA – .+)$")
ENDS_COMPLETE = re.compile(r"(\d{1,2}[./]\d{1,2}[./]\s?\d{4}\)?|:)\s*$")
CONTINUES = re.compile(r"\d|nr rej|^[a-ząćęłńóśźż(,;./]|^[IVXL]+\b")
MAX_LINES = 40


def items(pages: list[str]) -> list[dict[str, Any]]:
    """Dash items of a list's pages (text of each page as pypdf extracts it), in page order."""
    out: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    heading: str | None = None
    previous_blank = True
    for number, text in enumerate(pages, start=1):
        lines = [x.strip() for x in text.splitlines()]
        head = [x for x in lines[:6] if x][:3]
        if len(head) < 3 or not HEADER.match(head[0]) or not PAGE_NO.match(head[1]) or not STATE.match(head[2]):
            raise ValueError(f"page {number}: no page header (header, page number, 'Stan na <date>'): {head!r}")
        # Drop the header lines (the first occurrence of each, in order), keep the body.
        body, wanted = [], list(head)
        for line in lines:
            if wanted and line == wanted[0]:
                wanted.pop(0)
                continue
            if wanted and not line:
                continue
            body.append(line)
        for line in body:
            if not line:
                previous_blank = True
                continue
            if DASH.match(line):
                current = {"page_number": number, "page_header": head[0], "state_text": head[2],
                           "heading_text": heading, "lines": [DASH.sub("", line)]}
                out.append(current)
            elif (
                current is not None
                and not previous_blank
                and not (ENDS_COMPLETE.search(current["lines"][-1]) and not CONTINUES.search(line))
            ):
                current["lines"].append(line)
                if len(current["lines"]) > MAX_LINES:
                    raise ValueError(f"page {number}: an item runs over {MAX_LINES} lines")
            else:
                heading, current = line, None
            previous_blank = False
    if not out:
        raise ValueError("no item")
    return out


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    from pypdf import PdfReader
    from pypdf.errors import PyPdfError

    try:
        reader = PdfReader(io.BytesIO(body))
        pages = [page.extract_text() or "" for page in reader.pages]
    except PyPdfError as exc:
        raise ValueError(f"not a readable PDF: {exc}") from exc
    found = items(pages)
    count = len(found)
    prov = bulk_bronze.provenance(receipt)
    data: dict[str, list[Any]] = {name: [prov[name]] * count for name in ("entity", "raw_sha256", "source_version",
                                                                          "ingested_at")}
    data["page_number"] = [i["page_number"] for i in found]
    data["item_number"] = list(range(1, count + 1))
    data["page_header"] = [i["page_header"] for i in found]
    data["state_text"] = [i["state_text"] for i in found]
    data["heading_text"] = [i["heading_text"] for i in found]
    data["item_text"] = [re.sub(r"\s+", " ", " ".join(i["lines"])).strip() for i in found]
    data["line_count"] = [len(i["lines"]) for i in found]
    data["source_row_number"] = list(range(1, count + 1))
    return pa.Table.from_pydict({name: data[name] for name, _ in COLUMNS}, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: 17 PDF lists of 1-2 MB, read in memory one at a time."""
    from ingestion.to_landing.nid_monuments import LISTS

    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "items",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=[{"entity": entity} for entity, _ in LISTS.values()],
    )
