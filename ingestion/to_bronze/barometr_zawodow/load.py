#!/usr/bin/env python3
"""Barometr Zawodów bronze.load: newest copy of each landed PDF → bronze.barometr_zawodow_occupations.

Every poster page of a file is read (posters.py): the national poster (entity plakat_<year>, one poster: Polska)
and the posters printed in the national report (raport_<year>: the 16 voivodeships, in most years Polska too). One
Bronze row per occupation printed on a poster: forecast year and area as printed in the poster's header, the
category label of the block the occupation is printed in (as printed, e.g. "DEFICYT"), the labels of all blocks of
the poster top to bottom, and the occupation name as printed (lines joined, ligatures resolved).

A file is rejected whole (bronze.barometr_zawodow_rejected_files, with the reason) when a poster page does not read
cleanly (posters.py), the posters' year differs from the entity's year, an area is printed on two posters of one
file, a report holds no voivodeship poster or the poster file not exactly one Polska poster, or the file has no
poster page at all (a scanned file or a new layout). It is never half-read.
"""

from __future__ import annotations

import io
import logging
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "barometr_zawodow"
TABLE_ID = ("bronze", "barometr_zawodow_occupations")
REJECTED_ID = ("bronze", "barometr_zawodow_rejected_files")
COLUMNS = [
    ("entity", "VARCHAR"),  # raport_<year> or plakat_<year> (file kind and forecast year, from the publication page)
    ("page_number", "BIGINT"),
    ("edition_year", "VARCHAR"),  # the year of the poster's title "Barometr zawodów <year>" (the forecast year)
    ("area_label", "VARCHAR"),  # as printed: "Polska" or "województwo <name>"
    ("category_label", "VARCHAR"),  # label of the occupation's block as printed, e.g. "DEFICYT", "RÓWNOWAGA"
    ("poster_categories", "VARCHAR"),  # labels of all blocks of the poster, top to bottom, "|"-separated
    ("item_number", "BIGINT"),  # occupations in reading order on the page (block by block, line by line), from 1
    ("occupation_name", "VARCHAR"),  # as printed, wrapped lines joined with one space
    ("raw_sha256", "VARCHAR"),
    ("source_version", "VARCHAR"),
    ("ingested_at", "TIMESTAMPTZ"),
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    from pypdf import PdfReader
    from pypdf.errors import PdfReadError

    from ingestion.to_bronze.barometr_zawodow import posters

    logging.getLogger("pypdf").setLevel(logging.ERROR)  # font warnings of the posters' CFF fonts
    if not body.startswith(b"%PDF-"):
        raise ValueError("not a PDF")
    kind, year = receipt["entity"].rsplit("_", 1)
    try:
        reader = PdfReader(io.BytesIO(body))
        pages = list(reader.pages)
    except PdfReadError as exc:
        raise ValueError(f"PDF not readable: {exc}") from exc
    base = bulk_bronze.provenance(receipt)
    rows: list[dict[str, Any]] = []
    areas: dict[str, int] = {}
    for number, page in enumerate(pages, 1):
        try:
            poster = posters.parse(page, reader)
        except ValueError as exc:
            raise ValueError(f"page {number}: {exc}") from exc
        except Exception as exc:  # a page pypdf cannot interpret rejects the file, with the reason
            raise ValueError(f"page {number}: {type(exc).__name__}: {exc}") from exc
        if poster is None:
            continue
        if poster["year"] != year:
            raise ValueError(f"page {number}: poster for {poster['year']} in the file of {year}")
        if poster["area"] in areas:
            raise ValueError(f"area {poster['area']!r} printed on pages {areas[poster['area']]} and {number}")
        areas[poster["area"]] = number
        categories = "|".join(poster["categories"])
        for item, (category, occupation) in enumerate(poster["rows"], 1):
            rows.append(
                {
                    **base,
                    "page_number": number,
                    "edition_year": poster["year"],
                    "area_label": poster["area"],
                    "category_label": category,
                    "poster_categories": categories,
                    "item_number": item,
                    "occupation_name": occupation,
                    "source_row_number": len(rows) + 1,
                }
            )
    if not rows:
        raise ValueError("no poster page found (a scanned PDF or a new layout)")
    voivodeships = [a for a in areas if a != "Polska"]
    if kind == "plakat" and list(areas) != ["Polska"]:
        raise ValueError(f"the national poster file holds the posters {sorted(areas)}, not one for Polska")
    if kind == "raport" and not voivodeships:
        raise ValueError(f"the report holds no voivodeship poster (posters: {sorted(areas)})")
    return pa.Table.from_pylist(rows, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: 22 PDFs of 0.1-11 MB, decoded in memory one at a time."""
    from ingestion.to_landing import barometr_zawodow as landing

    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "occupations",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=[{"entity": s["entity"]} for s in landing.specs()],
    )
