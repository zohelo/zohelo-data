#!/usr/bin/env python3
"""ure_prices bronze.load: newest Landing copy of each indicator page → bronze.ure_prices_cells.

One row per data cell of the page's HTML tables (row label = the year, the column's header text resolved through
rowspan/colspan, the cell text as shown), with the page's "Data modyfikacji" and "Data publikacji". Every page has a
known layout in ``LAYOUTS``: the number of tables and, per table, the exact column headers. A page with another
layout, a data row of another width, a merged data cell, a row label that is not a year, a repeated year or no
modification date is rejected into bronze.ure_prices_rejected_files with the reason, never guessed. Parsed with the
standard library (html.parser). Loader: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

from html.parser import HTMLParser
import re
from typing import Any

from ingestion.common import bulk_bronze

SOURCE = "ure_prices"
TABLE = "ure_prices_cells"
REJECTED = "ure_prices_rejected_files"
COLUMNS = [
    ("entity", "VARCHAR"),
    ("page_title", "VARCHAR"),
    ("page_published", "VARCHAR"),  # "Data publikacji", DD.MM.YYYY as shown
    ("page_modified", "VARCHAR"),  # "Data modyfikacji", DD.MM.YYYY as shown
    ("table_number", "BIGINT"),  # 1-based, page order
    ("row_number", "BIGINT"),  # 1-based among the table's data rows
    ("column_number", "BIGINT"),  # 1-based; column 1 is the row label
    ("row_label", "VARCHAR"),
    ("column_header", "VARCHAR"),  # header cells above the column, top to bottom, joined by " | "
    ("cell_text", "VARCHAR"),
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
QUARTERS = ["I kwartał", "II kwartał", "III kwartał", "IV kwartał"]
COMPETITIVE_Q = "Średnia kwartalna cena sprzedaży energii elektrycznej na rynku konkurencyjnym [zł/MWh]*"
GAS_EU = "Średnie ceny zakupu gazu ziemnego z zagranicy w poszczególnych kwartałach [zł/MWh]"
GAS_NON_EU = "Średnia cena zakupu gazu ziemnego sprowadzanego z innych państw niż członkowie UE lub EFTA [zł/MWh]"
LAYOUTS = {  # entity → the column headers of each table, as read on 26 Nov 2026 (Dev probe 45790234209)
    "electricity_competitive_market": [
        [
            "Rok",
            "Średnia roczna cena sprzedaży energii elektrycznej na rynku konkurencyjnym [zł/MWh]",
            *[f"{COMPETITIVE_Q} | {q}" for q in QUARTERS],
            "Nr Informacji/Komunikatu Prezesa URE",
        ]
    ],
    "electricity_households": [
        [
            "Rok",
            "Cena energii elektrycznej dla odbiorcy w gospodarstwie domowym uwzględniająca opłatę za świadczenie usługi"
            " dystrybucji energii elektrycznej [zł/kWh]",
            "Nr Informacji/Komunikatu Prezesa URE",
        ]
    ],
    "gas_import_prices": [
        ["Rok", *[f"{GAS_EU} | {q}" for q in QUARTERS], "Nr Informacji Prezesa URE"],
        ["Rok", *[f"{GAS_NON_EU} | {q}" for q in QUARTERS], "Nr Informacji Prezesa URE"],
    ],
}


def clean(text: str) -> str:
    """Cell text as shown: non-breaking spaces as spaces, runs of white space as one, trimmed."""
    return " ".join(text.replace("\xa0", " ").split())


class _Page(HTMLParser):
    """Every <table> of the page as rows of cells (tag, text, rowspan, colspan), the <title> and the visible text."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tables: list[list[list[dict[str, Any]]]] = []
        self.depth = 0
        self.row: list[dict[str, Any]] | None = None
        self.cell: dict[str, Any] | None = None
        self.in_title, self.title, self.skip = False, "", 0
        self.text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        a = dict(attrs)
        if tag in ("script", "style"):
            self.skip += 1
        elif tag == "title":
            self.in_title = True
        elif tag == "table":
            if self.depth:
                raise ValueError("a table inside a table: unknown layout")
            self.depth, self.row = 1, None
            self.tables.append([])
        elif tag == "tr" and self.depth:
            self.row = []
        elif tag in ("td", "th") and self.row is not None:
            span = {k: str(a.get(k) or "1").strip() for k in ("rowspan", "colspan")}
            self.cell = {"tag": tag, "parts": [], **{k: int(v) if v.isdigit() else -1 for k, v in span.items()}}
        elif tag == "br" and self.cell is not None:
            self.cell["parts"].append(" ")

    def handle_endtag(self, tag: str) -> None:
        if tag in ("script", "style") and self.skip:
            self.skip -= 1
        elif tag == "title":
            self.in_title = False
        elif tag in ("td", "th") and self.cell is not None and self.row is not None:
            self.row.append({**self.cell, "text": clean("".join(self.cell.pop("parts")))})
            self.cell = None
        elif tag == "tr" and self.row is not None:
            self.tables[-1].append(self.row)
            self.row = None
        elif tag == "table" and self.depth:
            self.depth, self.row = 0, None

    def handle_data(self, data: str) -> None:
        if self.skip:
            return
        if self.in_title:
            self.title += data
        if self.cell is not None:
            self.cell["parts"].append(data)
        self.text.append(data)


def headers(rows: list[list[dict[str, Any]]]) -> list[str]:
    """The column headers of the leading rows made only of <th>, placed through rowspan and colspan."""
    grid: dict[tuple[int, int], str] = {}
    for r, row in enumerate(rows):
        c = 0
        for cell in row:
            while (r, c) in grid:
                c += 1
            if cell["rowspan"] < 1 or cell["colspan"] < 1:
                raise ValueError(f"header row {r + 1}: unreadable rowspan/colspan")
            for dr in range(cell["rowspan"]):
                for dc in range(cell["colspan"]):
                    grid[(r + dr, c + dc)] = cell["text"]
            c += cell["colspan"]
    width = 1 + max((c for _, c in grid), default=-1)
    out = []
    for c in range(width):
        parts: list[str] = []
        for r in range(len(rows)):
            text = grid.get((r, c), "")
            if text and text not in parts:
                parts.append(text)
        out.append(" | ".join(parts))
    return out


def cells(body: bytes, entity: str) -> tuple[dict[str, str], list[dict[str, Any]]]:
    """(page fields, one dict per data cell); raises ValueError on any layout the entity does not have."""
    if entity not in LAYOUTS:
        raise ValueError(f"no known layout for entity {entity!r}")
    page = _Page()
    page.feed(body.decode("utf-8"))
    page.close()
    visible = clean(" ".join(page.text))
    modified = re.findall(r"Data modyfikacji\s*:\s*(\d{2}\.\d{2}\.\d{4})", visible)
    published = re.findall(r"Data publikacji\s*:\s*(\d{2}\.\d{2}\.\d{4})", visible)
    if len(modified) != 1:
        raise ValueError(f"expected one 'Data modyfikacji' date, found {modified[:3]}")
    fields = {
        "page_title": clean(page.title),
        "page_modified": modified[0],
        "page_published": published[0] if len(published) == 1 else None,
    }
    expected = LAYOUTS[entity]
    if len(page.tables) != len(expected):
        raise ValueError(f"{len(page.tables)} tables on the page, the known layout has {len(expected)}")
    out: list[dict[str, Any]] = []
    for t, (rows, known) in enumerate(zip(page.tables, expected, strict=True), 1):
        n_head = next((i for i, row in enumerate(rows) if any(c["tag"] != "th" for c in row)), len(rows))
        found = headers(rows[:n_head])
        if found != known:
            raise ValueError(f"table {t}: column headers {found} are not the known layout {known}"[:900])
        years = set()
        for r, row in enumerate(rows[n_head:], 1):
            if any(c["tag"] != "td" or c["rowspan"] != 1 or c["colspan"] != 1 for c in row) or len(row) != len(known):
                raise ValueError(
                    f"table {t} data row {r}: {len(row)} cells or a merged/header cell, known: {len(known)}"
                )
            label = row[0]["text"]
            if not re.fullmatch(r"(19|20)\d{2}", label) or label in years:
                raise ValueError(f"table {t} data row {r}: row label {label!r} is not a new year")
            years.add(label)
            for c, cell in enumerate(row[1:], 2):
                out.append(
                    {
                        "table_number": t,
                        "row_number": r,
                        "column_number": c,
                        "row_label": label,
                        "column_header": known[c - 1],
                        "cell_text": cell["text"],
                    }
                )
        if not years:
            raise ValueError(f"table {t}: no data rows")
    return fields, out


def decode(body: bytes, receipt: dict[str, Any]):
    """bulk_bronze decoder: the page's data cells as an Arrow table of COLUMNS."""
    fields, found = cells(body, receipt["entity"])
    base = {**bulk_bronze.provenance(receipt), **fields}
    rows = [{**base, **cell, "source_row_number": i} for i, cell in enumerate(found, 1)]
    return bulk_bronze.pa.Table.from_pylist(rows, schema=bulk_bronze.arrow_schema(COLUMNS))


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: three pages of about 20-60 KB."""
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
