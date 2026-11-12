#!/usr/bin/env python3
"""URE price indicators bronze.load: newest Landing copy of each indicator page → bronze.ure_table_cells.

Each entity is one public HTML page of www.ure.gov.pl ("Ceny i wskaźniki") whose article holds the indicator's
whole series as an HTML table. The page is parsed with the standard library (html.parser); Bronze keeps one row per
table cell of the indicator table, as source text (decimal comma, footnote marks and units as printed), with its
table, row and column number, the cell tag (th | td), colspan and rowspan, and the page's title, publication date
and modification date ("Data publikacji" / "Data modyfikacji").

Every entity declares the layout it is loaded with (LAYOUTS: the text of the indicator table's header row). A page
whose tables do not match it, a page without a modification date, or an undeclared entity is rejected
(bronze.ure_rejected_files) with the tables it found, so a changed page is never loaded silently or guessed.
"""

from __future__ import annotations

from html.parser import HTMLParser
import re
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "ure"
TABLE_ID = ("bronze", "ure_table_cells")
REJECTED_ID = ("bronze", "ure_rejected_files")
# entity → the indicator table's layout, read in Dev trial 43286298849: ``header`` regexes the header rows' text
# (cells joined by " | ") must match in order (re.search), ``width`` the cell count of every data row (first cell a
# year). Pages 8425, 10600, 9165 and the gas page 7859 have other layouts (not loaded; docs/sources.md).
LAYOUTS: dict[str, dict[str, Any]] = {
    "page_7852": {  # average sale price on the competitive market: year | yearly | Q1..Q4 | URE notice numbers
        "header": [
            r"^Rok \| Średnia roczna cena sprzedaży energii elektrycznej na rynku konkurencyjnym \[zł/MWh\] \| "
            r"Średnia kwartalna cena sprzedaży energii elektrycznej na rynku konkurencyjnym[^|]* \| Nr Informacji",
            r"^I kwartał \| II kwartał \| III kwartał \| IV kwartał$",
        ],
        "width": 7,
    },
    "page_7853": {  # household price including the distribution charge: year | price | URE notice number
        "header": [
            r"^Rok \| Cena energii elektrycznej dla odbiorcy w gospodarstwie domowym[^|]*\[zł/kWh\] \| Nr Informacji"
        ],
        "width": 3,
    },
}
YEAR = re.compile(r"^\d{4}$")
COLUMNS = [
    ("entity", "VARCHAR"),
    ("page_title", "VARCHAR"),  # <title> of the page, as published
    ("page_published", "VARCHAR"),  # "Data publikacji", dd.mm.yyyy
    ("page_modified", "VARCHAR"),  # "Data modyfikacji", dd.mm.yyyy
    ("table_number", "BIGINT"),  # 1-based, in page order, among all tables of the page
    ("row_number", "BIGINT"),  # 1-based within the table
    ("column_number", "BIGINT"),  # 1-based position of the cell in its row (colspan not expanded)
    ("cell_tag", "VARCHAR"),  # th | td
    ("colspan", "BIGINT"),
    ("rowspan", "BIGINT"),
    ("value_text", "VARCHAR"),  # cell text, whitespace collapsed
    *bulk_bronze.PROVENANCE,
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)
SKIP = {"script", "style", "noscript"}
DATE = r"\s*:?\s*(\d{2}\.\d{2}\.\d{4})"


class _Page(HTMLParser):
    """Tables (rows of cells) and visible text of an HTML page."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tables: list[list[list[dict[str, Any]]]] = []
        self.stack: list[int] = []  # open tables (index into self.tables), innermost last
        self.cell: dict[str, Any] | None = None
        self.skip = 0
        self.title: list[str] = []
        self.in_title = False
        self.text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        a = dict(attrs)
        if tag in SKIP:
            self.skip += 1
        elif tag == "title":
            self.in_title = True
        elif tag == "table":
            self.tables.append([])
            self.stack.append(len(self.tables) - 1)
        elif tag == "tr" and self.stack:
            self.tables[self.stack[-1]].append([])
        elif tag in ("td", "th") and self.stack:
            rows = self.tables[self.stack[-1]]
            if not rows:
                rows.append([])
            self.cell = {"tag": tag, "text": [], "colspan": _int(a.get("colspan")), "rowspan": _int(a.get("rowspan"))}
            rows[-1].append(self.cell)
        elif tag == "br":
            self._data(" ")

    def handle_endtag(self, tag: str) -> None:
        if tag in SKIP:
            self.skip = max(0, self.skip - 1)
        elif tag == "title":
            self.in_title = False
        elif tag == "table" and self.stack:
            self.stack.pop()
            self.cell = None
        elif tag in ("td", "th"):
            self.cell = None
        elif tag in ("p", "div", "li"):
            self._data(" ")

    def handle_data(self, data: str) -> None:
        if self.in_title:
            self.title.append(data)
        self._data(data)

    def _data(self, data: str) -> None:
        if self.skip:
            return
        self.text.append(data)
        if self.cell is not None:
            self.cell["text"].append(data)


def _int(value: str | None) -> int:
    try:
        return max(1, int(str(value).strip()))
    except (TypeError, ValueError):
        return 1


def clean(text: str) -> str:
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


def parse(body: bytes) -> dict[str, Any]:
    """Title, publication and modification date, and every table (rows of cells with clean text) of a page."""
    page = _Page()
    page.feed(body.decode("utf-8"))
    page.close()
    text = clean("".join(page.text))
    published = re.search(r"Data publikacji" + DATE, text)
    modified = re.search(r"Data modyfikacji" + DATE, text)
    tables = [
        [[{**c, "text": clean("".join(c["text"]))} for c in row] for row in table if row] for table in page.tables
    ]
    return {
        "title": clean("".join(page.title)),
        "published": published.group(1) if published else None,
        "modified": modified.group(1) if modified else None,
        "tables": tables,
    }


def header(table: list[list[dict[str, Any]]]) -> str:
    return " | ".join(c["text"] for c in table[0]) if table else ""


def fits(table: list[list[dict[str, Any]]], layout: dict[str, Any]) -> bool:
    """The header rows match the layout's patterns and every other row is a year row of the layout's width."""
    heads = layout["header"]
    if len(table) <= len(heads):
        return False
    if not all(re.search(p, " | ".join(c["text"] for c in row)) for p, row in zip(heads, table, strict=False)):
        return False
    return all(YEAR.match(row[0]["text"]) and len(row) == layout["width"] for row in table[len(heads) :])


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    page = parse(body)
    entity = receipt["entity"]
    found = [f"{n}: {len(t)} rows, header {header(t)[:160]!r}" for n, t in enumerate(page["tables"], 1)]
    if entity not in LAYOUTS:
        raise ValueError(f"no layout declared for entity {entity!r}; tables: {found}"[:990])
    if not page["modified"]:
        raise ValueError("no 'Data modyfikacji' on the page")
    layout = LAYOUTS[entity]
    matched = [n for n, t in enumerate(page["tables"], 1) if fits(t, layout)]
    if len(matched) != 1:
        raise ValueError(f"{len(matched)} tables match the layout of {entity}; tables: {found}"[:990])
    number = matched[0]
    base = {
        **bulk_bronze.provenance(receipt),
        "page_title": page["title"],
        "page_published": page["published"],
        "page_modified": page["modified"],
        "table_number": number,
    }
    rows = [
        {
            **base,
            "row_number": r,
            "column_number": c,
            "cell_tag": cell["tag"],
            "colspan": cell["colspan"],
            "rowspan": cell["rowspan"],
            "value_text": cell["text"],
        }
        for r, row in enumerate(page["tables"][number - 1], 1)
        for c, cell in enumerate(row, 1)
    ]
    return pa.Table.from_pylist(rows, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: a few HTML pages of under 100 kB."""
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "table_cells",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
    )
