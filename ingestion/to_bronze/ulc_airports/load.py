#!/usr/bin/env python3
"""ULC airport traffic bronze.load: newest copy of each landed PDF → bronze.ulc_airports_rows.

Each PDF "statystyki wg portów lotniczych" (register PL-ENV-026) holds two to four tables: passengers handled and
passenger flight operations per Polish airport and a total row ("Suma"), in the reporting period and the same period
of the two previous years, plus two growth rates. From Q2 2023 every measure has its own table (airport name, five
values on one line); until Q1 2023 one table holds both measures (a numbered airport line, then the rows "Liczba
pasażerów" and "Liczba operacji pax"). Files of Q2-Q4 carry a cumulative table (first half, three quarters, whole
year) before the quarter's own table; Q1 files carry the quarter's table twice.

The tables are read from the PDF text layer (pypdf, layout mode: columns are separated by two or more spaces; text
printed twice at one place, as in the 2012-2013 files, is read once). One Bronze row per printed data row: the
table's title and column headers, the airport and measure labels and the five values as printed (text). A file is
rejected whole (bronze.ulc_airports_rejected_files) when a table has no title, no column years or no "Suma" row, a
data row has not exactly five values, or a line inside a table is not recognised; it is never half-read.
"""

from __future__ import annotations

import io
import re
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "ulc_airports"
TABLE_ID = ("bronze", "ulc_airports_rows")
REJECTED_ID = ("bronze", "ulc_airports_rejected_files")
COLUMNS = [
    ("entity", "VARCHAR"),  # reporting quarter of the file, <year>_q<n> (from the listing)
    ("page_number", "BIGINT"),
    ("table_number", "BIGINT"),  # tables in file order, from 1
    ("table_title", "VARCHAR"),  # title as printed, lines joined by one space
    ("period_header", "VARCHAR"),  # column header words, e.g. "pierwszy kwartał", "cały rok", "półrocze"
    ("column_labels", "VARCHAR"),  # the five column labels, "|"-separated: three years, two "<year>/<year>"
    ("row_number", "BIGINT"),  # data rows in table order, from 1
    ("airport_label", "VARCHAR"),  # as printed, with the numbering of the old layout ("1. Warszawa im. Chopina")
    ("measure_label", "VARCHAR"),  # row label of the old layout, else the measure words of the title
    ("value_1", "VARCHAR"),  # values as printed: three yearly values, then two growth rates
    ("value_2", "VARCHAR"),
    ("value_3", "VARCHAR"),
    ("value_4", "VARCHAR"),
    ("value_5", "VARCHAR"),
    ("raw_sha256", "VARCHAR"),
    ("source_version", "VARCHAR"),
    ("ingested_at", "TIMESTAMPTZ"),
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)

TITLE = re.compile(r"^Liczba (obsłużonych pasażerów|operacji pasażerskich)")
HEADER = re.compile(r"^Nazwa (portu lotniczego|Portu)\b", re.IGNORECASE)
END = re.compile(r"^(Uwaga|Źródło)\b")
OLD_MEASURE = re.compile(r"^Liczba (pasażerów|operacji pax)$")
NUMBERED = re.compile(r"^\d{1,2}\.\s*\S")
# a number, a growth rate, or a sign for no value: "-", "b/d" (brak danych), "#DZIEL/0!" (a spreadsheet's division
# by zero, in growth columns of 2013)
VALUE = re.compile(r"^(-?\d{1,3}( \d{3})*|-?\d+,\d+ ?%|-|b/d|#DZIEL/0!)$")
QUARTERS = {"pierwszy kwartał": 1, "drugi kwartał": 2, "trzeci kwartał": 3, "czwarty kwartał": 4}
YEAR = re.compile(r"^(19|20)\d\d$")
RATIO = re.compile(r"^(19|20)\d\d/(19|20)\d\d$")


def _cells(line: str) -> list[str]:
    """A layout line split into cells (two or more spaces). A cell printed twice in place reads as its text twice
    ("330 658330 658", "0,8%0,8%"): it is read once, unless it is itself a valid value or year ("2020")."""
    out = []
    for cell in re.split(r"\s{2,}", line.strip()):
        half = len(cell) // 2
        doubled = half and len(cell) % 2 == 0 and cell[:half] == cell[half:]
        if doubled and not (VALUE.match(cell) or YEAR.match(cell) or RATIO.match(cell)):
            cell = cell[:half]
        out.append(cell)
    return out


def _header(lines: list[str]) -> tuple[str, list[str]]:
    """(period words, the five column labels) from the header lines (the "Nazwa …" line and those after it)."""
    words, labels = [], []
    for line in lines:
        for token in " ".join(_cells(line)).split():
            if YEAR.match(token) or RATIO.match(token):
                labels.append(token)
            elif token.lower() not in {"nazwa", "portu", "lotniczego", "dynamika"}:
                words.append(token)
    period = " ".join(dict.fromkeys(" ".join(words).replace("kwartał ", "kwartał|").split("|")))
    years = [t for t in labels if YEAR.match(t)]
    ratios = [t for t in labels if RATIO.match(t)]
    if len(years) != 3 or len(ratios) != 2:
        raise ValueError(f"table header without three years and two growth columns: {labels}")
    return " ".join(period.split()), years + ratios


def tables(page_text: str) -> list[dict[str, Any]]:
    """The tables of one page: title, header and data rows (airport, measure, five values)."""
    found: list[dict[str, Any]] = []
    lines = [line for line in page_text.splitlines() if line.strip()]
    i = 0
    while i < len(lines):
        text = " ".join(_cells(lines[i]))
        if not TITLE.match(text):
            if found and not END.match(text):
                raise ValueError(f"line outside a table: {text[:80]!r}")
            i += 1
            continue
        title = [text]
        i += 1
        while i < len(lines) and not HEADER.match(" ".join(_cells(lines[i]))):
            title.append(" ".join(_cells(lines[i])))
            i += 1
            if len(title) > 3:
                raise ValueError(f"table title without a header: {' '.join(title)[:120]!r}")
        head = [lines[i]]
        i += 1
        while i < len(lines) and all(YEAR.match(t) or RATIO.match(t) or t == "dynamika" for t in lines[i].split()):
            head.append(lines[i])  # column labels below the header line: years, growth columns
            i += 1
        period, labels = _header(head)
        table = {"title": " ".join(title), "period": period, "labels": labels, "rows": []}
        measure = TITLE.match(title[0]).group(0)
        airport = None
        while i < len(lines):
            cells = _cells(lines[i])
            text = " ".join(cells)
            if END.match(text) or TITLE.match(text):
                break
            if len(cells) == 1 and (NUMBERED.match(text) or text == "Suma"):  # old layout: airport line
                airport = text
            elif len(cells) == 6 and all(VALUE.match(c) for c in cells[1:]):
                if OLD_MEASURE.match(cells[0]):
                    if airport is None:
                        raise ValueError(f"measure row before an airport line: {text[:80]!r}")
                    table["rows"].append((airport, cells[0], cells[1:]))
                else:
                    table["rows"].append((cells[0], measure, cells[1:]))
            else:
                raise ValueError(f"line not recognised in table {table['title'][:60]!r}: {text[:80]!r}")
            i += 1
        totals = {m for a, m, _ in table["rows"] if a == "Suma"}
        if not table["rows"] or totals != {m for _, m, _ in table["rows"]}:
            raise ValueError(f"table without a 'Suma' row for every measure: {table['title'][:80]!r}")
        found.append(table)
    return found


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    from pypdf import PdfReader
    from pypdf.errors import PdfReadError

    if not body.startswith(b"%PDF-"):
        raise ValueError("not a PDF")
    try:
        pages = [p.extract_text(extraction_mode="layout") for p in PdfReader(io.BytesIO(body)).pages]
    except PdfReadError as exc:
        raise ValueError(f"PDF not readable: {exc}") from exc
    base = bulk_bronze.provenance(receipt)
    rows: list[dict[str, Any]] = []
    number = 0
    for page_number, text in enumerate(pages, 1):
        for table in tables(text):
            number += 1
            for row_number, (airport, measure, values) in enumerate(table["rows"], 1):
                rows.append(
                    {
                        **base,
                        "page_number": page_number,
                        "table_number": number,
                        "table_title": table["title"],
                        "period_header": table["period"],
                        "column_labels": "|".join(table["labels"]),
                        "row_number": row_number,
                        "airport_label": airport,
                        "measure_label": measure,
                        **{f"value_{k}": v for k, v in enumerate(values, 1)},
                        "source_row_number": len(rows) + 1,
                    }
                )
    if not rows:
        raise ValueError("no table found in the text layer (a scanned PDF or a new layout)")
    quarters = {
        f"{r['column_labels'][:4]}_q{QUARTERS[r['period_header']]}" for r in rows if r["period_header"] in QUARTERS
    }
    if quarters != {receipt["entity"]}:  # the listing's link text and the file's own quarter table agree
        raise ValueError(
            f"quarter tables of the file ({sorted(quarters)}) differ from the listing's {receipt['entity']}"
        )
    return pa.Table.from_pylist(rows, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: about 65 PDFs of a few hundred KB, decoded in memory."""
    from ingestion.to_landing import ulc_airports as landing

    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "rows",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=[{"entity": e} for e in sorted(landing.held_entities(landing_root))],
    )
