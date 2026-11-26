"""ARiMR direct-payment statistics: Landing → Bronze activities (register PLS-034).

bronze.load       newest copy of every landed report → bronze.arimr_payments_cells (one row per table cell of the
                  PDF's text layer); a file whose layout is not recognised is rejected, with the reason, into
                  bronze.arimr_payments_rejected_files
bronze.verify     ingestion/common/lineage.py --source arimr_payments
landing.archive   ingestion/common/archive.py --source arimr_payments (move to Archive)
archive.prune     ingestion/common/retention.py --source arimr_payments (archive_policy.retention)

The reports are Excel sheets printed to PDF. The text layer (pypdf, layout mode, which keeps the columns apart by
runs of spaces) holds tables of one shape: a title ("Liczba złożonych wniosków o przyznanie płatności
obszarowych", "Liczba wydanych decyzji o przyznani(e|u) płatności obszarowych", "Kwota zrealizowanych płatności
w ramach płatności bezpośrednich"), "Dane na dzień <dd.mm.yyyy> r.", header lines naming one campaign year per
column ("wniosków 2021", "w kampanii 2004", "Kampania 2023**") after that date line, then the 16 voivodeships and
"Razem". The last amounts table of a report may end with a column "Razem" over "Kampanie 2004-2025", the row's total
over the campaigns; it is kept as printed (column label "Razem Kampanie ...") and Silver leaves it out. No value is
taken by its position alone; a table is accepted only when
- its header names as many columns as every row has values: campaign years, each once and in increasing order, then
  at most that one total column;
- it has each of the 16 voivodeships and "Razem" exactly once, and no other row;
- every value is a number as printed (digits grouped by spaces, or a few cells ungrouped as "126042" in the
  31.12.2024 report; decimal comma);
- in every column "Razem" equals the sum of the voivodeships (counts exactly; amounts, printed rounded to the
  złoty in some reports, within 9 zł);
- every table of the file has the same "Dane na dzień" date.
Anything else rejects the whole file. Only table cells reach Bronze: the footer lines (source system, dates, the
names of the ARiMR staff responsible for the release) stay in the Landing file.
"""

from __future__ import annotations

from decimal import Decimal
import io
import re
from typing import Any

SOURCE = "arimr_payments"
COLUMNS = [
    ("entity", "VARCHAR"),
    ("page_number", "BIGINT"),
    ("table_number", "BIGINT"),
    ("table_title", "VARCHAR"),
    ("as_of_text", "VARCHAR"),
    ("column_number", "BIGINT"),
    ("column_label", "VARCHAR"),
    ("row_label", "VARCHAR"),
    ("value_text", "VARCHAR"),
    ("raw_sha256", "VARCHAR"),
    ("source_version", "VARCHAR"),
    ("ingested_at", "TIMESTAMPTZ"),
    ("source_row_number", "BIGINT"),
]
VOIVODESHIPS = {
    "dolnośląskie", "kujawsko-pomorskie", "lubelskie", "lubuskie", "łódzkie", "małopolskie", "mazowieckie",
    "opolskie", "podkarpackie", "podlaskie", "pomorskie", "śląskie", "świętokrzyskie", "warmińsko-mazurskie",
    "wielkopolskie", "zachodniopomorskie",
}  # fmt: skip
TOTAL = "razem"
TITLE = re.compile(
    r"^\s*(Liczba złożonych wniosków o przyzna|Liczba\s*wydanych decyzji o przyzna|Kwota zrealizowanych płatności)"
)
# "Dane na dzień 30.09.2026 r."; the year-end reports 2017 to 2019 print "31-12-2018"
AS_OF = re.compile(r"Dane na dzień\s+(\d{2}[.-]\d{2}[.-]\d{4})")
YEAR = re.compile(r"(?:[Kk]ampani[ia]|wniosków)\s+((?:19|20)\d{2})(\**)(?![\d,-])")
# a last header "Razem" over "Kampanie 2004-2025": the row's total over the campaigns (amounts table of the newest
# reports; it sums 2004 to the newest campaign whatever its label says, report of 30.09.2026)
# ("Razem Kampanie 2004-2024" on one line in the 31.12.2024 report)
TOTAL_HEADER = re.compile(r"\s{2,}Razem(?:\s+(Kampanie\s+\d{4}\s*-\s*\d{4}))?\s*$")
TOTAL_SPAN = re.compile(r"^\s*(Kampanie\s+\d{4}\s*-\s*\d{4})\s*$")
ROW = re.compile(r"^\s*([A-ZŁŚŻŹĆŃÓĘĄ][a-ząćęłńóśźż]+(?:\s*-\s*[a-ząćęłńóśźż]+)?)\s{2,}(\S.*?)\s*$")
NUMBER = re.compile(r"(?:\d{1,3}(?: \d{3})*|\d+)(?:,\d{1,2})?")
AMOUNT_TOLERANCE = Decimal(9)  # 17 values rounded to the złoty: at most 8.5 zł apart


class Layout(ValueError):
    """The text layer does not have the expected table shape: the file is rejected, never guessed."""


def label(text: str) -> str:
    """A row label as a key: lower case, 'Kujawsko - pomorskie' → 'kujawsko-pomorskie'."""
    return re.sub(r"\s*-\s*", "-", text.strip()).lower()


def number(text: str) -> Decimal:
    return Decimal(text.replace(" ", "").replace(",", "."))


def check(table: dict[str, Any]) -> None:
    """The guards of a finished table (module docstring); raises Layout."""
    where = f"page {table['page']} table {table['number']} ({table['title'][:60]!r})"
    years = [y for y, _ in table["columns"] if y]
    if not years or years != sorted(set(years)):
        raise Layout(f"{where}: campaign years in the header not increasing: {years}")
    if [y for y, _ in table["columns"]][: len(years)] != years:
        raise Layout(f"{where}: a column without a campaign year before the last: {table['columns']}")
    labels = [label(r) for r, _ in table["rows"]]
    if sorted(labels) != sorted([*VOIVODESHIPS, TOTAL]):
        twice = sorted({x for x in labels if labels.count(x) > 1})
        missing = sorted({*VOIVODESHIPS, TOTAL} - set(labels))
        raise Layout(f"{where}: rows printed twice {twice}, missing {missing}")
    columns = [heading for _, heading in table["columns"]]
    for row, values in table["rows"]:
        if len(values) != len(columns):
            raise Layout(f"{where}: row {row!r} has {len(values)} values for {len(columns)} columns {columns}")
        bad = [v for v in values if not NUMBER.fullmatch(v)]
        if bad:
            raise Layout(f"{where}: row {row!r} values not numbers as printed: {bad[:3]}")
    if not table["as_of"]:
        raise Layout(f"{where}: no 'Dane na dzień' date")
    total = next(values for row, values in table["rows"] if label(row) == TOTAL)
    for i, year in enumerate(columns):
        parts = sum(number(values[i]) for row, values in table["rows"] if label(row) != TOTAL)
        tolerance = AMOUNT_TOLERANCE if "," in total[i] else Decimal(0)
        if abs(parts - number(total[i])) > tolerance:
            raise Layout(f"{where}: campaign {year}: voivodeships sum to {parts}, Razem is {total[i]}")


def parse(pages: list[str]) -> list[dict[str, Any]]:
    """The table cells of a report's text layer (one string per page), each with its page, table, title, date,
    column (campaign year and footnote marks as printed), row label and value as printed. Raises Layout."""
    tables: list[dict[str, Any]] = []
    table: dict[str, Any] | None = None
    for page_number, text in enumerate(pages, start=1):
        for line in text.replace("\xa0", " ").splitlines():  # titles of the year-end reports use no-break spaces
            if TITLE.match(line):
                if table is not None:
                    raise Layout(f"page {page_number}: a new title before 'Razem' of {table['title'][:60]!r}")
                table = {"page": page_number, "number": len(tables) + 1, "title": " ".join(line.split())}
                table.update({"as_of": None, "columns": [], "rows": []})
                continue
            if table is None:
                if ROW.match(line) and label(ROW.match(line).group(1)) in {*VOIVODESHIPS, TOTAL}:
                    raise Layout(f"page {page_number}: a table row outside a table: {line.strip()[:80]!r}")
                continue
            row = ROW.match(line)
            if row and label(row.group(1)) in {*VOIVODESHIPS, TOTAL}:
                if not table["columns"]:
                    raise Layout(f"page {page_number}: rows before any campaign year in {table['title'][:60]!r}")
                table["rows"].append((row.group(1), re.split(r"\s{2,}", row.group(2))))
                if label(row.group(1)) == TOTAL:
                    check(table)
                    tables.append(table)
                    table = None
                continue
            if table["rows"]:
                if line.strip():
                    raise Layout(f"page {page_number}: an unexpected line inside a table: {line.strip()[:80]!r}")
                continue
            as_of = AS_OF.search(line)
            if as_of:
                table["as_of"] = as_of.group(1)
            elif table["as_of"]:
                # header lines follow the date; the title's own lines ("w ramach kampanii 2021-2026") do not count
                table["columns"] += [(y, y + marks) for y, marks in YEAR.findall(line)]
                span, total = TOTAL_SPAN.match(line), TOTAL_HEADER.search(line)
                if total:
                    table["columns"].append((None, " ".join(("Razem " + (total.group(1) or "")).split())))
                elif span and table["columns"] and table["columns"][-1] == (None, "Razem"):
                    table["columns"][-1] = (None, "Razem " + " ".join(span.group(1).split()))
    if table is not None:
        raise Layout(f"table {table['title'][:60]!r} has no 'Razem' row")
    if not tables:
        raise Layout("no table found")
    dates = {t["as_of"] for t in tables}
    if len(dates) != 1:
        raise Layout(f"tables with different 'Dane na dzień' dates: {sorted(dates)}")
    cells = []
    for t in tables:
        for row, values in t["rows"]:
            for i, ((_, column_label), value) in enumerate(zip(t["columns"], values, strict=True), start=1):
                cells.append(
                    {
                        "page_number": t["page"],
                        "table_number": t["number"],
                        "table_title": t["title"],
                        "as_of_text": t["as_of"],
                        "column_number": i,
                        "column_label": column_label,
                        "row_label": " ".join(row.split()),
                        "value_text": value,
                    }
                )
    return cells


def text_pages(body: bytes) -> list[str]:
    """The text layer of every page (pypdf, layout mode: columns stay apart by runs of spaces)."""
    from pypdf import PdfReader
    from pypdf.errors import PdfReadError

    if not body.startswith(b"%PDF"):
        raise Layout("not a PDF file")
    try:
        reader = PdfReader(io.BytesIO(body))
        return [page.extract_text(extraction_mode="layout") or "" for page in reader.pages]
    except PdfReadError as exc:
        raise Layout(f"unreadable PDF: {exc}") from exc


def decode(body: bytes, receipt: dict[str, Any]):
    """bulk_bronze decoder: the table cells of one report."""
    from ingestion.common import bulk_bronze

    base = bulk_bronze.provenance(receipt)
    rows = [{**base, **cell, "source_row_number": i} for i, cell in enumerate(parse(text_pages(body)), start=1)]
    return bulk_bronze.pa.Table.from_pylist(rows, schema=bulk_bronze.arrow_schema(COLUMNS))


def bronze_load(entities: list[str], landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    from ingestion.common import bulk_bronze

    return bulk_bronze.load(
        SOURCE,
        (namespace, f"{SOURCE}_cells"),
        (namespace, f"{SOURCE}_rejected_files"),
        COLUMNS,
        decode,
        "cells",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=[{"entity": e} for e in sorted(entities)],
    )


def _entities() -> list[str]:
    from ingestion.to_landing import arimr_payments as landing

    return sorted(landing.held())


def _bronze_activities() -> dict[str, Any]:
    from ingestion.common import gus_publications

    return gus_publications.bronze_activities(SOURCE, _entities)


ACTIVITIES = {
    **_bronze_activities(),
    "bronze.load": lambda p: bronze_load(_entities()),
}
