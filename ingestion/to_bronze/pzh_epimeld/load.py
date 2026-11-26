#!/usr/bin/env python3
"""NIZP PZH-PIB EPIMELD bronze.load: newest copy of each annual bulletin → bronze.pzh_epimeld_rows.

Each annual bulletin "Choroby zakaźne i zatrucia w Polsce w <year> roku" (Ch_<year>.pdf, register PLS-019) prints,
per disease (title in capitals with the ICD-10 codes in brackets), the table "Liczba zachorowań (w kwartałach i
ogółem), zapadalność oraz liczba i procent hospitalizowanych wg województw": a POLSKA row for the previous year and
for the bulletin's year, then one row per voivodeship (numbered 1-16 in alphabetical order; voivodeships without a
case are left out, footnote "*"), each with cases in quarters I-IV, cases in the year, incidence per 100,000 and the
number and per cent hospitalised. The bulletin's other tables (age, sex, place, season) are not read.

The tables are read from the PDF text layer (pypdf, plain mode: the voivodeship rows come column by column). The
text layer splits Polish diacritics into a letter and a spacing mark ("zachorowan´", "S˛skie"): titles and headers
are matched on a key without spaces, marks and diacritics (``key``), and labels are stored with the marks recombined
(``clean``). The page header must name the bulletin's year. A table is located by its title and its column headers
must match the expected list (quarters I-IV, cases, incidence "na 100 tys.", hospitalisation number and %); a table
without its disease title, with other headers, without the POLSKA row of the bulletin's year, with a year other than
the bulletin's or the year before, with a voivodeship number outside 1-16 or twice, a data row without exactly eight
values, or an unknown line before its first data row rejects the whole file (bronze.pzh_epimeld_rejected_files); it is
never half-read. One Bronze row per printed data row, values as printed (text).
"""

from __future__ import annotations

import io
import re
from typing import Any
import unicodedata

import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "pzh_epimeld"
TABLE_ID = ("bronze", "pzh_epimeld_rows")
REJECTED_ID = ("bronze", "pzh_epimeld_rejected_files")
COLUMNS = [
    ("entity", "VARCHAR"),  # the bulletin, ch_<year>
    ("edition_year", "BIGINT"),  # the bulletin's year (file name and page header agree)
    ("preliminary", "VARCHAR"),  # "true" when the bulletin's text says "wstępne" (preliminary data) anywhere
    ("table_number", "BIGINT"),  # voivodeship tables in file order, from 1
    ("page_number", "BIGINT"),  # page of the table's title
    ("disease_label", "VARCHAR"),  # disease title as printed, diacritics recombined
    ("disease_codes", "VARCHAR"),  # the ICD-10 codes in the title's last brackets, as printed
    ("incidence_header", "VARCHAR"),  # the incidence column's header, e.g. "na 100 tys."
    ("row_kind", "VARCHAR"),  # poland | voivodeship
    ("period_year", "BIGINT"),  # the row's year: printed on POLSKA rows, the bulletin's year on voivodeship rows
    ("area_number", "BIGINT"),  # voivodeship number 1-16 as printed (null on POLSKA rows)
    ("area_label", "VARCHAR"),  # the row label as printed, diacritics recombined where the text allows
    ("value_q1", "VARCHAR"),  # values as printed ("-" no case, "x" not applicable): cases in quarters I-IV
    ("value_q2", "VARCHAR"),
    ("value_q3", "VARCHAR"),
    ("value_q4", "VARCHAR"),
    ("value_cases", "VARCHAR"),  # cases in the year
    ("value_incidence", "VARCHAR"),  # incidence per the incidence_header denominator
    ("value_hospitalised", "VARCHAR"),  # hospitalised cases
    ("value_hospitalised_pct", "VARCHAR"),  # per cent of cases hospitalised
    ("raw_sha256", "VARCHAR"),
    ("source_version", "VARCHAR"),
    ("ingested_at", "TIMESTAMPTZ"),
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)

MARKS = "´˛˙¸˝`¨˘ˇ"  # spacing accent marks the text layer prints after (or before) the letter
COMBINE = {"´": "́", "˛": "̨", "˙": "̇"}
TABLE_KEY = "liczbazachorowan(wkwartałachiogołem),zapadalnoscorazliczbaiprocenthospitalizowanychwgwojewodztw"
# the column headers as one key (an optional "*" after "Województwo" removed); the incidence denominator is captured
HEADER = re.compile(
    r"^wojewodztwoliczbazachorowanwkwartałachliczbazachorowanwrokuzapadalnosc(na100tys.*)hospitalizacjaiiiiiiivliczba%$"
)
PAGE_HEADER = re.compile(r"^chorobyzakaznei(zatrucia)?wpolscew(\d{4})roku")
# a value as printed: a count, a decimal (comma), "-" (no case) or "x" (not applicable: no case to hospitalise)
VALUE = re.compile(r"^(-|x|\d+|\d+,\d+)$")
YEAR_ROW = re.compile(r"^((?:19|20)\d\d) r\. (.*)$")
NUMBER = re.compile(r"^(\d{1,2})\.$")
ONE_ROW = re.compile(r"^(\d{1,2})\. (\D.*?) ((?:\S+ ){7}\S+)$")  # a table with one voivodeship prints it as one line
CODES = re.compile(r"\(([^()]*)\)\s*$")


def key(text: str) -> str:
    """Matching key: no spaces, no spacing marks, no diacritics (ł kept), lower case."""
    text = "".join(c for c in unicodedata.normalize("NFKD", text) if not unicodedata.combining(c))
    return re.sub(r"\s+", "", "".join(c for c in text if c not in MARKS)).lower()


def clean(text: str) -> str:
    """A label with the spacing marks recombined with the letter before them (NFC), spaces collapsed."""
    text = re.sub(rf"\s+([{MARKS}])", r"\1", " ".join(text.split()))
    text = re.sub(rf"(\w)([{''.join(COMBINE)}])", lambda m: m.group(1) + COMBINE[m.group(2)], text)
    return unicodedata.normalize("NFC", text)


def title_like(line: str) -> bool:
    """A disease title: capitals, ends with the ICD codes in brackets."""
    text = clean(line)
    head = text[: text.rfind("(")]
    return bool(CODES.search(text)) and head.upper() == head and bool(head.strip())


def tables(lines: list[tuple[int, str]], edition: int) -> list[dict[str, Any]]:
    """The voivodeship tables of one bulletin from its plain text: [(page, line)], page headers removed.

    The plain text prints a table as: disease title, table title, header lines, "POLSKA", one line per year
    ("2024 r." and eight values), then the voivodeship rows column by column: n numbers ("7."), n names, then the n
    values of each of the eight columns. A part continued on the next page repeats that block."""
    text = [t.strip() for _, t in lines]
    found: list[dict[str, Any]] = []
    i = 0
    while i < len(text):
        if key(text[i]) != TABLE_KEY:
            i += 1
            continue
        page = lines[i][0]
        above = [t for t in text[max(0, i - 3) : i] if t]
        if not above or not title_like(above[-1]):
            raise ValueError(f"table on page {page} without a disease title above it: {above[-1:]!r}")
        title = above[-1]
        if len(above) > 1 and not CODES.search(above[-2]) and title_like(above[-2] + " (x)"):
            title = above[-2] + " " + title  # a title printed on two lines
        label = clean(title)
        table = {"page": page, "label": label, "codes": CODES.search(label).group(1).strip(), "rows": []}
        i += 1
        start = i
        while i < len(text) and key(text[i]) != "polska":
            i += 1
            if i - start > 12:
                raise ValueError(f"table {label!r} (page {page}): no POLSKA row after the headers")
        header = HEADER.match(key(" ".join(text[start:i])).replace("*", ""))
        if not header:
            raise ValueError(f"table {label!r} (page {page}): column headers differ: {key(' '.join(text[start:i]))!r}")
        zap = next(n for n in range(start, i) if key(text[n]).startswith("zapadalnosc"))
        hosp = next(n for n in range(zap, i) if key(text[n]).startswith("hospitalizacja"))
        table["incidence"] = clean(" ".join(text[zap + 1 : hosp]))
        i += 1
        while i < len(text) and YEAR_ROW.match(text[i]):
            year, rest = YEAR_ROW.match(text[i]).groups()
            values = rest.split()
            if len(values) != 8 or not all(VALUE.match(v) for v in values) or int(year) not in (edition - 1, edition):
                raise ValueError(f"table {label!r} (page {page}): POLSKA row not read: {text[i]!r}")
            table["rows"].append(("poland", int(year), None, "POLSKA", values))
            i += 1
        years = [r[1] for r in table["rows"]]
        if edition not in years or len(years) != len(set(years)):
            raise ValueError(f"table {label!r} (page {page}): POLSKA rows {years}, expected one for {edition}")
        numbers: list[int] = []
        while i < len(text) and (NUMBER.match(text[i]) or ONE_ROW.match(text[i])):
            one = ONE_ROW.match(text[i])
            if one and not NUMBER.match(text[i]):
                number, name, values = int(one.group(1)), one.group(2), one.group(3).split()
                if not all(VALUE.match(v) for v in values) or not 1 <= number <= 16 or numbers:
                    raise ValueError(f"table {label!r} (page {page}): voivodeship row not read: {text[i]!r}")
                numbers.append(number)
                table["rows"].append(("voivodeship", edition, number, clean(name), values))
                i += 1
                continue
            block = []  # a voivodeship block (again after a page break)
            while i < len(text) and NUMBER.match(text[i]):
                block.append(int(text[i][:-1]))
                i += 1
            names = text[i : i + len(block)]
            values = text[i + len(block) : i + 9 * len(block)]
            i += 9 * len(block)
            if any(VALUE.match(n) for n in names) or len(values) != 8 * len(block):
                raise ValueError(f"table {label!r} (page {page}): voivodeship block not read: {names!r}")
            if not all(VALUE.match(v) for v in values) or (i < len(text) and VALUE.match(text[i])):
                raise ValueError(f"table {label!r} (page {page}): voivodeship values misaligned: {values[:16]!r}")
            for k, number in enumerate(block):
                if not 1 <= number <= 16 or (numbers and number <= numbers[-1]):
                    raise ValueError(f"table {label!r} (page {page}): voivodeship number {number} out of order")
                numbers.append(number)
                row = [values[c * len(block) + k] for c in range(8)]
                table["rows"].append(("voivodeship", edition, number, clean(names[k]), row))
        found.append(table)
    return found


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    from pypdf import PdfReader
    from pypdf.errors import PdfReadError

    if not body.startswith(b"%PDF-"):
        raise ValueError("not a PDF")
    match = re.fullmatch(r"ch_(\d{4})", receipt["entity"])
    if not match:
        raise ValueError(f"entity {receipt['entity']!r} is not an annual bulletin ch_<year>")
    edition = int(match.group(1))
    try:
        pages = [p.extract_text() or "" for p in PdfReader(io.BytesIO(body)).pages]
    except PdfReadError as exc:
        raise ValueError(f"PDF not readable: {exc}") from exc
    lines: list[tuple[int, str]] = []
    for number, text in enumerate(pages, 1):
        for line in text.splitlines():
            header = PAGE_HEADER.match(key(line))
            if header and int(header.group(2)) != edition:
                raise ValueError(f"page {number} header names {header.group(2)}, the file is {edition}")
            if not header:
                lines.append((number, line))
    preliminary = str(any("wstepne" in key(t) for t in pages)).lower()
    base = bulk_bronze.provenance(receipt)
    rows: list[dict[str, Any]] = []
    found = tables(lines, edition)
    if not found:
        raise ValueError("no voivodeship table found in the text layer (a scanned PDF or a new layout)")
    for number, table in enumerate(found, 1):
        for kind, year, area, label, values in table["rows"]:
            rows.append(
                {
                    **base,
                    "edition_year": edition,
                    "preliminary": preliminary,
                    "table_number": number,
                    "page_number": table["page"],
                    "disease_label": table["label"],
                    "disease_codes": table["codes"],
                    "incidence_header": table["incidence"],
                    "row_kind": kind,
                    "period_year": year,
                    "area_number": area,
                    "area_label": label,
                    **dict(zip([c for c, _ in COLUMNS[12:20]], values, strict=True)),
                    "source_row_number": len(rows) + 1,
                }
            )
    return pa.Table.from_pylist(rows, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: about 20 PDFs of 1-2.5 MB, decoded in memory one at a time."""
    from ingestion.to_landing import pzh_epimeld as landing

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
