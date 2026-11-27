#!/usr/bin/env python3
"""NIZP PZH-PIB EPIMELD bronze.load: newest copy of each annual bulletin → bronze.pzh_epimeld_rows.

Each annual bulletin "Choroby zakaźne i zatrucia w Polsce w <year> roku" (Ch_<year>.pdf, register PLS-019) prints,
per disease, the table "Liczba zachorowań (w kwartałach i ogółem), zapadalność oraz liczba i procent
hospitalizowanych wg województw" (variants: "rozpoznanych" or "zgłoszonych" cases, "skierowanych do szpitala",
incidence "na 100 tys. urodzeń żywych" for the congenital diseases): a POLSKA row for the previous year and for the
bulletin's year, then one row per voivodeship with a case in the bulletin's year (numbered 1-16 in alphabetical
order), each with cases in quarters I-IV, cases in the year, incidence per 100,000 and the number and per cent
hospitalised. The disease (ICD-10 codes in brackets) is the capital title above the table or, in tables of several
diseases, a line above each POLSKA block. The bulletin's other tables (age, sex, place, season) are not read.

The tables are read from the PDF text layer (pypdf, plain mode: the voivodeship rows come column by column: n
numbers, n names, then the n values of each of the eight columns). The text layer splits Polish diacritics into a
letter and a spacing mark ("zachorowan´"): titles and headers are matched on a key without spaces, marks and
diacritics (``key``), labels are stored with the marks recombined (``clean``). The page header must name the
bulletin's year. A table is located by its title and its column headers must match the expected list; a table whose
headers differ, a disease without its ICD codes, a table without a POLSKA row or voivodeship numbers out of order
reject the whole file (bronze.pzh_epimeld_rejected_files). A printed row whose values cannot be split into exactly
eight values (glyphs printed over each other, a spreadsheet's "1E+05") or whose year is neither the bulletin's nor
the year before is kept with its printed text in ``row_text`` and the reason in ``parse_note``, values null; Silver
loads no such row. Footnote signs after a year or a value ("*", "a") go to ``row_marks``. One Bronze row per printed
data row, values as printed (text).
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
VALUES = [
    "value_q1",
    "value_q2",
    "value_q3",
    "value_q4",
    "value_cases",
    "value_incidence",
    "value_hospitalised",
    "value_hospitalised_pct",
]
COLUMNS = [
    ("entity", "VARCHAR"),  # the bulletin, ch_<year>
    ("edition_year", "BIGINT"),  # the bulletin's year (file name and page header agree)
    ("preliminary", "VARCHAR"),  # "true" when the bulletin's text says "wstępne" (preliminary data) anywhere
    ("table_number", "BIGINT"),  # disease blocks in file order, from 1
    ("page_number", "BIGINT"),  # page of the table title
    ("table_title", "VARCHAR"),  # the table title as printed, diacritics recombined
    ("table_note", "VARCHAR"),  # footnotes ("* ...") under the block, except the omitted-voivodeships note
    ("disease_label", "VARCHAR"),  # disease line as printed, diacritics recombined
    ("disease_codes", "VARCHAR"),  # the ICD-10 codes in its last brackets, as printed
    ("incidence_header", "VARCHAR"),  # the incidence denominator: "na 100 tys." or "na 100 tys. urodzeń żywych"
    ("row_kind", "VARCHAR"),  # poland | voivodeship | voivodeship_block (a block not read, see parse_note)
    ("period_year", "BIGINT"),  # the row's year: printed on POLSKA rows, the bulletin's year on voivodeship rows
    ("area_number", "BIGINT"),  # voivodeship number 1-16 as printed (null on POLSKA rows)
    ("area_label", "VARCHAR"),  # the row label as printed, diacritics recombined where the text allows
    *[(c, "VARCHAR") for c in VALUES],  # as printed: "-" no case, "x" not applicable, "." no data
    ("row_marks", "VARCHAR"),  # footnote signs printed on the row, in order
    ("row_text", "VARCHAR"),  # the printed row when it was not read
    ("parse_note", "VARCHAR"),  # why the row was not read (null = read)
    ("raw_sha256", "VARCHAR"),
    ("source_version", "VARCHAR"),
    ("ingested_at", "TIMESTAMPTZ"),
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)

MARKS = "´˛˙¸˝`¨˘ˇ"  # spacing accent marks the text layer prints after (or before) the letter
COMBINE = {"´": "́", "˛": "̨", "˙": "̇"}
TABLE = re.compile(
    r"^liczba(zachorowan|rozpoznanychzachorowan|zgłoszonychzachorowan)\(wkwartałachiogołem\)"
    r"(,zapadalnosc(?P<births>na100tys\.urodzenzywych)?orazliczbaiprocent(hospitalizowanych|chorychskierowanychdoszpitala)"
    r"|(i|oraz)zapadalnosc)wgwojewodztw\*?$"
)
# the column headers as one key, "*" removed and a leading "(Dane Zakładu ...)" line dropped: (pattern, values per row)
HEADERS = [
    (
        re.compile(
            r"^wojewodztwoliczbazachorowanwkwartałachliczbazachorowanwrokuzapadalnoscna100tys\.?"
            r"(hospitalizacja|skierowanidoszpitala)iiiiiiivliczba%$"
        ),
        8,
    ),
    (re.compile(r"^wojewodztwoliczbawkwartałachliczbawrokuczestoscna100tys\.?hospitalizacjaiiiiiiivliczba%$"), 8),
    (re.compile(r"^wojewodztwoliczbazachorowanwkwartałachliczbazachorowanwrokuzapadalnoscna100tys\.?iiiiiiiv$"), 6),
]
SOURCE_LINE = re.compile(r"^\(dane[^)]*\)")
PAGE_HEADER = re.compile(r"^chorobyzakaznei(zatrucia)?wpolscew(\d{4})roku")
VALUE = re.compile(r"^(-|x|\.|\d+|\d+,\d+)$")  # a count, a decimal comma, or a sign for no value
MARK = re.compile(r"^(\*+|[a-d])$")  # a footnote sign printed as its own token
YEAR_ROW = re.compile(r"^((?:19|20)\d\d) ?r\.(\**)(?: (.*))?$")
NUMBER = re.compile(r"^(\d{1,2})\.$")
ONE_ROW = re.compile(r"^(\d{1,2})\. (\D.*?) ((?:\S+ ){5,}\S+)$")  # a block of one voivodeship is printed on one line
CODES = re.compile(r"\(([^()]*\d[^()]*)\)$")
OMITTED = "*pominietowojewodztwa"


def key(text: str) -> str:
    """Matching key: no spaces, no spacing marks, no diacritics (ł kept), lower case."""
    text = "".join(c for c in unicodedata.normalize("NFKD", text) if not unicodedata.combining(c))
    return re.sub(r"\s+", "", "".join(c for c in text if c not in MARKS)).lower()


def clean(text: str) -> str:
    """A label with the spacing marks recombined with the letter before them (NFC), spaces collapsed."""
    text = re.sub(rf"\s+([{MARKS}])", r"\1", " ".join(text.split()))
    text = re.sub(rf"(\w)([{''.join(COMBINE)}])", lambda m: m.group(1) + COMBINE[m.group(2)], text)
    return unicodedata.normalize("NFC", text)


def disease(line: str, capitals: bool = False) -> tuple[str, str | None] | None:
    """(label, codes) of a disease line: ends with ICD codes in brackets (a footnote "*" and "- cd" removed); with
    ``capitals``, also a title in capitals without codes (food poisonings), codes None."""
    text = re.sub(r"\s*(-\s*c\s*d)?\s*\**\s*$", "", clean(line))
    text = re.sub(r"^\d+,\d+\s+", "", text)  # a value of the previous table glued to the title (2013)
    if re.match(r"^(\d|POLSKA|\*)", text) or not text:
        return None
    match = CODES.search(text)
    if match:
        return text, match.group(1).strip()
    if capitals and text.upper() == text and re.search(r"[A-ZĄĆĘŁŃÓŚŹŻ]{4}", text) and not re.search(r"\d", text):
        return text, None
    return None


def values(tokens: list[str], width: int) -> tuple[list[str] | None, str, str | None]:
    """(the row's values, padded to eight, or None; footnote signs; parse note) from the tokens of a printed row."""
    marks = [t for t in tokens if MARK.match(t)]
    kept = [t for t in tokens if not MARK.match(t)]
    kept = [t[:-1] if t.endswith("*") and VALUE.match(t[:-1]) else t for t in kept]
    if len(kept) != width or not all(VALUE.match(t) for t in kept):
        return None, " ".join(marks), f"{len(kept)} tokens, not {width} values"
    return kept + [None] * (8 - width), " ".join(marks), None


def tables(lines: list[tuple[int, str]], edition: int) -> list[dict[str, Any]]:
    """The disease blocks of one bulletin from its plain text: [(page, line)], page headers removed."""
    text = [t.strip() for _, t in lines]
    found: list[dict[str, Any]] = []
    i = 0
    while i < len(text):
        title_match = TABLE.match(key(text[i]))
        if not title_match:
            i += 1
            continue
        page, title = lines[i][0], clean(text[i])
        above = [t for t in text[max(0, i - 3) : i] if t and not SOURCE_LINE.match(key(t))][-2:]
        current = disease(above[-1], capitals=True) if above else None
        if current is not None and current[1] is None and len(above) > 1 and disease(above[-2] + " " + above[-1]):
            current = disease(above[-2] + " " + above[-1])  # a disease title printed on two lines
        if (
            current is None
            and found
            and found[-1]["basis"] != title_match.group(1)
            and text[i - 1].strip() == (found[-1]["last_line"])
        ):
            # a second table of the same disease right under the first (AIDS: reported, then diagnosed cases)
            current = (found[-1]["label"], found[-1]["codes"])
        start = i = i + 1
        while i < len(text) and key(text[i]) != "polska" and not (disease(text[i]) and key(text[i + 1]) == "polska"):
            i += 1
            if i - start > 14:
                raise ValueError(f"table {title[:60]!r} (page {page}): no POLSKA row after the headers")
        head = SOURCE_LINE.sub("", key(" ".join(text[start:i])).replace("*", ""))
        width = next((w for h, w in HEADERS if h.match(head)), None)
        if width is None:
            raise ValueError(f"table on page {page}: column headers differ: {head!r}")
        incidence = "na 100 tys. urodzeń żywych" if title_match.group("births") else "na 100 tys."
        while i < len(text):
            if disease(text[i]) and i + 1 < len(text) and key(text[i + 1]) == "polska":
                current = disease(text[i])
                i += 1
            if key(text[i]) != "polska":
                break
            if current is None:
                raise ValueError(f"table {title[:60]!r} (page {page}): POLSKA block without a disease and ICD codes")
            block = {
                "page": page,
                "title": title,
                "label": current[0],
                "codes": current[1],
                "incidence": incidence,
                "rows": [],
                "note": [],
                "width": width,
                "basis": title_match.group(1),
            }
            i = read_block(text, i + 1, edition, block)
            block["last_line"] = text[i - 1]
            while i < len(text) and text[i].startswith("*"):
                if not key(text[i]).startswith(OMITTED):
                    block["note"].append(clean(text[i]))
                i += 1
            found.append(block)
    return found


def read_block(text: list[str], i: int, edition: int, block: dict[str, Any]) -> int:
    """Read the year rows and the voivodeship rows of one POLSKA block; returns the next line."""
    label = f"{block['label'][:60]!r} (page {block['page']})"
    while i < len(text) and YEAR_ROW.match(text[i]):
        year, star, rest = YEAR_ROW.match(text[i]).groups()
        row, marks, note = values((rest or "").split(), block["width"])
        if note is None and int(year) not in (edition - 1, edition):
            note = f"year {year} is neither {edition - 1} nor {edition}"
        block["rows"].append(
            (
                "poland",
                int(year),
                None,
                "POLSKA",
                row,
                " ".join(filter(None, [star, marks])),
                text[i] if note else None,
                note,
            )
        )
        i += 1
    if not block["rows"]:
        raise ValueError(f"block {label}: no year row under POLSKA")
    numbers: list[int] = []
    while i < len(text) and (NUMBER.match(text[i]) or ONE_ROW.match(text[i])):
        one = ONE_ROW.match(text[i])
        if one and not NUMBER.match(text[i]):
            rows = [(int(one.group(1)), one.group(2), one.group(3).split(), text[i])]
            i += 1
        else:
            block_numbers = []
            while i < len(text) and NUMBER.match(text[i]):
                block_numbers.append(int(text[i][:-1]))
                i += 1
            names = text[i : i + len(block_numbers)]
            i += len(block_numbers)
            cells = []
            while i < len(text) and (VALUE.match(text[i]) or MARK.match(text[i])):
                cells.append(text[i])
                i += 1
            n, width = len(block_numbers), block["width"]
            if any(VALUE.match(x) for x in names) or len(cells) != width * n:
                note = f"voivodeship block of {n} rows with {len(cells)} values"
                block["rows"].append(
                    ("voivodeship_block", edition, None, " | ".join(names), None, "", " ".join(cells)[:500], note)
                )
                numbers += block_numbers
                continue
            rows = [
                (nr, names[k], [cells[c * n + k] for c in range(width)], None) for k, nr in enumerate(block_numbers)
            ]
        for number, name, tokens, printed in rows:
            if not 1 <= number <= 16 or (numbers and number <= numbers[-1]):
                raise ValueError(f"block {label}: voivodeship number {number} out of order")
            numbers.append(number)
            row, marks, note = values(tokens, block["width"])
            printed = printed or " ".join([name, *tokens])
            block["rows"].append(
                ("voivodeship", edition, number, clean(name), row, marks, printed if note else None, note)
            )
    return i


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
    found = tables(lines, edition)
    if not found:
        raise ValueError("no voivodeship table found in the text layer (a scanned PDF or a new layout)")
    base = bulk_bronze.provenance(receipt)
    rows: list[dict[str, Any]] = []
    for number, block in enumerate(found, 1):
        for kind, year, area, label, row, marks, printed, note in block["rows"]:
            rows.append(
                {
                    **base,
                    "edition_year": edition,
                    "preliminary": preliminary,
                    "table_number": number,
                    "page_number": block["page"],
                    "table_title": block["title"],
                    "table_note": " | ".join(block["note"]) or None,
                    "disease_label": block["label"],
                    "disease_codes": block["codes"],
                    "incidence_header": block["incidence"],
                    "row_kind": kind,
                    "period_year": year,
                    "area_number": area,
                    "area_label": label,
                    **dict(zip(VALUES, row or [None] * 8, strict=True)),
                    "row_marks": marks or None,
                    "row_text": printed,
                    "parse_note": note,
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
