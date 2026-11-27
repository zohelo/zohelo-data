"""UPRP industrial-property statistics: Landing → Bronze activities (register EX-001).

bronze.load       newest copy of every landed annual report → bronze.uprp_statistics_cells (one row per cell of the
                  nine statistics tables read, with its guard); a file that is no readable PDF or holds none of the
                  tables is rejected, with the reason, into bronze.uprp_statistics_rejected_files
bronze.verify     ingestion/common/lineage.py --source uprp_statistics
landing.archive   ingestion/common/archive.py --source uprp_statistics (move to Archive)
archive.prune     ingestion/common/retention.py --source uprp_statistics (archive_policy.retention)

The Polish annual reports since 2019 end with a statistics section of numbered tables, each over the five years up
to the report's year ("Procedura Krajowe/zagraniczne 2020 2021 2022 2023 2024"). Nine are read, found by their
title (spaces removed, so "zg łoszenia" in the 2023 text layer still matches; a table whose title is printed twice
or not at all is recorded, never guessed), see TABLES: applications and rights granted or registered for
inventions, utility models, trademarks and industrial designs (procedure and origin rows, "Suma końcowa") and
European patents validated in Poland (only the total over the countries of origin). The text layer is read with
pypdf, which applies the fonts' character maps. No value is taken by its position alone:
- the header must name consecutive years (the 2019 header prints "201 5"), and a row is read only when its
  numbers split into exactly one value per year in one way (digits grouped by single spaces, "12 230", and
  columns also separated by single spaces, so "13 284 12 841" is split by the grouping rule; a split that is
  not unique, glued digits such as "1 04274" or "00000", or a row with fewer values, e.g. empty cells, is no value);
- every cell is checked against the table's own sums, year by year: "Suma końcowa" = the procedure rows (or
  domestic + foreign), each procedure = its domestic + foreign rows; the validations total = the sum of the
  country rows. A cell is `ok` only when a sum it takes part in holds; otherwise its guard names why.
Every expected row of a table found yields a Bronze row, with the value as printed or none, and the guard (`ok` or
the reason); a table not found yields one row with its reason. Silver keeps the `ok` cells only. Natural persons
appear in the reports (ranking tables, authors); nothing outside the nine tables reaches Bronze.
"""

from __future__ import annotations

import io
import re
from typing import Any

SOURCE = "uprp_statistics"
COLUMNS = [
    ("entity", "VARCHAR"),
    ("table_code", "VARCHAR"),
    ("table_title", "VARCHAR"),
    ("page_number", "BIGINT"),
    ("row_code", "VARCHAR"),
    ("row_label", "VARCHAR"),
    ("year_text", "VARCHAR"),
    ("value_text", "VARCHAR"),
    ("guard", "VARCHAR"),
    ("raw_sha256", "VARCHAR"),
    ("source_version", "VARCHAR"),
    ("ingested_at", "TIMESTAMPTZ"),
    ("source_row_number", "BIGINT"),
]
# table code: (title without spaces, lower case; kind). "procedure": TRYB KRAJOWY, krajowe, zagraniczne, then PCT or
# TRYB MIĘDZYNARODOWY, krajowe, zagraniczne, Suma końcowa; "origin": Krajowe, Zagraniczne, Suma końcowa; "countries":
# one row per country of origin, then Suma końcowa (only the total is kept).
TABLES = {
    "inventions_applications": ("krajoweizagranicznezgłoszeniawynalazków", "procedure"),
    "inventions_grants": ("patentyudzielonepodmiotomkrajowymizagranicznym", "origin"),
    "utility_models_applications": ("krajoweizagranicznezgłoszeniawzorówużytkowych", "procedure"),
    "utility_models_grants": ("prawaochronnenawzoryużytkoweudzielonepodmiotomkrajowymizagranicznym", "origin"),
    "european_patents_validations": (
        "patentyeuropejskiewalidowanewpolscewedługkrajupochodzeniauprawnionego",
        "countries",
    ),
    "trademarks_applications": ("krajoweizagranicznezgłoszeniaznakówtowarowych", "procedure"),
    "trademarks_grants": (
        "prawaochronnenaznakitowaroweudzielonepodmiotomkrajowymizagranicznymwedługprocedury",
        "procedure",
    ),
    "industrial_designs_applications": ("krajoweizagranicznezgłoszeniawzorówprzemysłowych", "procedure"),
    "industrial_designs_grants": (
        "prawazrejestracjiwzorówprzemysłowychudzielonepodmiotomkrajowymizagranicznym",
        "procedure",
    ),
}
PROCEDURE_ROWS = [
    ("national", "trybkrajowy"),
    ("national_domestic", "krajowe"),
    ("national_foreign", "zagraniczne"),
    ("international", ("pct", "trybmiędzynarodowy", "trybmiędzynarodowy(pct)")),
    ("international_domestic", "krajowe"),
    ("international_foreign", "zagraniczne"),
    ("total", "sumakońcowa"),
]
ORIGIN_ROWS = [("domestic", "krajowe"), ("foreign", "zagraniczne"), ("total", "sumakońcowa")]
# sums checked per year: total = parts
PROCEDURE_SUMS = [
    ("total", ["national", "international"]),
    ("national", ["national_domestic", "national_foreign"]),
    ("international", ["international_domestic", "international_foreign"]),
]
ORIGIN_SUMS = [("total", ["domestic", "foreign"])]
NUMBERED = re.compile(r"^\s*\d\.\s?\d{1,2}\.?\s")  # "1.1. ", "3.1 ", "1. 7." never seen
LABEL = re.compile(r"^(\D*?)\s*(\d[\d\s]*)?$")  # label text, then digits and spaces to the line's end (linear)
# the summary page's labelled figure of the year, "walidacje patentów" / "europejskich 10 580" (lower case at a line's
# start: not the contents line "Walidacje patentów europejskich 17", not "wnioski o walidacje patentów europejskich")
VALIDATIONS_FIGURE = re.compile(r"^\s*walidacje\s+patentów\s*\n?\s*europejskich\s+([\d ]+?)\s*$", re.MULTILINE)
RUNNING_HEADS = ("raportroczny", "statysty", "urządpatentowy")  # page heads and feet, spaces removed ("STAT YST YKI")


class Layout(ValueError):
    """The file cannot be read as an annual report: rejected, never guessed."""


def key(text: str) -> str:
    return re.sub(r"\s+", "", text).lower()


def header_years(line: str) -> list[int] | None:
    """The consecutive years a header line ends with ("2015 2016 …", also "201 5 2016"); None when it has none."""
    digits = re.search(r"(\d[\d ]*)$", line.strip())
    run = re.sub(r"\s", "", digits.group(1)) if digits else ""
    if len(run) < 8 or len(run) % 4:
        return None
    years = [int(run[i : i + 4]) for i in range(0, len(run), 4)]
    if years[0] < 2000 or years != list(range(years[0], years[0] + len(years))):
        return None
    return years


def splits(tokens: list[str], count: int) -> list[list[str]]:
    """Every way to read the digit tokens as `count` numbers: a number is "0", a token of 4+ digits without a
    leading zero, or a token of 1-3 digits without a leading zero followed by groups of exactly 3 digits."""
    found: list[list[str]] = []

    def walk(i: int, done: list[str]) -> None:
        if len(found) > 1 or len(done) > count:
            return
        if i == len(tokens):
            if len(done) == count:
                found.append(done)
            return
        head = tokens[i]
        if head == "0" or (len(head) >= 4 and head[0] != "0"):
            walk(i + 1, [*done, head])
            return
        if not (1 <= len(head) <= 3) or head[0] == "0":
            return
        j = i + 1
        walk(j, [*done, head])
        while j < len(tokens) and len(tokens[j]) == 3:
            j += 1
            walk(j, [*done, " ".join(tokens[i:j])])

    walk(0, [])
    return found


def numbers(digits: str, count: int) -> tuple[list[str] | None, str]:
    """(values as printed, '') when the digits split into `count` numbers in exactly one way, else (None, reason)."""
    tokens = digits.split()
    if not tokens:
        return None, "no values"
    found = splits(tokens, count)
    if len(found) == 1:
        return found[0], ""
    reason = "values do not split into one per year" if not found else "values split into years in more than one way"
    return None, f"{reason}: {digits.strip()[:40]!r}"


def value(text: str) -> int:
    return int(text.replace(" ", ""))


def title_of(lines: list[tuple[int, str]]) -> str:
    """A numbered title with the lines it continues on before the year header (at most two: "5.6. Prawa z rejestracji
    … i zagranicznym" / "według klasyfikacji lokarneńskiej" in 2025 is not table 5.4)."""
    parts = [NUMBERED.sub("", lines[0][1], count=1)]
    for _, line in lines[1:3]:
        if re.search(r"\d\s*$", line):  # the year header or a row: the title has ended
            break
        parts.append(line)
    return " ".join(" ".join(parts).split())


def find_tables(pages: list[str]) -> dict[str, list[tuple[int, str]] | None]:
    """{table code: [(page, line), …] from its numbered title to the next one}; None for a title printed more than
    once (the caller records it, no copy is picked)."""
    sections: list[list[tuple[int, str]]] = []
    for page_number, text in enumerate(pages, start=1):
        for line in text.replace("\xa0", " ").splitlines():
            if NUMBERED.match(line):
                sections.append([(page_number, line)])
            elif sections:
                sections[-1].append((page_number, line))
    out: dict[str, list[list[tuple[int, str]]]] = {}
    for section in sections:
        title = key(title_of(section))
        for code, (wanted, kind) in TABLES.items():
            if title == wanted or (kind == "procedure" and title == wanted + "wedługprocedury"):
                out.setdefault(code, []).append(section)
    return {code: blocks[0] if len(blocks) == 1 else None for code, blocks in out.items()}


def validations_figure(pages: list[str]) -> tuple[int, int] | None:
    """(value, page) of the labelled figure "walidacje patentów europejskich <n>"; None when it is not printed, not
    read as one number, or printed with two different values."""
    found = []
    for page_number, text in enumerate(pages, start=1):
        for match in VALIDATIONS_FIGURE.finditer(text.replace("\xa0", " ")):
            one, _ = numbers(match.group(1), 1)
            found.append((value(one[0]) if one else None, page_number))
    values = {v for v, _ in found}
    return found[0] if len(values) == 1 and None not in values else None


def read_table(code: str, lines: list[tuple[int, str]], figure: tuple[int, int] | None = None) -> list[dict[str, Any]]:
    """The cells of one table with their guards (module docstring)."""
    kind = TABLES[code][1]
    title_page = lines[0][0]
    base = {
        "table_code": code,
        "table_title": title_of(lines),
        "page_number": title_page,
    }
    years: list[int] | None = None
    rows: list[tuple[str, str, int, str]] = []  # (label key, label as printed, page, digits)
    pending = ""
    for page_number, line in lines[1:]:
        text = line.strip()
        if not text or "|" in text or any(head in key(text) for head in RUNNING_HEADS):
            continue  # page heads and feet
        if years is None:
            years = header_years(text)
            continue
        if re.fullmatch(r"1\s*2\s*3\s*4\s*5\s*6(\s*7)?|\d+\s*I|I\s*\d+", text):
            continue  # column numbers; page numbers "I 77" (2025)
        match = LABEL.match(text)
        if not match:
            pending = ""  # a line with digits inside (a running head "84 3. Patenty …") names no country
            continue
        label, digits = match.group(1).strip(), match.group(2) or ""
        if label and not digits:
            pending = label  # a label whose values are printed on the next line ("Tryb krajowy" in 2025, countries)
            continue
        if not label and pending:
            label, pending = pending, ""
        if not label:
            continue  # a lone page number
        rows.append((key(label), label, page_number, digits))
        pending = ""
        if key(label) == "sumakońcowa":
            break
    if years is None:
        return [{**base, "row_code": None, "row_label": None, "year_text": None, "value_text": None,
                 "guard": "no header with consecutive years"}]  # fmt: skip
    if not rows or rows[-1][0] != "sumakońcowa":
        return [{**base, "row_code": None, "row_label": None, "year_text": None, "value_text": None,
                 "guard": "no 'Suma końcowa' row"}]  # fmt: skip
    if kind == "countries":
        return countries(base, years, rows, figure)
    expected = PROCEDURE_ROWS if kind == "procedure" else ORIGIN_ROWS
    sums = PROCEDURE_SUMS if kind == "procedure" else ORIGIN_SUMS
    shape_ok = len(rows) == len(expected) and all(
        row[0] == w or (isinstance(w, tuple) and row[0] in w) for row, (_, w) in zip(rows, expected, strict=False)
    )
    if not shape_ok:
        return [{**base, "row_code": None, "row_label": " / ".join(r[1] for r in rows)[:200], "year_text": None,
                 "value_text": None, "guard": "rows not as expected"}]  # fmt: skip
    parsed: dict[str, list[str] | None] = {}
    reasons: dict[str, str] = {}
    for (row_code, _), row in zip(expected, rows, strict=True):
        parsed[row_code], reasons[row_code] = numbers(row[3], len(years))
    checked: dict[tuple[str, int], str] = {}  # (row, year index) → 'ok' once a sum it takes part in holds
    for i in range(len(years)):
        for total, parts in sums:
            cells = [total, *parts]
            if any(parsed[c] is None for c in cells):
                continue
            if value(parsed[total][i]) == sum(value(parsed[p][i]) for p in parts):
                for c in cells:
                    checked[(c, i)] = "ok"
    out = []
    for (row_code, _), (_, label, page_number, _) in zip(expected, rows, strict=True):
        for i, year in enumerate(years):
            values = parsed[row_code]
            if values is None:
                guard = f"row not read: {reasons[row_code]}"
            else:
                guard = checked.get((row_code, i), "no sum of the table holds for this cell")
            out.append(
                {
                    **base,
                    "page_number": page_number,
                    "row_code": row_code,
                    "row_label": label,
                    "year_text": str(year),
                    "value_text": values[i] if values else None,
                    "guard": guard,
                }
            )
    return out


def countries(
    base: dict[str, Any], years: list[int], rows: list[tuple[str, str, int, str]], figure: tuple[int, int] | None
) -> list[dict[str, Any]]:
    """The validations total per year, `ok` when it equals the sum of the country rows of that year; when a country
    row cannot be read (an empty cell, glued digits), the total of the report's own year is `ok` if it equals the
    labelled figure of the summary page (validations_figure), the other years are not."""
    *parts, (_, label, page_number, digits) = rows
    total, reason = numbers(digits, len(years))
    read = [numbers(d, len(years)) for _, _, _, d in parts]
    unread = [(lab, why) for (_, lab, _, _), (v, why) in zip(parts, read, strict=True) if v is None]
    out = []
    for i, year in enumerate(years):
        if total is None:
            guard = f"row not read: {reason}"
        elif unread and figure and year == years[-1] and figure[0] == value(total[i]):
            guard = f"ok: country rows not read, equals the labelled figure on page {figure[1]}"
        elif unread:
            guard = f"country rows not read ({len(unread)} of {len(parts)}, first {unread[0][0][:30]!r})"
        elif sum(value(v[i]) for v, _ in read) != value(total[i]):
            guard = f"countries sum to {sum(value(v[i]) for v, _ in read)}, not the total"
        else:
            guard = "ok"
        out.append({**base, "page_number": page_number, "row_code": "total", "row_label": label,
                    "year_text": str(year), "value_text": total[i] if total else None, "guard": guard})  # fmt: skip
    return out


def parse(pages: list[str]) -> list[dict[str, Any]]:
    """The cells of the nine tables, one row per expected cell (or one per table not read). Raises Layout when
    none of the tables is found (not an annual report with a statistics section)."""
    found = find_tables(pages)
    if not found:
        raise Layout("none of the statistics tables found (no statistics section of the 2019+ layout)")
    cells, figure = [], validations_figure(pages)
    for code in TABLES:
        if code not in found:
            cells.append({"table_code": code, "table_title": None, "page_number": None, "row_code": None,
                          "row_label": None, "year_text": None, "value_text": None,
                          "guard": "table title not found"})  # fmt: skip
        elif found[code] is None:
            cells.append({"table_code": code, "table_title": None, "page_number": None, "row_code": None,
                          "row_label": None, "year_text": None, "value_text": None,
                          "guard": "table title printed twice"})  # fmt: skip
        else:
            cells += read_table(code, found[code], figure)
    return cells


def text_pages(body: bytes) -> list[str]:
    """The text layer of every page (pypdf plain extraction, with the fonts' character maps). A page whose content
    stream exceeds pypdf's safety limits (the picture covers of the 2019 report, pages 1 and 89: "Reached maximum
    number of tokens per stream") is read as empty: a table printed on it is then "table title not found", never
    guessed; the limits are not raised."""
    from pypdf import PdfReader
    from pypdf.errors import LimitReachedError, PyPdfError

    if not body.startswith(b"%PDF"):
        raise Layout("not a PDF file")
    try:
        reader = PdfReader(io.BytesIO(body))
        pages = []
        for page in reader.pages:
            try:
                pages.append(page.extract_text() or "")
            except LimitReachedError:
                pages.append("")
        return pages
    except PyPdfError as exc:
        raise Layout(f"unreadable PDF: {exc}") from exc


def decode(body: bytes, receipt: dict[str, Any]):
    """bulk_bronze decoder: the guarded table cells of one annual report."""
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
    from ingestion.to_landing import uprp_statistics as landing

    return sorted(landing.held())


def _bronze_activities() -> dict[str, Any]:
    from ingestion.common import gus_publications

    return gus_publications.bronze_activities(SOURCE, _entities)


ACTIVITIES = {
    **_bronze_activities(),
    "bronze.load": lambda p: bronze_load(_entities()),
}
