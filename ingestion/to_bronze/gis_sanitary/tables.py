"""Read the food-samples table of the GIS annual report from the PDF text layer (register PLS-018).

Every edition since 2006 ("Stan sanitarny kraju", from 2024 "Raport z działań Państwowej Inspekcji Sanitarnej")
prints the table "Jakość zdrowotna środków spożywczych": one row per year (the report year and several years
before it) with eight number cells, samples examined and the per cent disqualified for all foodstuffs ("razem")
and for domestic, imported and EU foodstuffs. It is found and checked before a single cell is kept:

- the page: its text holds the title, the header words (``HEADER``, "UE", "Rok" or "Lata") and at least two year
  rows (the table of contents and the table by food group have none); when several pages qualify their year rows
  must be identical (2024: pages 24 and 25, Dev trials of #442) and the first is kept;
- a year row: a cell ``20yy`` followed by eight cells alternating count / share (layout text, cells split at two or
  more spaces; a count is digits with single-space thousands groups, a share at most two decimals with a comma);
- the years: unique, without gaps, the last one the edition's report year (from the entity name);
- the header order of the two last groups (``importowane`` and ``UE`` were printed in either order) from the line
  that names ``krajowe`` and both of them; each cell keeps its printed group;
- sums: domestic + imported + EU samples equal the total, to 1 / 1,000 of it (the 2020 row is 50 short of its
  58,883 in every edition that prints it; a misread cell is off by far more); for the report year also the total
  share against the components' share weighted by their samples, to 0.06 points (printed to one or two decimals).
  Later reprints are not held to the share check: from 2021 every edition reprints 2020 as 3,2 % where the 2020
  edition and its components give 3,09 %, so the reprints stay as printed and Gold takes each year from its own
  report.

Any failed check rejects the file (ValueError) with the reason. Nothing else of the report is read: no text, no
photographs, no names. No-break spaces count as spaces (some editions use them as thousands separators).
"""

from __future__ import annotations

import re

TITLE = "Jakość zdrowotna środków spożywczych"
TITLE_TEXT = re.compile(r"Jakość zdrowotna środków (?:\S+ ){0,3}?spożywczych")  # 2012: title broken by a cell
HEADER = ("próbki", "krajowe", "importowane")  # any case; "UE" and "Rok" or "Lata" as printed
YEAR = re.compile(r"^20\d\d$")
COUNT = re.compile(r"^\d{1,6}(?: \d{1,3})*$")  # "7692", "6 6 913" (66 913 in 2012-2014) pass; the sums guard
SHARE = re.compile(r"^\d{1,2}(?:,\d{1,2})?$|^100$")
SUM_TOLERANCE = 1_000
SHARE_TOLERANCE = 0.06
MEASURES = {"samples": "Próbki zbadane", "disqualified_pct": "Próbki zakwestionowane %"}
GROUPS = {"razem": "total", "krajowe": "domestic", "importowane": "imported", "UE": "eu"}


def cells(line: str) -> list[str]:
    return [c for c in re.split(r" {2,}", line.strip()) if c]


def count(text: str) -> int:
    return int(text.replace(" ", ""))


def share(text: str) -> float:
    return float(text.replace(",", "."))


def year_row(parts: list[str]) -> list[str] | None:
    """The year and its eight number cells, when the line holds a year row (other cells may follow)."""
    for i, part in enumerate(parts):
        rest = parts[i + 1 : i + 9]
        if (
            YEAR.match(part)
            and len(rest) == 8
            and all((SHARE if k % 2 else COUNT).match(c) for k, c in enumerate(rest))
        ):
            return [part, *rest]
    return None


def group_order(lines: list[list[str]]) -> tuple[str, str]:
    for parts in lines:
        if "krajowe" in parts and "importowane" in parts and "UE" in parts:
            first, second = sorted(("importowane", "UE"), key=parts.index)
            return GROUPS[first], GROUPS[second]
    raise ValueError("header: no line names krajowe, importowane and UE as columns")


def read_page(lines: list[list[str]], report_year: int) -> list[list[str]]:
    rows = [row for parts in lines if (row := year_row(parts))]
    years = [int(r[0]) for r in rows]
    if len(set(years)) != len(years):
        raise ValueError(f"year printed twice: {years}")
    if years != list(range(years[0], years[0] + len(years))) or years[-1] != report_year:
        raise ValueError(f"years {years} are not consecutive up to the report year {report_year}")
    for row in rows:
        total, parts = count(row[1]), [count(row[k]) for k in (3, 5, 7)]
        if abs(sum(parts) - total) > total // SUM_TOLERANCE:
            raise ValueError(f"{row[0]}: groups add up to {sum(parts)}, total {total}")
        if any(share(row[k]) > 100 for k in (2, 4, 6, 8)):
            raise ValueError(f"{row[0]}: a share above 100")
    last = rows[-1]
    weighted = sum(count(last[k]) * share(last[k + 1]) for k in (3, 5, 7)) / count(last[1])
    if abs(weighted - share(last[2])) > SHARE_TOLERANCE:
        raise ValueError(f"{last[0]}: total share {last[2]} is not the groups' weighted share {weighted:.3f}")
    return rows


def read_report(pages: list[str], report_year: int) -> list[dict]:
    """The cells of the table: one dict per value cell, with page, row and column position and printed labels."""
    found = []
    for number, page in enumerate(pages, start=1):
        text = page.replace(" ", " ").replace(" ", " ")
        flat = " ".join(text.split())
        words = all(w in flat.lower() for w in HEADER) and re.search(r"\bUE\b", flat)
        if not TITLE_TEXT.search(flat) or not words or not re.search(r"\b(Rok|Lata)\b", flat):
            continue
        lines = [cells(line) for line in text.split("\n")]
        if sum(1 for parts in lines if year_row(parts)) >= 2:
            found.append((number, lines))
    if not found:
        raise ValueError(f"table {TITLE!r}: no page with its title, header and year rows")
    rows_of = [[row for parts in lines if (row := year_row(parts))] for _, lines in found]
    if any(rows != rows_of[0] for rows in rows_of[1:]):
        raise ValueError(f"table {TITLE!r}: different year rows on pages {[n for n, _ in found]}")
    number, lines = found[0]
    third, fourth = group_order(lines)
    columns = [(g, m) for g in ("total", "domestic", third, fourth) for m in ("samples", "disqualified_pct")]
    labels = {code: printed for printed, code in GROUPS.items()}
    out = []
    for row_number, row in enumerate(read_page(lines, report_year), start=1):
        for k, ((group, measure), text) in enumerate(zip(columns, row[1:], strict=True)):
            out.append(
                {
                    "table_title": TITLE,
                    "page_number": number,
                    "row_number": row_number,
                    "column_number": k + 2,
                    "row_label": row[0],
                    "column_header": f"{labels[group]} · {MEASURES[measure]}",
                    "column_code": f"{group}_{measure}",
                    "value_text": text,
                }
            )
    return out
