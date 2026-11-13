"""Read the coverage tables of the internet.gov.pl national report from the PDF text layer (register PL-ENV-034).

The report ("Polska w zasięgu stacjonarnego dostępu do internetu", one PDF per quarter) prints three tables this
source loads, each found by its title and checked before a single cell is kept:

- ``territory`` "Liczba punktów adresowych w zasięgu na terenie całego kraju": the 16 voivodeships by printed name
  and SUMA; columns coverages, address points in coverage, all address points;
- ``service_type`` "Liczba punktów adresowych w podziale na rodzaj usług": Rzeczywisty, Hurtowy (wyłącznie),
  Teoretyczny (wyłącznie), Brak zasięgu and SUMA; columns address points and their share;
- ``bandwidth`` "Liczba zasięgów i punktów adresowych w podziale na przepustowość": bandwidth classes in Mb/s and
  SUMA, possibly continued on the next page under a repeated header; columns coverages, share, address points,
  share.

Text comes from pypdf's layout mode, which keeps each printed line and puts two or more spaces between table
columns, while a number's thousands separator is at most one space (the 2024 editions, made with cairo, print the
groups without any separator). A row is its label plus number cells; every cell is checked against its column's
format, and the table is rejected (ValueError) when its header, its row labels (a fixed list per table), its
SUMA row (counts must add up exactly, every share must be its count's share of SUMA to the printed precision) or
the number of cells per row does not match. Nothing is taken by position without its label: a value is kept with
its row label and column header as printed. A label printed on two lines ("warmińsko-" / "mazurskie") is joined
when the first line ends with a hyphen and the second holds the rest of a known name.

The reference date ("według stanu na dzień 30 września 2026") is read from the text and must be the last day of
the edition's quarter (the entity name), else the file is rejected.
"""

from __future__ import annotations

from dataclasses import dataclass
import re

VOIVODESHIPS = (
    "dolnośląskie",
    "kujawsko-pomorskie",
    "lubelskie",
    "lubuskie",
    "łódzkie",
    "małopolskie",
    "mazowieckie",
    "opolskie",
    "podkarpackie",
    "podlaskie",
    "pomorskie",
    "śląskie",
    "świętokrzyskie",
    "warmińsko-mazurskie",
    "wielkopolskie",
    "zachodniopomorskie",
)
SERVICE_TYPES = ("Rzeczywisty", "Hurtowy (wyłącznie)", "Teoretyczny (wyłącznie)", "Brak zasięgu")
BANDWIDTHS = tuple(str(n) for n in (2, 10, 20, 30, 40, 50, 60, 70, 80, 90, *range(100, 1000, 100), *range(1000, 10001, 1000)))
MONTHS = {"marca": 3, "czerwca": 6, "września": 9, "grudnia": 12}
COUNT = re.compile(r"^(?:\d{1,3}(?:[   ]\d{3})+|\d+)$")
# The report's own rows do not always add up exactly: the bandwidth classes' address points differ from SUMA by
# -124 (2024-Q4), -180 (2025-Q1), -1 (2025-Q3) and +5 (2026-Q2) of about 7.7 million (Dev probes 43323654663,
# 43326898115; the other columns add up exactly). A misread cell is off by thousands at least, so sums are checked
# to 1 / 10,000 of SUMA (at least 1); the differences stay visible in Silver (test
# internet_gov_broadband_coverage_complete applies the same rule).
SUM_TOLERANCE = 10_000
SHARE = re.compile(r"^(?:\d{1,3},\d{2}%|100%)$")


@dataclass(frozen=True)
class Table:
    code: str
    title: str  # printed after "Tabela <n>: "
    header: tuple[str, ...]  # printed column headings, in order (label column first)
    columns: tuple[str, ...]  # column_code per number column
    labels: tuple[str, ...]  # every row label the table must have, besides SUMA


TABLES = (
    Table(
        "territory",
        "Liczba punktów adresowych w zasięgu na terenie całego kraju.",
        ("Województwo", "Liczba zasięgów", "Liczba punktów adresowych objętych zasięgiem",
         "Liczba punktów adresowych ogółem"),
        ("coverages", "address_points", "address_points_total"),
        VOIVODESHIPS,
    ),
    Table(
        "service_type",
        "Liczba punktów adresowych w podziale na rodzaj usług.",
        ("", "Liczba adresów", "Udział %"),
        ("address_points", "address_points_share"),
        SERVICE_TYPES,
    ),
    Table(
        "bandwidth",
        "Liczba zasięgów i punktów adresowych w podziale na przepustowość.",
        ("Przepustowość [Mb/s]", "Liczba zasięgów", "Udział %", "Liczba punktów adresowych", "Udział %"),
        ("coverages", "coverages_share", "address_points", "address_points_share"),
        BANDWIDTHS,
    ),
)
TITLE = re.compile(r"^Tabela (\d+): (.*)$")


def cells(line: str) -> list[str]:
    """A layout line split into cells at two or more spaces."""
    return [c for c in re.split(r" {2,}", line.strip()) if c]


def is_number(text: str) -> bool:
    return bool(COUNT.match(text) or SHARE.match(text))


def number(text: str) -> int:
    return int(re.sub(r"[   ]", "", text))


def share(text: str) -> float:
    return float(text.rstrip("%").replace(",", "."))


def reference_date(pages: list[str]) -> tuple[str, tuple[int, int, int]]:
    """The printed reference date ("30 września 2026") and (year, month, day); exactly one distinct date."""
    text = " ".join(" ".join(pages).split())
    found = set(re.findall(r"według stanu na dzień (\d{1,2}) (\w+) (\d{4})", text))
    if len(found) != 1:
        raise ValueError(f"reference date: expected one 'według stanu na dzień' date, found {sorted(found)}")
    day, month, year = found.pop()
    if month not in MONTHS:
        raise ValueError(f"reference date: unknown month {month!r}")
    return f"{day} {month} {year}", (int(year), MONTHS[month], int(day))


def header_text(lines: list[str]) -> str:
    return " ".join(" ".join(lines).split())


def header_matches(lines: list[str], expected: str) -> bool:
    """A one-line heading must read as expected (spaces aside); a heading set on several lines interleaves its
    columns line by line, so there the words must be the same (column order is then checked by the row rules)."""
    if len(lines) == 1:
        return header_text(lines).replace(" ", "") == expected.replace(" ", "")
    return sorted(header_text(lines).split()) == sorted(expected.split())


def body(page: str) -> list[str]:
    """The page's layout lines without the running head (page number and section name) and the footer."""
    lines = [line for line in page.split("\n") if line.strip() and line.strip() != "internet.gov.pl"]
    if lines and re.match(r"^\s*\d{1,3} {2,}\D", lines[0]):
        lines = lines[1:]
    return lines


def read_table(spec: Table, number_text: str, page_lines: list[tuple[int, list[str]]]) -> list[dict]:
    """The cells of one table: ``page_lines`` = (page number, layout lines after the title line) for the title's page
    and the pages after it. Raises ValueError on any mismatch."""
    expected_header = header_text(list(spec.header))
    rows: list[tuple[int, str, list[str]]] = []  # (page, label, number cells)
    header_seen = 0
    pending: list[str] = []  # lines between a header start and the first row (multi-line headings)
    prefix = ""  # first line of a label printed on two lines
    done = False
    for page, lines in page_lines:
        if done:
            break
        for line in lines:
            parts = cells(line)
            if not parts:
                continue
            numbers = [p for p in parts if is_number(p)]
            if not numbers:
                if rows and rows[-1][1].endswith("-") and len(parts) == 1 and not pending:
                    joined = rows[-1][1] + parts[0]
                    if joined in spec.labels:  # a label printed on two lines, numbers on the first
                        rows[-1] = (rows[-1][0], joined, rows[-1][2])
                        continue
                if rows and len(parts) == 1 and parts[0].endswith("-") and not pending and not prefix:
                    prefix = parts[0]  # a label printed on two lines, numbers on the second
                    continue
                pending.append(line)
                continue
            if pending:  # the heading lines just before this row (first page or a repeated header)
                printed = header_text(pending)
                if not header_matches(pending, expected_header):
                    if not rows and not header_seen:
                        raise ValueError(f"{spec.code}: header {printed!r} is not {expected_header!r}")
                    if rows:
                        raise ValueError(f"{spec.code}: text {printed!r} inside the table")
                header_seen += 1
                pending = []
            label_parts = parts[: len(parts) - len(numbers)]
            if parts[len(label_parts):] != numbers:
                raise ValueError(f"{spec.code}: label and numbers interleaved in {line.strip()!r}")
            label = prefix + " ".join(label_parts) if label_parts else ""
            if prefix and label not in spec.labels:
                raise ValueError(f"{spec.code}: {prefix!r} does not continue as a known label ({label!r})")
            prefix = ""
            if spec.code == "bandwidth" and not label_parts:  # the class is a number itself
                label, numbers = numbers[0], numbers[1:]
            rows.append((page, label, numbers))
            if label == "SUMA":
                done = True
                break
        else:
            pending = [] if rows else pending  # page footer / header text does not carry over pages
    if not rows or rows[-1][1] != "SUMA":
        raise ValueError(f"{spec.code}: no SUMA row")
    labels = [r[1] for r in rows[:-1]]
    if sorted(labels) != sorted(spec.labels) or len(set(labels)) != len(labels):
        missing, extra = sorted(set(spec.labels) - set(labels)), sorted(set(labels) - set(spec.labels))
        raise ValueError(f"{spec.code}: row labels differ (missing {missing}, unexpected {extra}, rows {len(labels)})")
    width = len(spec.columns)
    for page, label, numbers in rows:
        if len(numbers) != width:
            raise ValueError(f"{spec.code}: row {label!r} has {len(numbers)} number cells, expected {width}")
        for code, text in zip(spec.columns, numbers):
            if code.endswith("_share") != bool(SHARE.match(text)) or not (COUNT.match(text) or SHARE.match(text)):
                raise ValueError(f"{spec.code}: row {label!r} column {code}: {text!r} is not a {code}")
    total = rows[-1][2]
    for i, code in enumerate(spec.columns):
        if code.endswith("_share"):
            if share(total[i]) != 100.0:
                raise ValueError(f"{spec.code}: SUMA share {total[i]!r} is not 100%")
            count_index = i - 1
            for _, label, numbers in rows[:-1]:
                exact = 100.0 * number(numbers[count_index]) / number(total[count_index])
                if abs(share(numbers[i]) - exact) > 0.0051:
                    raise ValueError(f"{spec.code}: row {label!r} share {numbers[i]!r} is not {exact:.4f}% of SUMA")
        else:
            difference = sum(number(r[2][i]) for r in rows[:-1]) - number(total[i])
            if abs(difference) > max(1, number(total[i]) // SUM_TOLERANCE):
                raise ValueError(f"{spec.code}: column {code} adds up to SUMA {total[i]!r} {difference:+d}")
    columns = dict(zip(spec.columns, range(len(spec.columns))))
    if "coverages" in columns and "address_points" in columns:  # every address point in coverage has a coverage
        for _, label, numbers in rows:
            if number(numbers[columns["coverages"]]) < number(numbers[columns["address_points"]]):
                raise ValueError(f"{spec.code}: row {label!r} has fewer coverages than address points in coverage")
    if "address_points_total" in columns:
        for _, label, numbers in rows:
            if number(numbers[columns["address_points"]]) > number(numbers[columns["address_points_total"]]):
                raise ValueError(f"{spec.code}: row {label!r} has more address points in coverage than in total")
    out = []
    for row_number, (page, label, numbers) in enumerate(rows, start=1):
        for i, text in enumerate(numbers):
            out.append(
                {
                    "table_code": spec.code,
                    "table_number": number_text,
                    "table_title": spec.title,
                    "page_number": page,
                    "row_number": row_number,
                    "column_number": i + 2,
                    "row_label": label,
                    "column_header": spec.header[i + 1],
                    "column_code": spec.columns[i],
                    "value_text": text,
                }
            )
    return out


def read_report(pages: list[str], quarter_end: tuple[int, int, int]) -> tuple[str, list[dict]]:
    """(printed reference date, cells of the three tables) from the layout text of every page."""
    printed, date = reference_date(pages)
    if date != quarter_end:
        raise ValueError(f"reference date {printed!r} is not the edition's quarter end {quarter_end}")
    found: dict[str, list[dict]] = {}
    for index, page in enumerate(pages):
        lines = body(page)
        for n, line in enumerate(lines):
            match = TITLE.match(line.strip())
            if not match:
                continue
            title = " ".join(match.group(2).split())
            spec = next((t for t in TABLES if t.title == title), None)
            if spec is None:
                continue
            if spec.code in found:
                raise ValueError(f"{spec.code}: title printed twice")
            following = [(index + 1, lines[n + 1 :])] + [(k + 1, body(pages[k])) for k in range(index + 1, len(pages))]
            found[spec.code] = read_table(spec, match.group(1), following)
    missing = [t.code for t in TABLES if t.code not in found]
    if missing:
        raise ValueError(f"tables not found by title: {missing}")
    return printed, [c for t in TABLES for c in found[t.code]]
