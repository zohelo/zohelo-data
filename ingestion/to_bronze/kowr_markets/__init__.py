"""KOWR weekly grain market reports: Landing → Bronze activities (register PLS-033).

bronze.load       newest copy of every landed report → bronze.kowr_markets_cells (one row per cell of three tables of
                  the PDF's text layer); a file whose tables are not recognised is rejected, with the reason, into
                  bronze.kowr_markets_rejected_files
bronze.verify     ingestion/common/lineage.py --source kowr_markets
landing.archive   ingestion/common/archive.py --source kowr_markets (move to Archive)
archive.prune     ingestion/common/retention.py --source kowr_markets (archive_policy.retention)

The reports (Excel tables printed to PDF; pypdf's plain text layer keeps each table row on one line, the column
headers come after the rows) hold three tables that KOWR compiles and ZSRIR does not carry. Each is found by its
title line, and only the lines after that title on its page are read:

fob       "Eksportowe ceny zbóż na rynkach zagranicznych (na bazie FOB)": per commodity group (milling wheat, feed
          barley, maize) one row per export market, "<market> EUR/t <price> <3 changes in %> <price in zł/t>", the
          price as of the date in the header ("Cena z 21.10.2026"); stated source "KE, EBC, NBP". Guards: the header,
          the NBP rate line and the source line are present; every row is in a known group with a known market, once;
          the values are numbers or "-" as printed. A row whose zł/t price is not the EUR price × the NBP rate (±1 % +
          1 zł) gets a cell "kontrola" (Silver loads no price from it); some rows print no zł/t price, or nothing.
eu        "Ceny zakupu zbóż w krajach UE": the 27 member states and "Średnio w UE" (exactly this list, in this order),
          then three blocks of the rows "Cena w EUR/t",
          "tygodniowa", "miesięczna", "roczna" (changes in %) and "Cena w PLN/t", 28 values each (the PLN row may be
          empty, as in 2025), for the report week ("w okresie 12-18.10.2026"); stated source "Komisji Europejskiej".
          The commodity of a block (wheat, barley, maize: a merged label cell) is read from the text positions, as
          the text layer prints the labels in no fixed order. Guards: the list and row order, one label inside each
          block, 28 values per row, the week equals the file's week, at most two cells per block where EUR × the ECB
          rate of the table is not the PLN value (±1 % + 2 zł; each gets a "kontrola" cell, more reject the file).
exchange  "Ceny wybranych zbóż na krajowych giełdach towarowych": sale offers in zł/t (minimum, maximum, average)
          and three changes of the average in %, one row per known commodity, for the report week ("Oferty za okres
          12-18.10.2026"); stated source "krajowych giełd towarowych". Guards: the headers, known commodities once,
          the week, minimum ≤ average ≤ maximum.

The ZSRIR weekly price table, the charts, the summary box and the narrative (MATIF quotations from Kaack
Terminhandel, GUS monthly prices and purchases) are not read. Values are stored as printed ("-" = "brak danych",
"####" an overflowing cell, "0"); anything else rejects the whole file.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
import io
import re
from typing import Any

SOURCE = "kowr_markets"
COLUMNS = [
    ("entity", "VARCHAR"),
    ("page_number", "BIGINT"),
    ("table_code", "VARCHAR"),
    ("table_title", "VARCHAR"),
    ("period_text", "VARCHAR"),
    ("period_start", "VARCHAR"),
    ("period_end", "VARCHAR"),
    ("stated_source", "VARCHAR"),
    ("row_group", "VARCHAR"),
    ("row_label", "VARCHAR"),
    ("column_label", "VARCHAR"),
    ("value_text", "VARCHAR"),
    ("raw_sha256", "VARCHAR"),
    ("source_version", "VARCHAR"),
    ("ingested_at", "TIMESTAMPTZ"),
    ("source_row_number", "BIGINT"),
]
TITLES = {
    "fob": "Eksportowe ceny zbóż na rynkach zagranicznych (na bazie FOB)",
    "eu": "Ceny zakupu zbóż w krajach UE",
    "exchange": "Ceny wybranych zbóż na krajowych giełdach towarowych",
}
FOB_MARKETS = {
    "Pszenica konsumpcyjna": {"HRW (USA, Gulf)", "SRW (USA, Gulf)", "Francja (Rouen)", "porty Morza Czarnego"},
    "Jęczmień paszowy": {"porty Morza Czarnego", "Francja (Rouen)"},
    "Kukurydza": {"USA (Gulf)", "porty Morza Czarnego", "Francja (Bordeaux)"},
}
FOB_COLUMNS = ["cena", "zmiana_tygodniowa", "zmiana_miesieczna", "zmiana_roczna", "cena_zl_t"]
COUNTRIES = [
    "Austria", "Belgia", "Bułgaria", "Chorwacja", "Cypr", "Czechy", "Dania", "Estonia", "Finlandia", "Francja",
    "Grecja", "Hiszpania", "Holandia", "Irlandia", "Litwa", "Luksemburg", "Łotwa", "Malta", "Niemcy", "Polska",
    "Portugalia", "Rumunia", "Słowacja", "Słowenia", "Szwecja", "Węgry", "Włochy", "Średnio w UE",
]  # fmt: skip
EU_ROWS = ["Cena w EUR/t", "tygodniowa", "miesięczna", "roczna", "Cena w PLN/t"]
EU_GROUPS = ["Pszenica", "Jęczmień", "Kukurydza"]
EU_TAIL = {"- brak danych", "Procentowa", "zmiana", "ceny w EUR", *EU_GROUPS}
EXCHANGE_GOODS = {"pszenica konsumpcyjna", "pszenica paszowa", "żyto konsumpcyjne", "jęczmień paszowy", "kukurydza"}
EXCHANGE_COLUMNS = ["minimalna", "maksymalna", "średnia", "zmiana_tygodniowa", "zmiana_miesieczna", "zmiana_roczna"]
EXCHANGE_HEADERS = {"minimalna maksymalna średnia tygodniowa miesięczna roczna", "oferty sprzedaży (zł/t)", "Towar"}
NO_DATA = re.compile(r"""['"„”]?-['"”]? brak danych""")  # '"-" brak danych', '- brak danych', ''-" brak danych'


PRICE = re.compile(r"-|\d+(?:,\d+)?")
CHANGE = re.compile(r"-|-?\d+(?:,\d+)?|####")
GROUPED = r"-|[1-9]\d{0,2}(?: \d{3})*"
FOB_ROW = re.compile(r"(.+?) EUR/t(?: (.+))?")
# "Towar Jedn. miary Cenaz 21.10.2026  Zmiana ceny w % Cena w zł/t"
FOB_HEADER = re.compile(r"Towar Jedn\. ?miary .*?(\d{1,2}\.\d{2}\.\d{4}).*zł/t(?: tygodniowa miesięczna roczna)?")
# "Średni kurs NBP z dnia 21.10.2026 r.: EUR = 4,3716 zł." (FOB), "1 EUR = 4,3695 PLN (wg EBC - 22.10.2026 r.)" (EU)
RATE = re.compile(r"EUR ?= ?(\d[,.]\d{2,4}) ?(?:zł|PLN)")
SOURCE_LINE = re.compile(r"Źródło: (.+)")
# "12-18.10.2026 r.", "9-15.11.2026", "07-13.07.25", "27.03 - 2.04.2023", "30.06-06.07.25", "18-31.12.2023",
# "30.12.2024 - 05.01.2025", "29.12.2025 r. - 04.01.2026 r."
PERIOD = re.compile(
    r"(\d{1,2})(?:\.(\d{2}))?(?:\.(\d{2}|\d{4}))?(?: r\.)? ?- ?(\d{1,2})\.(\d{2})\.(\d{2}|\d{4})(?: r\.)?"
)
EXCHANGE_ROW = re.compile(
    rf"([a-ząćęłńóśźż ]+?) ({GROUPED}) ({GROUPED}) ({GROUPED}) ({CHANGE.pattern}) "
    rf"({CHANGE.pattern}) ({CHANGE.pattern})"
)
EXCHANGE_PERIOD = re.compile(r"Oferty za okres (.+?) Zmiana średniej ceny w %")
EU_PERIOD = re.compile(r"w okresie (.+)")


class Layout(ValueError):
    """The text layer does not have the expected table shape: the file is rejected, never guessed."""


def number(text: str) -> Decimal:
    return Decimal(text.replace(" ", "").replace(",", "."))


def period_of(text: str, end: date, where: str) -> tuple[str, str, str]:
    """(period text, first day, last day) of a weekly table. The EU and exchange tables often show the week before
    the report's, once two weeks (18-31.12.2023 in the report of 18-24.12.2023; Dev trial 45910578662): a period is
    accepted when it runs Monday to Sunday over one or two weeks and ends between four weeks before and one week after
    the report week's Sunday."""
    match = PERIOD.fullmatch(text.strip())
    if not match:
        raise Layout(f"{where}: period {text!r} not read")
    d1, m1, y1, d2, m2, y2 = match.groups()
    year = int(y2) + (2000 if len(y2) == 2 else 0)
    try:
        last = date(year, int(m2), int(d2))
        start_year = int(y1) + (2000 if len(y1) == 2 else 0) if y1 else year - (int(m1 or m2) > int(m2))
        first = date(start_year, int(m1 or m2), int(d1))
    except ValueError as exc:
        raise Layout(f"{where}: period {text!r}: {exc}") from exc
    if (
        first.weekday() != 0
        or last.weekday() != 6
        or (last - first).days not in (6, 13)
        or not (end - timedelta(28) <= last <= end + timedelta(7))
    ):
        raise Layout(f"{where}: period {text!r} is not one or two weeks ending near {end:%d.%m.%Y}")
    return text.strip(), first.isoformat(), last.isoformat()


def norm(line: str) -> str:
    return " ".join(line.replace("\xa0", " ").split())


def locate(pages: list[str], code: str) -> tuple[int, list[str]]:
    """(page number, the lines after the table's title on its page); the title must be printed exactly once."""
    hits = []
    for page_number, text in enumerate(pages, start=1):
        lines = [norm(x) for x in text.splitlines()]
        hits += [(page_number, lines[i + 1 :]) for i, x in enumerate(lines) if x == TITLES[code]]
    if len(hits) != 1:
        raise Layout(f"{code}: title {TITLES[code]!r} printed {len(hits)} times")
    return hits[0]


def stated(lines: list[str], code: str) -> str:
    found = [m.group(1) for m in map(SOURCE_LINE.fullmatch, lines) if m]
    if len(found) != 1:
        raise Layout(f"{code}: {len(found)} 'Źródło:' lines")
    return found[0]


def fob(lines: list[str]) -> tuple[tuple[str, str, str], str, list[tuple[str, str, str, str]]]:
    """((quote date text, ISO date, ISO date), stated source, cells (group, market, column, value))."""
    header = [m for m in map(FOB_HEADER.fullmatch, lines) if m]
    rate = [m for m in map(RATE.search, lines) if m]
    if len(header) != 1 or len(rate) > 1 or not any("tygodniowa miesięczna roczna" in x for x in lines):
        raise Layout(f"fob: header lines {len(header)}, rate lines {len(rate)}")
    nbp, source = number(rate[0].group(1)) if rate else None, stated(lines, "fob")
    group, cells, seen = None, [], set()
    for line in lines:
        if line in FOB_MARKETS:
            group = line
            continue
        row = FOB_ROW.fullmatch(line)
        if not row:
            if (
                line
                and not NO_DATA.fullmatch(line)
                and line != "tygodniowa miesięczna roczna"
                and not (FOB_HEADER.fullmatch(line) or RATE.search(line) or SOURCE_LINE.fullmatch(line))
            ):
                raise Layout(f"fob: unexpected line {line[:80]!r}")
            continue
        market, tokens = row.group(1), (row.group(2) or "").split(" ")
        if group is None or market not in FOB_MARKETS[group] or (group, market) in seen:
            raise Layout(f"fob: market {market!r} unknown, repeated or outside a group ({group})")
        seen.add((group, market))
        if tokens in (["0"], ["-"], [""]):  # "... EUR/t 0" (Bordeaux 2023), "... EUR/t -", "... EUR/t" (2024)
            cells += [(group, market, "cena", tokens[0])] if tokens[0] else []
            continue
        price, changes, pln = tokens[0], tokens[1:4], " ".join(tokens[4:])
        if (
            not PRICE.fullmatch(price)
            or len(changes) != 3
            or not all(CHANGE.fullmatch(c) for c in changes)
            or not re.fullmatch(f"(?:{GROUPED})?", pln)  # no zł/t price at all on some Black Sea rows (Jan-Mar 2024)
        ):
            raise Layout(f"fob: values of {group} / {market} not as printed: {row.group(2)!r}")
        if price == "-" and pln not in ("-", ""):
            raise Layout(f"fob: {group} / {market}: a zł/t price {pln!r} without an EUR price")
        values = [price, *changes, *([pln] if pln else [])]
        cells += [(group, market, c, v) for c, v in zip(FOB_COLUMNS, values, strict=False)]
        if (
            nbp
            and price != "-"
            and pln not in ("-", "")
            and abs(number(price) * nbp - number(pln)) > number(pln) / 100 + 1
        ):
            # the producer's zł/t column disagrees with its EUR price (feed barley, 32 rows from July 2024):
            # which one is wrong cannot be told, so the row is marked and Silver loads no price from it
            cells.append((group, market, "kontrola", f"{price} EUR/t x {nbp} <> {pln} zł/t"))
    if {g for g, _ in seen} != set(FOB_MARKETS):
        raise Layout(f"fob: commodity groups found {sorted({g for g, _ in seen})}")
    quoted = header[0].group(1)
    day = re.match(r"(\d{1,2})\.(\d{2})\.(\d{4})", quoted)
    iso = date(int(day.group(3)), int(day.group(2)), int(day.group(1))).isoformat()
    return (quoted, iso, iso), source, cells


def eu_groups(frags: list[tuple[str, float, float]]) -> list[str]:
    """The commodity of each block, top to bottom, from the positions of the text on the page: the block's heading
    ("Pszenica", "Jęczmień", "Kukurydza") lies above its "Cena w EUR/t" row and below the "Cena w PLN/t" row of the
    block before (the first: at most one block height above). The text layer prints these headings in no fixed order
    (Dev trial 45902040402), so their order is never read from it."""
    eur = [y for t, _, y in frags if t.strip().startswith("Cena w EUR/t")]
    pln = [y for t, _, y in frags if t.strip().startswith("Cena w PLN/t")]
    labels = [(t.strip(), y) for t, _, y in frags if t.strip() in EU_GROUPS]
    if len(eur) != 3 or len(pln) != 3 or sorted(t for t, _ in labels) != sorted(EU_GROUPS):
        raise Layout(f"eu: positions: {len(eur)} EUR rows, {len(pln)} PLN rows, labels {[t for t, _ in labels]}")
    if eur != sorted(eur, reverse=True) or any(p >= e for e, p in zip(eur, pln, strict=True)):
        raise Layout(f"eu: positions: rows not printed top to bottom (EUR {eur}, PLN {pln})")
    order = []
    for k, (top, bottom) in enumerate(zip(eur, pln, strict=True)):
        ceiling = pln[k - 1] if k else top + (top - bottom)
        inside = [t for t, y in labels if top < y < ceiling]
        if len(inside) != 1:
            raise Layout(f"eu: positions: block {top:.0f}..{bottom:.0f} has headings {inside}")
        order.append(inside[0])
    if sorted(order) != sorted(EU_GROUPS):
        raise Layout(f"eu: positions: blocks labelled {order}")
    return order


def eu(lines: list[str], end: date, frags=None) -> tuple[tuple[str, str, str], str, list[tuple[str, str, str, str]]]:
    """((week text, first day, last day), stated source, cells (commodity, country, row label, value)). The commodity
    of each block comes from the text positions (eu_groups); without them (tests on a text dump) the printed order."""
    countries = [x for x in lines if x in COUNTRIES]
    if countries != COUNTRIES:
        raise Layout(f"eu: countries not as expected: {len(countries)} of {len(COUNTRIES)}")
    rows, tail = [], []
    for line in lines:
        label = next((r for r in EU_ROWS if line == r or line.startswith(r + " ")), None)
        if label:
            rows.append((label, line[len(label) :].split()))
        elif rows:
            tail.append(line)
        elif line and line not in COUNTRIES:
            raise Layout(f"eu: unexpected line before the rows {line[:80]!r}")
    if [r for r, _ in rows] != EU_ROWS * 3:
        raise Layout(f"eu: row labels {[r for r, _ in rows]}")
    rate = [m for m in map(RATE.search, tail) if m]
    period = [m for m in map(EU_PERIOD.fullmatch, tail) if m]
    groups = eu_groups(frags) if frags is not None else [x for x in tail if x in EU_GROUPS]
    if len(rate) > 1 or len(period) != 1 or sorted(groups) != sorted(EU_GROUPS):
        raise Layout(f"eu: rate lines {len(rate)}, period lines {len(period)}, groups {groups}")
    for line in tail:
        if (
            line
            and line not in EU_TAIL
            and not NO_DATA.fullmatch(line)
            and not (RATE.search(line) or EU_PERIOD.fullmatch(line) or SOURCE_LINE.fullmatch(line))
        ):
            raise Layout(f"eu: unexpected line {line[:80]!r}")
    week, source, ecb = (
        period_of(period[0].group(1), end, "eu"),
        stated(tail, "eu"),
        number(rate[0].group(1)) if rate else None,
    )
    cells = []
    for i, (label, values) in enumerate(rows):
        group = groups[i // len(EU_ROWS)]
        if not (len(values) == len(COUNTRIES) or (label == "Cena w PLN/t" and not values)):
            raise Layout(f"eu: {group} / {label}: {len(values)} values for {len(COUNTRIES)} columns")
        valid = re.compile(r"-|\d+") if label == "Cena w PLN/t" else PRICE if label == "Cena w EUR/t" else CHANGE
        if not all(valid.fullmatch(v) for v in values):
            raise Layout(f"eu: {group} / {label}: values not as printed: {values[:6]}")
        cells += [(group, c, label, v) for c, v in zip(COUNTRIES, values, strict=False)]
        if label == "Cena w PLN/t" and values and ecb:
            off = [
                (country, eur, pln)
                for country, eur, pln in zip(COUNTRIES, rows[i - 4][1], values, strict=True)
                # Polska is skipped: its PLN price is ZSRIR's, its EUR price the Commission's own conversion
                if country != "Polska"
                and eur not in ("-", "0")
                and pln != "-"
                and abs(number(eur) * ecb - number(pln)) > number(pln) / 100 + 2
            ]
            if len(off) > 2:  # the columns are shifted, not one cell mistyped
                raise Layout(f"eu: {group}: EUR x {ecb} is not the PLN price in {len(off)} columns: {off[:3]}")
            # one or two cells where the producer's PLN price disagrees with its EUR price: marked, not loaded
            cells += [(group, c, "kontrola", f"{e} EUR/t x {ecb} <> {p} PLN/t") for c, e, p in off]
    return week, source, cells


def exchange(lines: list[str], end: date) -> tuple[tuple[str, str, str], str, list[tuple[str, str, str, str]]]:
    """((week text, first day, last day), stated source, cells ('', commodity, column, value))."""
    period = [m for m in map(EXCHANGE_PERIOD.fullmatch, lines) if m]
    if len(period) != 1 or not set(lines) >= EXCHANGE_HEADERS:
        raise Layout(f"exchange: period lines {len(period)}, headers {sorted(EXCHANGE_HEADERS - set(lines))} missing")
    sources = [m.group(1) for m in map(SOURCE_LINE.fullmatch, lines) if m]
    if len(sources) > 1:
        raise Layout(f"exchange: {len(sources)} 'Źródło:' lines")
    # two reports of January 2026 print no source line under this table: stated_source stays empty
    week, source = period_of(period[0].group(1), end, "exchange"), (sources or [""])[0]
    cells, seen = [], set()
    for line in lines:
        row = EXCHANGE_ROW.fullmatch(line)
        if not row:
            if (
                line
                and line not in EXCHANGE_HEADERS
                and not NO_DATA.fullmatch(line)
                and not (EXCHANGE_PERIOD.fullmatch(line) or SOURCE_LINE.fullmatch(line))
            ):
                raise Layout(f"exchange: unexpected line {line[:80]!r}")
            continue
        good, values = row.group(1), list(row.groups()[1:])
        if good not in EXCHANGE_GOODS or good in seen:
            raise Layout(f"exchange: commodity {good!r} unknown or repeated")
        seen.add(good)
        low, high, mean = values[:3]
        cells += [("", good, c, v) for c, v in zip(EXCHANGE_COLUMNS, values, strict=True)]
        if "-" not in (low, high, mean) and not number(low) <= number(mean) <= number(high):
            # the producer's average lies outside its own minimum and maximum (2 reports): marked, Silver loads none
            cells.append(("", good, "kontrola", f"min {low} max {high} avg {mean}"))
    if not seen:
        raise Layout("exchange: no commodity row")
    return week, source, cells


def parse(
    pages: list[str], monday: date, frags: list[list[tuple[str, float, float]]] | None = None
) -> list[dict[str, Any]]:
    """The cells of the three tables of one report (one string per page; the text fragments with their positions
    per page, for the EU table). Raises Layout."""
    end = monday + timedelta(6)
    out = []
    for code in TITLES:
        page, lines = locate(pages, code)
        (period, first, last), source, cells = (
            fob(lines)
            if code == "fob"
            else eu(lines, end, None if frags is None else frags[page - 1])
            if code == "eu"
            else exchange(lines, end)
        )
        for group, row, column, value in cells:
            out.append(
                {
                    "page_number": page,
                    "table_code": code,
                    "table_title": TITLES[code],
                    "period_text": period,
                    "period_start": first,
                    "period_end": last,
                    "stated_source": source,
                    "row_group": group,
                    "row_label": row,
                    "column_label": column,
                    "value_text": value,
                }
            )
    return out


def text_pages(body: bytes) -> tuple[list[str], list[list[tuple[str, float, float]]]]:
    """The text layer of every page (pypdf, plain mode: one line per table row) and its text fragments with their
    position on the page (x, y in points, y upwards: the text matrix times the current matrix)."""
    from pypdf import PdfReader
    from pypdf.errors import PdfReadError

    if not body.startswith(b"%PDF"):
        raise Layout("not a PDF file")
    texts, frags = [], []
    try:
        for page in PdfReader(io.BytesIO(body)).pages:
            found: list[tuple[str, float, float]] = []

            def visit(text, cm, tm, font, size, found=found):
                if text.strip():
                    x = tm[4] * cm[0] + tm[5] * cm[2] + cm[4]
                    y = tm[4] * cm[1] + tm[5] * cm[3] + cm[5]
                    found.append((text, float(x), float(y)))

            texts.append(page.extract_text(visitor_text=visit) or "")
            frags.append(found)
    except PdfReadError as exc:
        raise Layout(f"unreadable PDF: {exc}") from exc
    return texts, frags


def monday_of(entity: str) -> date:
    match = re.fullmatch(r"grain_(\d{4})(\d{2})(\d{2})", entity)
    if not match:
        raise Layout(f"entity {entity!r} is not grain_<yyyymmdd>")
    return date(*(int(g) for g in match.groups()))


def decode(body: bytes, receipt: dict[str, Any]):
    """bulk_bronze decoder: the table cells of one report."""
    from ingestion.common import bulk_bronze

    base = bulk_bronze.provenance(receipt)
    texts, frags = text_pages(body)
    cells = parse(texts, monday_of(receipt["entity"]), frags)
    rows = [{**base, **cell, "source_row_number": i} for i, cell in enumerate(cells, start=1)]
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
    from ingestion.to_landing import kowr_markets as landing

    return sorted(landing.held())


def _bronze_activities() -> dict[str, Any]:
    from ingestion.common import gus_publications

    return gus_publications.bronze_activities(SOURCE, _entities)


ACTIVITIES = {
    **_bronze_activities(),
    "bronze.load": lambda p: bronze_load(_entities()),
}
