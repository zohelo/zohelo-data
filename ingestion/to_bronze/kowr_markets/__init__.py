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
          the values are numbers or "-" as printed; the zł/t price equals the EUR price × the NBP rate (±1 % + 1 zł).
eu        "Ceny zakupu zbóż w krajach UE": the 27 member states and "Średnio w UE" (exactly this list, in this order),
          then three blocks (wheat, barley, maize, named in this order under the table) of the rows "Cena w EUR/t",
          "tygodniowa", "miesięczna", "roczna" (changes in %) and "Cena w PLN/t", 28 values each (the PLN row may be
          empty, as in 2025), for the report week ("w okresie 12-18.10.2026"); stated source "Komisji Europejskiej".
          Guards: the list and block order, 28 values per row, the week equals the file's week, EUR × the ECB rate of
          the table equals the PLN value (±1 % + 2 zł) wherever both are printed.
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
NO_DATA = {'"-" brak danych', "- brak danych"}

PRICE = re.compile(r"-|\d+(?:,\d+)?")
CHANGE = re.compile(r"-|-?\d+(?:,\d+)?|####")
GROUPED = r"-|[1-9]\d{0,2}(?: \d{3})*"
FOB_ROW = re.compile(r"(.+?) EUR/t (.+)")
FOB_HEADER = re.compile(r"Towar Jedn\. miary Cena ?z (\d{1,2}\.\d{2}\.\d{4}) Zmiana ceny w % Cena w zł/t")
NBP_RATE = re.compile(r"Średni kurs NBP z dnia \d{1,2}\.\d{2}\.\d{4} r\.: EUR = (\d+,\d+) zł\.")
ECB_RATE = re.compile(r"1 EUR = (\d+,\d+) PLN \(wg EBC - \d{1,2}\.\d{2}\.\d{4} r\.\)")
SOURCE_LINE = re.compile(r"Źródło: (.+)")
LAST_DATE = re.compile(r"(\d{1,2})\.(\d{2})\.(\d{4})(?:\D*)$")
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


def week_end(text: str, end: date, where: str) -> str:
    """The period text, checked to end on the report week's Sunday."""
    match = LAST_DATE.search(text.strip())
    if not match or date(int(match.group(3)), int(match.group(2)), int(match.group(1))) != end:
        raise Layout(f"{where}: period {text!r} does not end on {end:%d.%m.%Y}")
    return text.strip()


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


def fob(lines: list[str]) -> tuple[str, str, list[tuple[str, str, str, str]]]:
    """(quote date text, stated source, cells (group, market, column, value))."""
    header = [m for m in map(FOB_HEADER.fullmatch, lines) if m]
    rate = [m for m in map(NBP_RATE.fullmatch, lines) if m]
    if len(header) != 1 or len(rate) != 1 or "tygodniowa miesięczna roczna" not in lines:
        raise Layout(f"fob: header {len(header)}, NBP rate lines {len(rate)}")
    nbp, source = number(rate[0].group(1)), stated(lines, "fob")
    group, cells, seen = None, [], set()
    for line in lines:
        if line in FOB_MARKETS:
            group = line
            continue
        row = FOB_ROW.fullmatch(line)
        if not row:
            if (
                line
                and line not in NO_DATA
                and line != "tygodniowa miesięczna roczna"
                and not (FOB_HEADER.fullmatch(line) or NBP_RATE.fullmatch(line) or SOURCE_LINE.fullmatch(line))
            ):
                raise Layout(f"fob: unexpected line {line[:80]!r}")
            continue
        market, tokens = row.group(1), row.group(2).split(" ")
        if group is None or market not in FOB_MARKETS[group] or (group, market) in seen:
            raise Layout(f"fob: market {market!r} unknown, repeated or outside a group ({group})")
        seen.add((group, market))
        price, changes, pln = tokens[0], tokens[1:4], " ".join(tokens[4:])
        if (
            not PRICE.fullmatch(price)
            or len(changes) != 3
            or not all(CHANGE.fullmatch(c) for c in changes)
            or not re.fullmatch(GROUPED, pln)
        ):
            raise Layout(f"fob: values of {group} / {market} not as printed: {row.group(2)!r}")
        if (price == "-") != (pln == "-"):
            raise Layout(f"fob: {group} / {market}: EUR price {price!r} without zł/t price {pln!r} or the reverse")
        if price != "-" and abs(number(price) * nbp - number(pln)) > number(pln) / 100 + 1:
            raise Layout(f"fob: {group} / {market}: {price} EUR/t x {nbp} is not {pln} zł/t")
        cells += [(group, market, c, v) for c, v in zip(FOB_COLUMNS, [price, *changes, pln], strict=True)]
    if {g for g, _ in seen} != set(FOB_MARKETS):
        raise Layout(f"fob: commodity groups found {sorted({g for g, _ in seen})}")
    return header[0].group(1), source, cells


def eu(lines: list[str], end: date) -> tuple[str, str, list[tuple[str, str, str, str]]]:
    """(week text, stated source, cells (commodity, country, row label, value))."""
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
    rate = [m for m in map(ECB_RATE.fullmatch, tail) if m]
    period = [m for m in map(EU_PERIOD.fullmatch, tail) if m]
    groups = [x for x in tail if x in EU_GROUPS]
    if len(rate) != 1 or len(period) != 1 or groups != EU_GROUPS:
        raise Layout(f"eu: ECB rate lines {len(rate)}, period lines {len(period)}, groups {groups}")
    for line in tail:
        if (
            line
            and line not in EU_TAIL
            and not (ECB_RATE.fullmatch(line) or EU_PERIOD.fullmatch(line) or SOURCE_LINE.fullmatch(line))
        ):
            raise Layout(f"eu: unexpected line {line[:80]!r}")
    week, source, ecb = week_end(period[0].group(1), end, "eu"), stated(tail, "eu"), number(rate[0].group(1))
    cells = []
    for i, (label, values) in enumerate(rows):
        group = EU_GROUPS[i // len(EU_ROWS)]
        if not (len(values) == len(COUNTRIES) or (label == "Cena w PLN/t" and not values)):
            raise Layout(f"eu: {group} / {label}: {len(values)} values for {len(COUNTRIES)} columns")
        valid = re.compile(r"-|\d+") if label == "Cena w PLN/t" else PRICE if label == "Cena w EUR/t" else CHANGE
        if not all(valid.fullmatch(v) for v in values):
            raise Layout(f"eu: {group} / {label}: values not as printed: {values[:6]}")
        cells += [(group, c, label, v) for c, v in zip(COUNTRIES, values, strict=False)]
        if label == "Cena w PLN/t" and values:
            for country, eur, pln in zip(COUNTRIES, rows[i - 4][1], values, strict=True):
                if (
                    eur not in ("-", "0")
                    and pln != "-"
                    and abs(number(eur) * ecb - number(pln)) > number(pln) / 100 + 2
                ):
                    raise Layout(f"eu: {group} / {country}: {eur} EUR/t x {ecb} is not {pln} PLN/t")
    return week, source, cells


def exchange(lines: list[str], end: date) -> tuple[str, str, list[tuple[str, str, str, str]]]:
    """(week text, stated source, cells ('', commodity, column, value))."""
    period = [m for m in map(EXCHANGE_PERIOD.fullmatch, lines) if m]
    if len(period) != 1 or not set(lines) >= EXCHANGE_HEADERS:
        raise Layout(f"exchange: period lines {len(period)}, headers {sorted(EXCHANGE_HEADERS - set(lines))} missing")
    week, source = week_end(period[0].group(1), end, "exchange"), stated(lines, "exchange")
    cells, seen = [], set()
    for line in lines:
        row = EXCHANGE_ROW.fullmatch(line)
        if not row:
            if (
                line
                and line not in NO_DATA | EXCHANGE_HEADERS
                and not (EXCHANGE_PERIOD.fullmatch(line) or SOURCE_LINE.fullmatch(line))
            ):
                raise Layout(f"exchange: unexpected line {line[:80]!r}")
            continue
        good, values = row.group(1), list(row.groups()[1:])
        if good not in EXCHANGE_GOODS or good in seen:
            raise Layout(f"exchange: commodity {good!r} unknown or repeated")
        seen.add(good)
        low, high, mean = values[:3]
        if "-" not in (low, high, mean) and not number(low) <= number(mean) <= number(high):
            raise Layout(f"exchange: {good}: not minimum <= average <= maximum: {values[:3]}")
        cells += [("", good, c, v) for c, v in zip(EXCHANGE_COLUMNS, values, strict=True)]
    if not seen:
        raise Layout("exchange: no commodity row")
    return week, source, cells


def parse(pages: list[str], monday: date) -> list[dict[str, Any]]:
    """The cells of the three tables of one report (one string per page). Raises Layout."""
    end = monday + timedelta(6)
    out = []
    for code in TITLES:
        page, lines = locate(pages, code)
        period, source, cells = (
            fob(lines) if code == "fob" else eu(lines, end) if code == "eu" else exchange(lines, end)
        )
        for group, row, column, value in cells:
            out.append(
                {
                    "page_number": page,
                    "table_code": code,
                    "table_title": TITLES[code],
                    "period_text": period,
                    "stated_source": source,
                    "row_group": group,
                    "row_label": row,
                    "column_label": column,
                    "value_text": value,
                }
            )
    return out


def text_pages(body: bytes) -> list[str]:
    """The text layer of every page (pypdf, plain mode: one line per table row)."""
    from pypdf import PdfReader
    from pypdf.errors import PdfReadError

    if not body.startswith(b"%PDF"):
        raise Layout("not a PDF file")
    try:
        return [page.extract_text() or "" for page in PdfReader(io.BytesIO(body)).pages]
    except PdfReadError as exc:
        raise Layout(f"unreadable PDF: {exc}") from exc


def monday_of(entity: str) -> date:
    match = re.fullmatch(r"grain_(\d{4})(\d{2})(\d{2})", entity)
    if not match:
        raise Layout(f"entity {entity!r} is not grain_<yyyymmdd>")
    return date(*(int(g) for g in match.groups()))


def decode(body: bytes, receipt: dict[str, Any]):
    """bulk_bronze decoder: the table cells of one report."""
    from ingestion.common import bulk_bronze

    base = bulk_bronze.provenance(receipt)
    cells = parse(text_pages(body), monday_of(receipt["entity"]))
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
