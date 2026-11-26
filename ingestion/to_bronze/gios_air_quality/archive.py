"""GIOŚ yearly archive of verified measurements: decoder of one year's ZIP (#3119).

A yearly file ("Wyniki pomiarów z <year> roku", https://powietrze.gios.gov.pl/pjp/archives) is a ZIP of workbooks,
one per indicator and averaging time (``<year>_<indicator>_<averaging>.xlsx``: ``2025_PM10_1g.xlsx``,
``2013_PM2.5_24g.xlsx``, ``2025_Jony_PM25_24g.xlsx`` with several indicators). The first sheet is a wide table:
a header block of rows labelled in column A, one column per station and indicator, then one row per period with
the period in column A as an Excel serial number (hourly values stamped at the end of the hour: 01:00 to 24:00;
daily values at midnight) and the values as numbers or, 2016 to 2018, as text with a decimal comma. Header labels
seen (Dev probes 45747293591, 45750793415): ``Kod stacji, Wskaźnik, Czas uśredniania`` (2000 to 2015, some workbooks
without ``Wskaźnik``), ``Numer`` (2012), ``Nr, …, Jednostka, Czas pomiaru`` (2016 to 2018), ``Rok`` (2021), ``Nr, Kod
stacji, Wskaźnik, Czas uśredniania, Jednostka, Kod stanowiska`` (2019 to 2025). The header block is every row above
the first period; its rows are matched by their label, never by position, and a row without a label (2012) is kept
as ``header_unlabeled``.

Bronze keeps one row per non-empty value cell (empty cells are no measurement and are not records), every field as
text as DuckDB's ``read_xlsx`` prints it, with the column's header attributes as published, the member, the sheet
row and column and the provenance. A label outside LABELS, a header block without ``Kod stacji``, a value in a
column without a station code, a period cell that is not a number or lies more than five minutes off a whole hour
in a row holding values rejects the whole year (bronze.gios_air_quality_rejected_files). Members of a declared other
layout (SKIPPED: deposition, monthly with two date columns) and the further sheets of a workbook (a list of old
station codes, or the ions of 2015 and 2018 split by ion or station) are not loaded and are listed in the result.
"""

from __future__ import annotations

import io
from pathlib import Path
import re
import tempfile
from typing import Any
import zipfile

import duckdb

from ingestion.common import bulk_bronze

# Header label (column A) → Bronze column. "Numer" (2012) is "Nr"; "Czas pomiaru" (2016) labels the period column;
# None: a header row without a label.
LABELS = {
    "Nr": "column_number",
    "Numer": "column_number",
    "Kod stacji": "station_code",
    "Wskaźnik": "indicator",
    "Czas uśredniania": "averaging_time",
    "Jednostka": "unit",
    "Kod stanowiska": "sensor_code",
    "Czas pomiaru": "period_label",
    "Rok": "header_year",
    None: "header_unlabeled",
}
FIELDS = list(dict.fromkeys(LABELS.values()))
REQUIRED = ("Kod stacji",)
SKIPPED = {re.compile(r"(?i)\d{4}_depozycja(_1m)?\.xlsx"): "deposition: monthly, two date columns, another layout"}
MEMBER = re.compile(r"\d{4}_.+\.xlsx")
COLUMNS = [
    ("entity", "VARCHAR"),
    ("member", "VARCHAR"),
    ("sheet", "VARCHAR"),
    *[(c, "VARCHAR") for c in FIELDS],
    ("period", "VARCHAR"),
    ("value", "VARCHAR"),
    *bulk_bronze.PROVENANCE,
    ("sheet_row", "BIGINT"),
    ("sheet_column", "VARCHAR"),
]


class ArchiveDecodeError(ValueError):
    """The yearly file is not of a declared layout (the year is rejected)."""


def _dimension(workbook: zipfile.ZipFile, part: str) -> str | None:
    """The sheet's used range from <dimension ref="A1:GC8766"> (first bytes of the sheet part), or None."""
    with workbook.open(part) as handle:
        head = handle.read(4096).decode("utf-8", "replace")
    match = re.search(r'<dimension ref="(A1:[A-Z]+\d+)"', head)
    return match.group(1) if match else None


def _sheet(data: bytes, name: str) -> tuple[str, list[str], str | None]:
    from ingestion.common import xlsx_cells

    with zipfile.ZipFile(io.BytesIO(data)) as workbook:
        sheets = xlsx_cells.sheets(workbook)
        if not sheets:
            raise ArchiveDecodeError(f"{name}: no sheet")
        return sheets[0][0], [s for s, _ in sheets[1:]], _dimension(workbook, sheets[0][1])


def member(con, data: bytes, name: str, prov: dict[str, Any], work: Path, output: Path) -> dict[str, Any]:
    """One workbook → one Parquet file of value cells; returns {rows, layout, sheet, other_sheets}."""
    try:
        sheet, others, used = _sheet(data, name)
    except (zipfile.BadZipFile, KeyError) as exc:
        raise ArchiveDecodeError(f"{name}: not a workbook: {exc}") from exc
    path = work / "member.xlsx"
    path.write_bytes(data)
    quoted = sheet.replace("'", "''")
    try:
        con.execute("drop table if exists raw")
        con.execute(
            "create temp table raw as select cast(row_number() over () as bigint) as sheet_row, * from "
            f"read_xlsx('{path}', sheet = '{quoted}', header = false, all_varchar = true, stop_at_empty = false"
            + (f", range = '{used}')" if used else ")")
        )
    except duckdb.Error as exc:
        raise ArchiveDecodeError(f"{name}: read_xlsx failed: {str(exc)[:300]}") from exc
    columns = [r[0] for r in con.execute("describe raw").fetchall()][1:]
    first, rest = columns[0], columns[1:]
    labels: list[str | None] = []
    for (label,) in con.execute(f'select "{first}" from raw order by sheet_row limit 13').fetchall():
        if label is not None and re.fullmatch(r"-?\d+(\.\d+)?([eE][-+]?\d+)?", label.strip()):
            break
        labels.append(label.strip() if label is not None and label.strip() else None)
    named = [lab for lab in labels if lab is not None]
    unknown = [lab for lab in named if lab not in LABELS]
    if len(labels) > 12 or unknown or not all(r in named for r in REQUIRED) or len(set(named)) != len(named):
        raise ArchiveDecodeError(f"{name}: header block {labels} is not a declared layout")
    k = len(labels)
    header = con.execute(
        f"select {', '.join(f'"{c}"' for c in rest)} from raw where sheet_row <= {k} order by sheet_row"
    ).fetchall()
    attrs = []
    for i, col in enumerate(rest):
        values: dict[str, str | None] = {}
        for r in range(k):
            cell = header[r][i].strip() or None if header[r][i] is not None else None
            field = LABELS[labels[r]]
            values[field] = " | ".join(v for v in (values.get(field), cell) if v) or None
        attrs.append((col, *[values.get(c) for c in FIELDS]))
    con.execute("drop table if exists head")
    con.execute(f"create temp table head (sheet_column varchar, {', '.join(f'{c} varchar' for c in FIELDS)})")
    if attrs:
        con.executemany(f"insert into head values ({', '.join(['?'] * (len(FIELDS) + 1))})", attrs)
    con.execute("drop table if exists cells")
    if rest:
        con.execute(
            f"""create temp table cells as
            select sheet_row, "{first}" as period, sheet_column, value
            from (unpivot (select * from raw where sheet_row > {k}) on {", ".join(f'"{c}"' for c in rest)}
                  into name sheet_column value value)"""
        )
    else:
        con.execute("create temp table cells (sheet_row bigint, period varchar, sheet_column varchar, value varchar)")
    orphan = con.execute(
        "select count(*), min(c.sheet_column) from cells as c join head as h using (sheet_column) "
        "where h.station_code is null"
    ).fetchone()
    if orphan[0]:
        raise ArchiveDecodeError(f"{name}: {orphan[0]} values in a column without a station code ({orphan[1]})")
    bad = con.execute("select count(*), min(period) from cells where try_cast(period as double) is null").fetchone()
    if bad[0]:
        raise ArchiveDecodeError(f"{name}: {bad[0]} values in rows whose period is not a number ({bad[1]!r})")
    drift = con.execute(
        "select count(*), min(period) from cells "
        "where abs(cast(period as double) * 24 - round(cast(period as double) * 24)) > 1.0 / 12"
    ).fetchone()
    if drift[0]:
        raise ArchiveDecodeError(f"{name}: {drift[0]} values at a period off the whole hour ({drift[1]!r})")
    con.execute(
        f"""copy (
            select ? as entity, ? as member, ? as sheet,
                {", ".join(f"h.{c}" for c in FIELDS)},
                c.period, c.value,
                ? as raw_sha256, ? as source_version, cast(? as timestamptz) as ingested_at,
                c.sheet_row, c.sheet_column
            from cells as c join head as h using (sheet_column)
            order by c.sheet_row, c.sheet_column
        ) to '{output}' (format parquet, compression zstd)""",
        [prov["entity"], name, sheet, prov["raw_sha256"], prov["source_version"], prov["ingested_at"]],
    )
    rows = con.execute(f"select count(*) from read_parquet('{output}')").fetchone()[0]
    return {"rows": rows, "layout": [lab or "(no label)" for lab in labels], "sheet": sheet, "other_sheets": others}


def decode(body: bytes, receipt: dict[str, Any], output: Path, memory_limit: str = "4GB") -> dict[str, Any]:
    """One yearly ZIP → one Bronze Parquet file (every member's value cells); returns rows and a member report."""
    try:
        archive = zipfile.ZipFile(io.BytesIO(body))
    except zipfile.BadZipFile as exc:
        raise ArchiveDecodeError(f"not a ZIP: {exc}") from exc
    prov = bulk_bronze.provenance(receipt)
    names = [n for n in archive.namelist() if not n.endswith("/")]
    report: dict[str, Any] = {"members": len(names), "loaded": {}, "skipped": {}, "other_sheets": {}, "layouts": {}}
    with tempfile.TemporaryDirectory(prefix="zohelo-gios-archive-") as temporary:
        work = Path(temporary)
        con = duckdb.connect()
        try:
            con.execute(f"set memory_limit = '{memory_limit}'; set preserve_insertion_order = true")
            con.execute(f"set temp_directory = '{work}'")
            con.execute("install excel; load excel")
            parts = []
            for name in sorted(names):
                base = name.rsplit("/", 1)[-1]
                skip = next((why for pattern, why in SKIPPED.items() if pattern.fullmatch(base)), None)
                if skip:
                    report["skipped"][base] = skip
                    continue
                if not MEMBER.fullmatch(base):
                    raise ArchiveDecodeError(f"unknown member {name!r}")
                part = work / f"part-{len(parts):03d}.parquet"
                result = member(con, archive.read(name), base, prov, work, part)
                parts.append(part)
                report["loaded"][base] = result["rows"]
                report["layouts"].setdefault(" | ".join(result["layout"]), []).append(base)
                if result["other_sheets"]:
                    report["other_sheets"][base] = result["other_sheets"]
            if not parts:
                raise ArchiveDecodeError("no member loaded")
            listed = ", ".join(f"'{p}'" for p in parts)
            con.execute(
                f"copy (select * from read_parquet([{listed}])) to '{output}' (format parquet, compression zstd)"
            )
            report["rows"] = con.execute(f"select count(*) from read_parquet('{output}')").fetchone()[0]
        except duckdb.Error as exc:
            raise ArchiveDecodeError(f"DuckDB: {str(exc)[:300]}") from exc
        finally:
            con.close()
    if report["rows"] != sum(report["loaded"].values()):
        raise ArchiveDecodeError(f"{report['rows']} rows written, members hold {sum(report['loaded'].values())}")
    return report
