#!/usr/bin/env python3
"""WIPO IP Statistics bronze.load: newest Landing copy of each ZIP → bronze.wipo_ip_stats_counts.

Each Landing file is one ZIP of the IP statistics page (ingestion/sources/wipo_ip_stats.yaml). Its CSV members are
counts as ``year,office,origin,<tec_id|class_id>,count``: one Bronze row per CSV row, every value as source text, with
the member name, the indicator (the member name without ``dc_indicator_`` and ``.csv``) and the name of the class
column. The patent ZIP also holds ``patent_technology_field_names.xlsx``: one row per technology field of its sheet
``technology_fields`` (found by name), with ``class_id`` and the English name ``field_en`` in ``class_label`` and the
count columns null. DuckDB reads the CSVs (no per-cell Python); ``source_row_number`` is the line (CSV) or row
(workbook) number.

Rejected (bronze.wipo_ip_stats_rejected_files), never guessed: a file that is not a ZIP, a member set other than the
entity's MEMBERS (a member missing or unknown), a CSV header other than the declared one, a CSV without rows, or a
names workbook without the sheet, its header ``class_id``/``field_en``, a row without both cells or a repeated id.
"""

from __future__ import annotations

import io
from pathlib import Path
import tempfile
from typing import Any
import zipfile

import duckdb
import pyarrow as pa

from ingestion.common import bulk_bronze, xlsx_cells

SOURCE = "wipo_ip_stats"
TABLE = "wipo_ip_stats_counts"
REJECTED = "wipo_ip_stats_rejected_files"
# The members of each entity: CSV → its class column; the names workbook of the patent ZIP → None.
MEMBERS: dict[str, dict[str, str | None]] = {
    "patent_indicators": {
        "dc_indicator_patent_4_publication_by_technology.csv": "tec_id",
        "dc_indicator_patent_5_grant_by_technology.csv": "tec_id",
        "patent_technology_field_names.xlsx": None,
    },
    "design_indicators": {
        "dc_indicator_design_3_application_by_class.csv": "class_id",
        "dc_indicator_design_4_registration_by_class.csv": "class_id",
    },
}
LABEL_SHEET = "technology_fields"
VALUES = ["member", "indicator", "year", "office", "origin", "class_column", "class_id", "class_label", "count"]
COLUMNS = [
    ("entity", "VARCHAR"),
    *[(c, "VARCHAR") for c in VALUES],
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def csv_rows(con, path: Path, member: str, column: str, prov: dict[str, Any]) -> pa.Table:
    with path.open(encoding="utf-8", errors="strict") as f:
        header = f.readline().rstrip("\r\n")
    if header != f"year,office,origin,{column},count":
        raise ValueError(f"{member}: unknown header {header[:120]!r}")
    table = con.sql(
        f"""
        select
            ? as entity, ? as member, ? as indicator, year, office, origin, ? as class_column, {column} as class_id,
            cast(null as varchar) as class_label, count, ? as raw_sha256, ? as source_version,
            cast(? as timestamptz) as ingested_at, cast(row_number() over () + 1 as bigint) as source_row_number
        from read_csv(?, header = true, all_varchar = true, delim = ',', quote = '"', escape = '"',
                      strict_mode = true, null_padding = false, encoding = 'utf-8')
        """,
        params=[
            prov["entity"],
            member,
            member.removeprefix("dc_indicator_").removesuffix(".csv"),
            column,
            prov["raw_sha256"],
            prov["source_version"],
            prov["ingested_at"],
            str(path),
        ],
    ).to_arrow_table()
    if not table.num_rows:
        raise ValueError(f"{member}: no rows")
    return table.cast(ARROW)


def label_rows(body: bytes, member: str, prov: dict[str, Any]) -> pa.Table:
    grid: dict[int, dict[int, str]] = {}
    for c in xlsx_cells.cells(body):
        if c["sheet_name"] == LABEL_SHEET:
            grid.setdefault(c["row_number"], {})[c["column_number"]] = c["value_text"].strip()
    if not grid or grid.get(1, {}).get(1) != "class_id" or grid[1].get(2) != "field_en":
        raise ValueError(f"{member}: no sheet {LABEL_SHEET!r} with header class_id, field_en")
    rows, seen = [], set()
    for number in sorted(n for n in grid if n > 1):
        code, label = grid[number].get(1), grid[number].get(2)
        if not code or not label or code in seen:
            raise ValueError(f"{member}: row {number} without class_id and field_en, or a repeated class_id {code!r}")
        seen.add(code)
        rows.append(
            {
                **prov,
                "member": member,
                "indicator": "technology_field_names",
                "class_column": "class_id",
                "class_id": code,
                "class_label": label,
                "source_row_number": number,
            }
        )
    return pa.Table.from_pylist(rows, schema=ARROW)


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    expected = MEMBERS[receipt["entity"]]
    try:
        archive = zipfile.ZipFile(io.BytesIO(body))
    except zipfile.BadZipFile as exc:
        raise ValueError(f"not a zip file: {exc}") from exc
    names = archive.namelist()
    if sorted(names) != sorted(expected):
        raise ValueError(f"members {sorted(names)}, expected {sorted(expected)}")
    prov = bulk_bronze.provenance(receipt)
    parts = []
    with tempfile.TemporaryDirectory(prefix="zohelo-wipo-") as temporary:
        con = duckdb.connect()
        con.execute("set preserve_insertion_order = true")
        for member, column in expected.items():
            if column is None:
                parts.append(label_rows(archive.read(member), member, prov))
                continue
            path = Path(temporary) / member
            with archive.open(member) as source, path.open("wb") as target:
                while chunk := source.read(1 << 22):
                    target.write(chunk)
            parts.append(csv_rows(con, path, member, column, prov))
            path.unlink()
        con.close()
    return pa.concat_tables(parts)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: two ZIPs of 4 MB and 1.5 MB."""
    del budget_minutes
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE),
        (namespace, REJECTED),
        COLUMNS,
        decode,
        "counts",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
    )
