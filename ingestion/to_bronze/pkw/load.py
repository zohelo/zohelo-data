#!/usr/bin/env python3
"""PKW bronze.load: newest Landing copy of every declared election file → bronze.pkw_cells.

The files are the area-level result files of the election sites (ingestion/sources/pkw.yaml): a ZIP holding one
CSV (``<name>_csv.zip``) or an XLSX workbook (samorzad2024 publishes its voivodeship assembly results only as
XLSX). Both are stored in one shape, the cell shape of the workbook sources (ingestion/common/xlsx_cells.py): one
row per non-empty cell with its member (CSV file name or sheet), row and column number and the source text, so the
wide files (one column per committee or candidate, a different set per election) need no layout of their own in
Bronze; Silver reads the header row of each file. CSV members are read by DuckDB (``read_csv`` with every column as
text, UNPIVOT to cells; no per-cell Python) after a strict UTF-8 check; a member that is not UTF-8, has rows of
different widths, or a ZIP without exactly one CSV member rejects the file (bronze.pkw_rejected_files), never
guessed. Loader: ingestion/common/bulk_bronze.py.
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

SOURCE = "pkw"
TABLE = "pkw_cells"
REJECTED = "pkw_rejected_files"
COLUMNS = xlsx_cells.COLUMNS
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def csv_cells(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    """The one CSV member of a ``_csv.zip`` as cells (sheet_name = member name, sheet_index 1)."""
    try:
        archive = zipfile.ZipFile(io.BytesIO(body))
    except zipfile.BadZipFile as exc:
        raise ValueError(f"not a zip file: {exc}") from exc
    members = [n for n in archive.namelist() if n.lower().endswith(".csv")]
    if len(members) != 1 or len(archive.namelist()) != 1:
        raise ValueError(f"expected one CSV member, found {archive.namelist()}")
    raw = archive.read(members[0])
    try:
        text = raw.decode("utf-8-sig")  # strict: a member in another encoding is rejected, not guessed
    except UnicodeDecodeError as exc:
        raise ValueError(f"CSV member is not UTF-8: {exc}") from exc
    first = text.split("\n", 1)[0]
    if first.count(";") < 2:
        raise ValueError(f"not a semicolon-separated header: {first[:200]!r}")
    prov = bulk_bronze.provenance(receipt)
    with tempfile.TemporaryDirectory(prefix="zohelo-pkw-") as temporary:
        path = Path(temporary) / "member.csv"
        path.write_text(text, encoding="utf-8")
        con = duckdb.connect()
        con.execute("set preserve_insertion_order = true")
        con.execute(
            """
            create table wide as
            select row_number() over () as rn, *
            from read_csv(?, header = false, all_varchar = true, delim = ';', quote = '"', escape = '"',
                          strict_mode = true, null_padding = false, encoding = 'utf-8')
            """,
            [str(path)],
        )
        table = con.sql(
            """
            select
                ? as entity,
                cast(1 as bigint) as sheet_index,
                ? as sheet_name,
                cast(rn as bigint) as row_number,
                cast(regexp_extract(col, '(\\d+)$', 1) as bigint) + 1 as column_number,
                cast(null as varchar) as cell_ref,
                'str' as cell_type,
                value_text,
                cast(null as varchar) as number_format,
                ? as raw_sha256,
                ? as source_version,
                cast(? as timestamptz) as ingested_at,
                cast(row_number() over (order by rn, cast(regexp_extract(col, '(\\d+)$', 1) as bigint)) as bigint)
                    as source_row_number
            from (unpivot wide on columns(* exclude (rn)) into name col value value_text)
            where trim(value_text) <> ''
            order by source_row_number
            """,
            params=[prov["entity"], members[0], prov["raw_sha256"], prov["source_version"], prov["ingested_at"]],
        ).to_arrow_table()
        con.close()
    if not table.num_rows:
        raise ValueError("CSV member without cells")
    return table.cast(ARROW)


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    """A ``.xlsx`` Landing copy through the shared workbook decoder, a ``_csv.zip`` copy through csv_cells."""
    key = receipt["object"]["key"]
    if receipt.get("source_url", "").endswith(".xlsx") or key.endswith(".xlsx"):
        return xlsx_cells.decode(body, receipt)
    return csv_cells(body, receipt)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: about twenty files below 1 MB each."""
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE),
        (namespace, REJECTED),
        COLUMNS,
        decode,
        "cells",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
    )
