#!/usr/bin/env python3
"""GUS exam results bronze.load: newest Landing copy of the eighth-grade CSV → bronze.gus_exam_results_means.

The file (``srednie_wyniki_uczniow_na_egzaminie_osmoklasisty.csv``, GUS republication of the OKE results; see the
source declaration) is UTF-8 with a byte order mark, ';' separated, CRLF, one header line with the HEADER below and one
row per variable x area x subject x sex x year. One Bronze row per CSV row, every value as published text: the value
(``wartosc``) and the GUS sign (``flaga``: '(.)' no or unreliable data, '(–)' did not occur, both sent with value
'0') stay side by side, so Silver decides; nothing is turned into a number here. DuckDB reads the CSV (no per-cell
Python, strict: a row of another width fails). Another header, text that is not UTF-8, an unreadable CSV or a file
without rows is rejected (bronze.gus_exam_results_rejected_files), never guessed. Code shared with other small bulk
sources: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

import csv
from pathlib import Path
import tempfile
from typing import Any

import duckdb
import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "gus_exam_results"
TABLE_ID = ("bronze", "gus_exam_results_means")
REJECTED_ID = ("bronze", "gus_exam_results_rejected_files")
HEADER: list[tuple[str, str]] = [  # (CSV header as published, Bronze column)
    ("nazwa_zmiennej", "variable_name"),
    ("kraj", "country"),
    ("wojewodztwo", "area"),
    ("przedmiot", "subject"),
    ("plec", "sex"),
    ("typ_informacji_z_jednostka_miary", "unit_label"),
    ("rok", "period"),
    ("wartosc", "value"),
    ("flaga", "flag"),
]
COLUMNS = [
    ("entity", "VARCHAR"),
    *[(column, "VARCHAR") for _, column in HEADER],
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    body = body.removeprefix(b"\xef\xbb\xbf")
    try:
        body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"not UTF-8: {exc}") from exc
    first = body.split(b"\n", 1)[0].decode("utf-8").rstrip("\r")
    names = next(csv.reader([first], delimiter=";", quotechar='"'))
    if names != [name for name, _ in HEADER]:
        raise ValueError(f"unknown exam results header: {names}")
    prov = bulk_bronze.provenance(receipt)
    kept = ", ".join(f"c{i} as {column}" for i, (_, column) in enumerate(HEADER))
    types = ", ".join(f"'c{i}': 'VARCHAR'" for i in range(len(HEADER)))
    with tempfile.TemporaryDirectory(prefix="zohelo-gus-exam-") as temporary:
        path = Path(temporary) / "means.csv"
        path.write_bytes(body)
        con = duckdb.connect()
        con.execute("set preserve_insertion_order = true")
        con.execute("set threads = 1")
        try:
            table = con.sql(
                f"""
                select
                    ? as entity,
                    {kept},
                    ? as raw_sha256,
                    ? as source_version,
                    cast(? as timestamptz) as ingested_at,
                    cast(row_number() over () as bigint) as source_row_number
                from read_csv(?, header = true, all_varchar = true, quote = '"', escape = '"', delim = ';',
                              strict_mode = true, null_padding = false, auto_detect = false,
                              columns = {{{types}}})
                """,
                params=[prov["entity"], prov["raw_sha256"], prov["source_version"], prov["ingested_at"], str(path)],
            ).to_arrow_table()
        except duckdb.Error as exc:
            raise ValueError(f"CSV not readable: {str(exc).splitlines()[0][:300]}") from exc
        finally:
            con.close()
    if not table.num_rows:
        raise ValueError("no rows")
    return table.cast(ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: one CSV of about 340 kB."""
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "means",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
    )
