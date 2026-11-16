#!/usr/bin/env python3
"""KPRM work list bronze.load: newest Landing copy of the work-list CSV → bronze.kprm_work_list_drafts.

The file (``Rejestr_<n>.csv`` linked from https://www.gov.pl/web/premier/wplip-rm, ingestion/sources/kprm_work_list.yaml)
is UTF-8 with a byte order mark, ';' separated, '"' quoted (long text cells span lines), one header line with the
HEADER below and one row per draft of the work list of the Council of Ministers. One Bronze row per CSV row, every
value as published text. Not loaded (``None`` in HEADER): the two long text columns (aims and substance of the draft,
up to 90,000 characters) and the responsible official's name (Osoba odpowiedzialna; register PLG-034: no personal
data); they stay only in the landed bytes. DuckDB reads the CSV (no per-cell Python, strict: a row of another width
fails). Another header, an unreadable CSV or a file without rows is rejected (bronze.kprm_work_list_rejected_files).
Code shared with other small bulk sources: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

import csv
from pathlib import Path
import tempfile
from typing import Any

import duckdb
import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "kprm_work_list"
TABLE_ID = ("bronze", "kprm_work_list_drafts")
REJECTED_ID = ("bronze", "kprm_work_list_rejected_files")
HEADER: list[tuple[str, str | None]] = [  # (CSV header as published, Bronze column or None = not loaded)
    ("Numer projektu", "draft_number"),
    ("Tytuł", "title"),
    ("Rodzaj dokumentu", "document_kind"),
    ("Typ dokumentu", "document_type"),
    ("Informacja dodatkowa", "additional_info"),
    ("Istota rozwiązań planowanych w projekcie", None),
    ("Cele projektu", None),
    ("EMPTY1", "empty_1"),
    ("EMPTY2", "empty_2"),
    ("EMPTY3", "empty_3"),
    ("Organ odpowiedzialny za opracowanie projektu", "drafting_body"),
    ("Organ współpracujący", "cooperating_body"),
    ("Osoba odpowiedzialna", None),
    ("Organ odpowiedzialny", "responsible_body"),
    ("Planowane przyjęcie przez RM", "planned_adoption"),
    ("Informacja o rezygnacji", "withdrawal_info"),
    ("Status realizacji", "status_text"),
    ("Data publikacji", "published_at"),
    ("Podgląd", "preview_url"),
]
COLUMNS = [
    ("entity", "VARCHAR"),
    *[(column, "VARCHAR") for _, column in HEADER if column],
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    if not body.startswith(b"\xef\xbb\xbf"):
        raise ValueError("not UTF-8 with a byte order mark")
    first = body[3:].split(b"\n", 1)[0].decode("utf-8").rstrip("\r")
    names = next(csv.reader([first], delimiter=";", quotechar='"'))
    if names != [name for name, _ in HEADER]:
        raise ValueError(f"unknown work-list header: {names}")
    prov = bulk_bronze.provenance(receipt)
    positional = [f"c{i}" for i in range(len(HEADER))]
    kept = ", ".join(f"c{i} as {column}" for i, (_, column) in enumerate(HEADER) if column)
    with tempfile.TemporaryDirectory(prefix="zohelo-kprm-") as temporary:
        path = Path(temporary) / "work_list.csv"
        path.write_bytes(body[3:])
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
                from read_csv(?, header = true, all_varchar = true, quote = '"',
                              escape = '"', delim = ';', strict_mode = true, null_padding = false,
                              auto_detect = false, columns = {{{", ".join(f"'{c}': 'VARCHAR'" for c in positional)}}})
                """,
                params=[prov["entity"], prov["raw_sha256"], prov["source_version"], prov["ingested_at"], str(path)],
            ).to_arrow_table()
        except duckdb.Error as exc:
            # first line only: DuckDB quotes the offending row, which may hold an official's name
            raise ValueError(f"CSV not readable: {str(exc).splitlines()[0][:300]}") from exc
        finally:
            con.close()
    if not table.num_rows:
        raise ValueError("no rows")
    return table.cast(ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: one CSV of about 11 MB."""
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "drafts",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
    )
