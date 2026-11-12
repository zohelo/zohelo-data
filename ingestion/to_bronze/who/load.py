#!/usr/bin/env python3
"""WHO data.who.int bronze.load: newest Landing copy of every declared indicator CSV → bronze.who_observations.

Each file is the "Download" CSV of one data.who.int indicator (``<uid>_ALL_LATEST.csv``) with the columns
IND_ID, IND_CODE, IND_UUID, IND_PER_CODE, DIM_TIME, DIM_TIME_TYPE, DIM_GEO_CODE_M49, DIM_GEO_CODE_TYPE,
DIM_PUBLISH_STATE_CODE, IND_NAME, GEO_NAME_SHORT, DIM_SEX, AMOUNT_N, AMOUNT_NL, AMOUNT_NU. One row per published
row, every value as source text, column names lower-cased. DuckDB reads the CSV (no per-cell Python). A file whose
header differs from these columns is rejected (bronze.who_rejected_files): an indicator with other dimensions needs
its own columns first. Code shared with other small bulk sources: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

from pathlib import Path
import tempfile
from typing import Any

import duckdb
import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "who"
TABLE_ID = ("bronze", "who_observations")
REJECTED_ID = ("bronze", "who_rejected_files")
HEADER = [
    "IND_ID",
    "IND_CODE",
    "IND_UUID",
    "IND_PER_CODE",
    "DIM_TIME",
    "DIM_TIME_TYPE",
    "DIM_GEO_CODE_M49",
    "DIM_GEO_CODE_TYPE",
    "DIM_PUBLISH_STATE_CODE",
    "IND_NAME",
    "GEO_NAME_SHORT",
    "DIM_SEX",
    "AMOUNT_N",
    "AMOUNT_NL",
    "AMOUNT_NU",
]
COLUMNS = [
    ("entity", "VARCHAR"),
    *[(name.lower(), "VARCHAR") for name in HEADER],
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    text = body.decode("utf-8-sig")  # UnicodeDecodeError rejects the file
    names = [h.strip().strip('"') for h in text.split("\n", 1)[0].rstrip("\r").split(",")]
    if names != HEADER:
        raise ValueError(f"unknown data.who.int header: {names}")
    prov = bulk_bronze.provenance(receipt)
    with tempfile.TemporaryDirectory(prefix="zohelo-who-") as temporary:
        path = Path(temporary) / "data.csv"
        path.write_text(text, encoding="utf-8")
        con = duckdb.connect()
        con.execute("set preserve_insertion_order = true")
        table = con.sql(
            f"""
            select
                ? as entity,
                {", ".join(f'"{h}" as {h.lower()}' for h in HEADER)},
                ? as raw_sha256,
                ? as source_version,
                cast(? as timestamptz) as ingested_at,
                cast(row_number() over () + 1 as bigint) as source_row_number
            from read_csv(?, header = true, all_varchar = true, quote = '"', escape = '"', delim = ',',
                          strict_mode = true, null_padding = false)
            """,
            params=[prov["entity"], prov["raw_sha256"], prov["source_version"], prov["ingested_at"], str(path)],
        ).to_arrow_table()
        con.close()
    if not table.num_rows:
        raise ValueError("no observations")
    return table.cast(ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: two CSV files of about 2 MB."""
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "observations",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
    )
