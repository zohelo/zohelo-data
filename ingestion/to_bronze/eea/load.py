#!/usr/bin/env python3
"""EEA bronze.load: newest Landing copy of the greenhouse gas inventory (GovReg) CSV ZIP → bronze.eea_ghg_inventory.

The ZIP (``UNFCCC_<n>_csv.zip``, ingestion/to_landing/eea/) holds one CSV, ``UNFCCC_<n>_csv/UNFCCC_v<n>.csv``, with
the columns Country_code, Country, Format_name, Pollutant_name, Sector_code, Sector_name, Parent_sector_code, Unit,
Year, emissions, Notation, PublicationDate, DataSource, Country_code_3: one row per country × sector × pollutant ×
year as reported to the UNFCCC and under the EU Governance Regulation. One Bronze row per CSV row, every value as
source text, column names lower-cased. DuckDB reads the CSV (no per-cell Python). A ZIP with another number of CSV
members, or a header other than these columns, is rejected (bronze.eea_rejected_files). Code shared with other
small bulk sources: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

import io
from pathlib import Path
import tempfile
from typing import Any
import zipfile

import duckdb
import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "eea"
ENTITY = "govreg_ghg_inventory"
TABLE_ID = ("bronze", "eea_ghg_inventory")
REJECTED_ID = ("bronze", "eea_rejected_files")
HEADER = [
    "Country_code",
    "Country",
    "Format_name",
    "Pollutant_name",
    "Sector_code",
    "Sector_name",
    "Parent_sector_code",
    "Unit",
    "Year",
    "emissions",
    "Notation",
    "PublicationDate",
    "DataSource",
    "Country_code_3",
]
COLUMNS = [
    ("entity", "VARCHAR"),
    *[(name.lower(), "VARCHAR") for name in HEADER],
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    try:
        archive = zipfile.ZipFile(io.BytesIO(body))
    except zipfile.BadZipFile as exc:
        raise ValueError(f"not a ZIP: {exc}") from exc
    members = [n for n in archive.namelist() if n.lower().endswith(".csv")]
    if len(members) != 1:
        raise ValueError(f"expected one CSV member, found {members}")
    prov = bulk_bronze.provenance(receipt)
    with tempfile.TemporaryDirectory(prefix="zohelo-eea-") as temporary:
        path = Path(temporary) / "data.csv"
        with archive.open(members[0]) as source, path.open("wb") as target:
            while chunk := source.read(8 << 20):
                target.write(chunk)
        with path.open("rb") as handle:
            first = handle.readline().decode("utf-8-sig").rstrip("\r\n")
        names = [h.strip().strip('"') for h in first.split(",")]
        if names != HEADER:
            raise ValueError(f"unknown GovReg CSV header: {names}")
        con = duckdb.connect()
        con.execute("set preserve_insertion_order = true")
        try:
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
        except duckdb.Error as exc:
            raise ValueError(f"CSV not readable: {exc}") from exc
        finally:
            con.close()
    if not table.num_rows:
        raise ValueError("no rows")
    return table.cast(ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: one ZIP of about 9 MB (100 MB of CSV)."""
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "ghg_inventory",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=[{"entity": ENTITY}],
    )
