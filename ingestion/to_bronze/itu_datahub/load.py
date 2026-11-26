#!/usr/bin/env python3
"""ITU DataHub bronze.load: newest Landing copy of the Data360 file ITU_DH.csv → bronze.itu_datahub_observations.

The file (World Bank Data360, dataset ITU_DH; register GL-031) is SDMX-CSV with labels: the 41 columns of HEADER, one
row per observation (indicator × economy × sex × age × urbanisation × three indicator-specific breakdowns × unit ×
year). Bronze keeps every row and every column as published text, the column names in lower case. DuckDB reads the
CSV (no per-cell Python). A file whose header is not exactly HEADER, with a row of another width, that is not UTF-8,
or that holds no row is rejected (bronze.itu_datahub_rejected_files). Code shared with other small bulk sources:
ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

import csv
import io
from pathlib import Path
import tempfile
from typing import Any

import duckdb
import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "itu_datahub"
TABLE_ID = ("bronze", "itu_datahub_observations")
REJECTED_ID = ("bronze", "itu_datahub_rejected_files")
CODES = [
    "STRUCTURE", "STRUCTURE_ID", "ACTION", "FREQ", "REF_AREA", "INDICATOR", "SEX", "AGE", "URBANISATION",
    "UNIT_MEASURE", "COMP_BREAKDOWN_1", "COMP_BREAKDOWN_2", "COMP_BREAKDOWN_3", "TIME_PERIOD", "OBS_VALUE",
    "AGG_METHOD", "UNIT_TYPE", "DECIMALS", "DATABASE_ID", "TIME_FORMAT", "UNIT_MULT", "OBS_STATUS", "OBS_CONF",
]  # fmt: skip
LABELS = [
    "FREQ", "REF_AREA", "INDICATOR", "SEX", "AGE", "URBANISATION", "UNIT_MEASURE", "COMP_BREAKDOWN_1",
    "COMP_BREAKDOWN_2", "COMP_BREAKDOWN_3", "AGG_METHOD", "UNIT_TYPE", "DECIMALS", "DATABASE_ID", "TIME_FORMAT",
    "UNIT_MULT", "OBS_STATUS", "OBS_CONF",
]  # fmt: skip
HEADER = CODES + [f"{c}_LABEL" for c in LABELS]
COLUMNS = [
    ("entity", "VARCHAR"),
    *[(name.lower(), "VARCHAR") for name in HEADER],
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),  # position of the row among the file's data rows, from 1
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    text = body.decode("utf-8-sig")  # UnicodeDecodeError rejects the file
    header = next(csv.reader(io.StringIO(text[:20000], newline="")), None)
    if header != HEADER:
        raise ValueError(f"not the Data360 SDMX-CSV header: {header[:6] if header else header}… ({len(header or [])})")
    prov = bulk_bronze.provenance(receipt)
    with tempfile.TemporaryDirectory(prefix="zohelo-itu-") as temporary:
        path = Path(temporary) / "data.csv"
        path.write_text(text, encoding="utf-8")
        con = duckdb.connect()
        try:
            con.execute("set threads = 1")  # file order for source_row_number
            names = ", ".join(f"'{h}'" for h in HEADER)
            table = con.sql(
                f"""
                select
                    ? as entity,
                    {", ".join(f'"{h}" as {h.lower()}' for h in HEADER)},
                    ? as raw_sha256,
                    ? as source_version,
                    cast(? as timestamptz) as ingested_at,
                    cast(row_number() over () as bigint) as source_row_number
                from read_csv(?, header = true, delim = ',', quote = '"', escape = '"', all_varchar = true,
                              strict_mode = true, null_padding = false, names = [{names}])
                """,
                params=[prov["entity"], prov["raw_sha256"], prov["source_version"], prov["ingested_at"], str(path)],
            ).to_arrow_table()
        except duckdb.Error as exc:
            raise ValueError(f"not a readable CSV: {str(exc)[:300]}") from exc
        finally:
            con.close()
    if not table.num_rows:
        raise ValueError("no rows")
    return table.cast(ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: one CSV of about 66 MB."""
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
