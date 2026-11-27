#!/usr/bin/env python3
"""World Bank Enterprise Surveys bronze.load: newest Landing copy of the Data360 file WB_ES.csv →
bronze.world_bank_es_observations.

The file (World Bank Data360, dataset WB_ES; register GL-010) is SDMX-CSV with labels, the same 41 columns as the
other Data360 files (ITU_DH): one row per observation (indicator × economy × sex × age × urbanisation × the three
breakdowns: sector, firm size, other firm characteristic × unit × survey year). Bronze keeps every row and every column
as published text, the column names in lower case. DuckDB reads the CSV (no per-cell Python). A file whose header is
not exactly HEADER, with a row of another width, that is not UTF-8, or that holds no row is rejected
(bronze.world_bank_es_rejected_files). About 606 MB: the bytes go to a temporary file as they are (no decoded copy in
memory), checked as UTF-8 in slices. Code shared with other small bulk sources: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

import codecs
import csv
import io
from pathlib import Path
import tempfile
from typing import Any

import duckdb
import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "world_bank_es"
TABLE_ID = ("bronze", "world_bank_es_observations")
REJECTED_ID = ("bronze", "world_bank_es_rejected_files")
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
SLICE = 16 << 20


def check_utf8(body: bytes) -> None:
    """Raises UnicodeDecodeError (the file is rejected) without holding a decoded copy of the whole file."""
    decoder = codecs.getincrementaldecoder("utf-8")()
    view = memoryview(body)
    for start in range(0, len(body), SLICE):
        decoder.decode(view[start : start + SLICE], final=start + SLICE >= len(body))


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    check_utf8(body)
    head = body[:20000].decode("utf-8-sig", errors="ignore")
    header = next(csv.reader(io.StringIO(head, newline="")), None)
    if header != HEADER:
        raise ValueError(f"not the Data360 SDMX-CSV header: {header[:6] if header else header}… ({len(header or [])})")
    prov = bulk_bronze.provenance(receipt)
    with tempfile.TemporaryDirectory(prefix="zohelo-wb-es-") as temporary:
        path = Path(temporary) / "data.csv"
        path.write_bytes(body[3:] if body.startswith(codecs.BOM_UTF8) else body)
        con = duckdb.connect()
        try:
            con.execute("set threads = 1")  # file order for source_row_number
            con.execute(f"set temp_directory = '{temporary}'")
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
    """budget_minutes is unused: one CSV of about 606 MB, decoded in one DuckDB pass."""
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
