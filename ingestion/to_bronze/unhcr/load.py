#!/usr/bin/env python3
"""UNHCR bronze.load: newest Landing copy of every declared API selection → bronze.unhcr_population.

Each file is one JSON response of the Refugee Statistics API ``/population/v1/population/`` for one fixed selection
(ingestion/sources/unhcr.yaml): ``{"page", "maxPages", "total", "items": [{"year", "coo_id", "coo_name", "coo",
"coo_iso", "coa_id", "coa_name", "coa", "coa_iso", "refugees", ...}, ...]}``. One row per item, every value as the
text the API sends (numbers as JSON prints them, "-" kept). The selection must arrive in one page: a response with
``maxPages`` other than 1 is rejected, so a truncated selection never loads. An item with other fields, a response
that is not such an object, or one without items is rejected (bronze.unhcr_rejected_files). DuckDB reads the
JSON (no per-cell Python). Code shared with other small bulk sources: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

from pathlib import Path
import tempfile
from typing import Any

import duckdb
import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "unhcr"
TABLE_ID = ("bronze", "unhcr_population")
REJECTED_ID = ("bronze", "unhcr_rejected_files")
FIELDS = [
    "year",
    "coo_id",
    "coo_name",
    "coo",
    "coo_iso",
    "coa_id",
    "coa_name",
    "coa",
    "coa_iso",
    "refugees",
    "asylum_seekers",
    "returned_refugees",
    "idps",
    "returned_idps",
    "stateless",
    "ooc",
    "oip",
    "hst",
]
COLUMNS = [
    ("entity", "VARCHAR"),
    *[(name, "VARCHAR") for name in FIELDS],
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    body.decode("utf-8")  # UnicodeDecodeError rejects the file
    prov = bulk_bronze.provenance(receipt)
    with tempfile.TemporaryDirectory(prefix="zohelo-unhcr-") as temporary:
        path = Path(temporary) / "data.json"
        path.write_bytes(body)
        con = duckdb.connect()
        try:
            head = con.execute(
                "select json_keys(j), j ->> '$.page', j ->> '$.maxPages', json_array_length(j -> '$.items') "
                "from read_json_objects(?) as t(j)",
                [str(path)],
            ).fetchall()
            if len(head) != 1 or "items" not in head[0][0]:
                raise ValueError(f"not a UNHCR population response: {head[:1]}")
            if head[0][1] != "1" or head[0][2] != "1":
                raise ValueError(f"selection not in one page: page {head[0][1]} of {head[0][2]}")
            if not head[0][3]:
                raise ValueError("no items")
            fields = con.execute(
                "select distinct unnest(json_keys(r)) from read_json_objects(?) as t(j), "
                "unnest(cast(j -> '$.items' as json[])) as u(r)",
                [str(path)],
            ).fetchall()
            unknown = sorted({f[0] for f in fields} - set(FIELDS))
            if unknown:
                raise ValueError(f"unknown item fields: {unknown}")
            table = con.sql(
                f"""
                select
                    ? as entity,
                    {", ".join(f"r ->> '$.{k}' as {k}" for k in FIELDS)},
                    ? as raw_sha256,
                    ? as source_version,
                    cast(? as timestamptz) as ingested_at,
                    cast(n as bigint) as source_row_number
                from read_json_objects(?) as t(j),
                    unnest(cast(j -> '$.items' as json[])) with ordinality as u(r, n)
                order by n
                """,
                params=[prov["entity"], prov["raw_sha256"], prov["source_version"], prov["ingested_at"], str(path)],
            ).to_arrow_table()
        except duckdb.Error as exc:
            raise ValueError(f"not JSON: {exc}") from exc
        finally:
            con.close()
    return table.cast(ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: two JSON responses of about 2 MB and 0.5 MB."""
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "population",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
    )
