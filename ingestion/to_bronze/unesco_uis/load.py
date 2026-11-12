#!/usr/bin/env python3
"""UNESCO UIS bronze.load: newest Landing copy of every declared Data API selection → bronze.unesco_uis_observations.

Each file is the JSON response of ``/api/public/data/indicators?indicator=<code>[&indicator=...]``:
``{"hints": [...], "records": [{"indicatorId", "geoUnit", "year", "value", "magnitude", "qualifier"}, ...]}``.
One row per record, every value as text (numbers as JSON prints them). DuckDB reads the JSON (no per-cell
Python). A file that is not such an object, has other record fields, or no records is rejected
(bronze.unesco_uis_rejected_files). Code shared with other small bulk sources: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

from pathlib import Path
import tempfile
from typing import Any

import duckdb
import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "unesco_uis"
TABLE_ID = ("bronze", "unesco_uis_observations")
REJECTED_ID = ("bronze", "unesco_uis_rejected_files")
FIELDS = {"indicatorId": "indicator_id", "geoUnit": "geo_unit", "year": "year", "value": "value",
          "magnitude": "magnitude", "qualifier": "qualifier"}
COLUMNS = [
    ("entity", "VARCHAR"),
    *[(name, "VARCHAR") for name in FIELDS.values()],
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    body.decode("utf-8")  # UnicodeDecodeError rejects the file
    prov = bulk_bronze.provenance(receipt)
    with tempfile.TemporaryDirectory(prefix="zohelo-uis-") as temporary:
        path = Path(temporary) / "data.json"
        path.write_bytes(body)
        con = duckdb.connect()
        try:
            keys = con.execute(
                "select json_keys(j), json_array_length(j -> '$.records') from read_json_objects(?) as t(j)",
                [str(path)],
            ).fetchall()
            if len(keys) != 1 or "records" not in keys[0][0]:
                raise ValueError(f"not a UIS data response: {keys[:1]}")
            if not keys[0][1]:
                raise ValueError("no records")
            fields = con.execute(
                "select distinct unnest(json_keys(r)) from read_json_objects(?) as t(j), "
                "unnest(cast(j -> '$.records' as json[])) as u(r)",
                [str(path)],
            ).fetchall()
            unknown = sorted({f[0] for f in fields} - set(FIELDS))
            if unknown:
                raise ValueError(f"unknown record fields: {unknown}")
            table = con.sql(
                f"""
                select
                    ? as entity,
                    {", ".join(f"r ->> '$.{k}' as {v}" for k, v in FIELDS.items())},
                    ? as raw_sha256,
                    ? as source_version,
                    cast(? as timestamptz) as ingested_at,
                    cast(n as bigint) as source_row_number
                from read_json_objects(?) as t(j),
                    unnest(cast(j -> '$.records' as json[])) with ordinality as u(r, n)
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
    """budget_minutes is unused: two JSON responses of a few hundred KB."""
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
