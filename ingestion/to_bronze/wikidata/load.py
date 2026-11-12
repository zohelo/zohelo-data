#!/usr/bin/env python3
"""Wikidata bronze.load: newest Landing copy of every SPARQL selection → bronze.wikidata_bindings.

Each file is a SPARQL 1.1 JSON result (``{"head": {"vars": [...]}, "results": {"bindings": [...]}}``) of one
query in ingestion/to_landing/wikidata/queries.rq. One row per binding (solution), every variable's ``value`` as
source text (full entity, statement and rank URIs as published; variables a solution does not bind are null).
DuckDB reads the JSON (no per-cell Python). A file whose variables differ from its query's SELECT list, or that
has no bindings, is rejected (bronze.wikidata_rejected_files). Shared code: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

from pathlib import Path
import re
import tempfile
from typing import Any

import duckdb
import pyarrow as pa

from ingestion.common import bulk_bronze
from ingestion.to_landing import wikidata as landing

SOURCE = "wikidata"
TABLE_ID = ("bronze", "wikidata_bindings")
REJECTED_ID = ("bronze", "wikidata_rejected_files")
VARIABLES = ["item", "statement", "rank", "value", "unit", "time", "end_time", "lang", "dissolved",
             "label_pl", "label_en", "article_pl", "article_en"]  # fmt: skip
COLUMNS = [
    ("entity", "VARCHAR"),
    *[(name, "VARCHAR") for name in VARIABLES],
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def expected_variables(entity: str) -> list[str]:
    """The SELECT list of the entity's query, in order."""
    query = landing.queries()[entity]
    return re.findall(r"\?(\w+)", re.search(r"SELECT (?:DISTINCT )?(.*?) WHERE", query, re.S).group(1))


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    body.decode("utf-8")  # UnicodeDecodeError rejects the file
    prov = bulk_bronze.provenance(receipt)
    expected = expected_variables(prov["entity"])
    if set(expected) - set(VARIABLES):
        raise ValueError(f"query variables without a Bronze column: {sorted(set(expected) - set(VARIABLES))}")
    with tempfile.TemporaryDirectory(prefix="zohelo-wikidata-") as temporary:
        path = Path(temporary) / "data.json"
        path.write_bytes(body)
        con = duckdb.connect()
        try:
            head = con.execute(
                "select cast(j -> '$.head.vars' as varchar[]), json_array_length(j -> '$.results.bindings') "
                "from read_json_objects(?, maximum_object_size = 268435456) as t(j)",
                [str(path)],
            ).fetchall()
            if len(head) != 1 or head[0][0] != expected:
                raise ValueError(f"unknown SPARQL result layout: vars {head[0][0] if head else None} != {expected}")
            if not head[0][1]:
                raise ValueError("no bindings")
            table = con.sql(
                f"""
                select
                    ? as entity,
                    {", ".join(f"b ->> '$.{v}.value' as {v}" if v in expected else f"null as {v}" for v in VARIABLES)},
                    ? as raw_sha256,
                    ? as source_version,
                    cast(? as timestamptz) as ingested_at,
                    cast(n as bigint) as source_row_number
                from read_json_objects(?, maximum_object_size = 268435456) as t(j),
                    unnest(cast(j -> '$.results.bindings' as json[])) with ordinality as u(b, n)
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
    """budget_minutes is unused: six JSON answers of 1-2.5 MB."""
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "bindings",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=landing.specs(),
    )
