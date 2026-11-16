#!/usr/bin/env python3
"""RAD-on bronze.load: newest Landing copy of each resource → bronze.radon_institutions, bronze.radon_courses.

Each Landing file holds one JSON line per record, already reduced to the resource's allowlist
(ingestion/to_landing/radon/extract.py). One Bronze row per line, every value as source text; the nested lists
(``disciplines``, ``courseInstances``) as JSON text. DuckDB reads the lines (no per-cell Python). A file with a
line whose keys differ from the allowlist, without its UUID or repeating one, is rejected
(bronze.radon_rejected_files). Shared code: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

from pathlib import Path
import tempfile
from typing import Any

import duckdb
import pyarrow as pa

from ingestion.common import bulk_bronze
from ingestion.to_landing import radon as landing
from ingestion.to_landing.radon.extract import KEEP

SOURCE = "radon"
TABLES = {"institutions": "radon_institutions", "courses": "radon_courses"}
REJECTED = "radon_rejected_files"


def fields(entity: str) -> list[str]:
    _key, top, nested = KEEP[entity]
    return [*top, *nested]


def columns(entity: str) -> list[tuple[str, str]]:
    return [
        ("entity", "VARCHAR"),
        *[(name, "VARCHAR") for name in fields(entity)],
        *bulk_bronze.PROVENANCE,
        ("source_row_number", "BIGINT"),
    ]


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    body.decode("utf-8")  # UnicodeDecodeError rejects the file
    prov = bulk_bronze.provenance(receipt)
    entity = prov["entity"]
    key, names = KEEP[entity][0], fields(entity)
    with tempfile.TemporaryDirectory(prefix="zohelo-radon-") as temporary:
        path = Path(temporary) / "data.jsonl"
        path.write_bytes(body)
        con = duckdb.connect()
        try:
            con.execute(
                "create table lines as select j, row_number() over () as n "
                "from read_json_objects(?, format = 'newline_delimited', maximum_object_size = 16777216) as t(j)",
                [str(path)],
            ).fetchall()
            total, odd, keyless, repeated = con.execute(
                f"""
                select
                    count(*),
                    count(*) filter (where list_sort(json_keys(j)) <> list_sort(?::varchar[])),
                    count(*) filter (where coalesce(j ->> '$.{key}', '') = ''),
                    count(*) - count(distinct j ->> '$.{key}')
                from lines
                """,
                [names],
            ).fetchall()[0]
            if not total:
                raise ValueError("no records")
            if odd or keyless or repeated:
                raise ValueError(
                    f"unknown layout: {odd} lines with other keys, {keyless} without {key}, {repeated} repeated {key}"
                )
            values = ", ".join(
                f"""case when json_type(j -> '$.{n}') in ('VARCHAR') then j ->> '$.{n}'
                    when json_type(j -> '$.{n}') = 'NULL' then null else cast(j -> '$.{n}' as varchar) end as "{n}" """
                for n in names
            )
            table = con.sql(
                f"""
                select ? as entity, {values}, ? as raw_sha256, ? as source_version,
                    cast(? as timestamptz) as ingested_at, cast(n as bigint) as source_row_number
                from lines order by n
                """,
                params=[entity, prov["raw_sha256"], prov["source_version"], prov["ingested_at"]],
            ).to_arrow_table()
        except duckdb.Error as exc:
            raise ValueError(f"not JSON lines: {exc}") from exc
        finally:
            con.close()
    return table.cast(bulk_bronze.arrow_schema(columns(entity)))


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: two files of a few MB to some tens of MB."""
    status = 0
    for spec in landing.specs():
        status |= bulk_bronze.load(
            SOURCE,
            (namespace, TABLES[spec["entity"]]),
            (namespace, REJECTED),
            columns(spec["entity"]),
            decode,
            "records",
            landing_root=landing_root,
            lake_root="02_bronze" if namespace == "bronze" else namespace,
            specs=[spec],
        )
    return status
