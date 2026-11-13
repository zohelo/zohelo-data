#!/usr/bin/env python3
"""ESMA registers bronze.load: newest Landing copy of the Polish slice of esma_registers_upreg →
bronze.esma_registers_entries.

The file is one Solr JSON answer (``{"response": {"numFound", "start", "docs": [...]}}``, response header omitted)
of the fixed request in ingestion/sources/esma_registers.yaml: every parent entry whose home member state is Poland,
with the fields named in its ``fl``. One row per entry (doc), every field as the text the answer holds, in a column
named after the field in snake case (``ae_entityTypeCode`` → ``ae_entity_type_code``; numbers as
JSON prints them, the multi-valued ``ae_hostMemberStates`` as its JSON array text; a field the entry does not carry
is null). DuckDB reads the JSON (no per-cell Python). Rejected (bronze.esma_registers_rejected_files), never
guessed: an answer that is not such an object, holds no entries, or does not hold the whole slice in one page
(``start`` other than 0 or ``numFound`` larger than the entries); an entry with a field outside the declared list,
an array where one value is expected, or without id, name, type, status, home member state or ``type_s``; an entry
outside the selection (not ``parent``, home member state not POLAND). Shared code: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

from pathlib import Path
import re
import tempfile
from typing import Any
from urllib.parse import parse_qs, urlsplit

import duckdb
import pyarrow as pa

from ingestion.common import bulk_bronze
from ingestion.to_landing.patterns import bulk_file

SOURCE = "esma_registers"
TABLE_ID = ("bronze", "esma_registers_entries")
REJECTED_ID = ("bronze", "esma_registers_rejected_files")
MULTI = {"ae_hostMemberStates"}  # multi-valued Solr fields, kept as JSON array text
REQUIRED = ["id", "type_s", "ae_entityName", "ae_entityTypeCode", "ae_status", "ae_homeMemberState"]


def declared_fields() -> list[str]:
    """The ``fl`` list of the declared request, in order (the Bronze columns follow it)."""
    url = bulk_file.declaration(SOURCE)[0]["url"]
    return parse_qs(urlsplit(url).query)["fl"][0].split(",")


def column(field: str) -> str:
    """The Bronze column of a Solr field: snake case (ae_entityTypeCode → ae_entity_type_code)."""
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", field).lower()


FIELDS = declared_fields()
COLUMNS = [
    ("entity", "VARCHAR"),
    *[(column(name), "VARCHAR") for name in FIELDS],
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def _check(con: duckdb.DuckDBPyConnection, path: str) -> None:
    head = con.execute(
        """
        select json_type(j -> '$.response.docs'), j ->> '$.response.numFound', j ->> '$.response.start',
            json_array_length(j -> '$.response.docs')
        from read_json_objects(?, maximum_object_size = 268435456) as t(j)
        """,
        [path],
    ).fetchall()
    if len(head) != 1 or head[0][0] != "ARRAY":
        raise ValueError(f"not a Solr select answer: {head[:1]}")
    _, found, start, docs = head[0]
    if not docs:
        raise ValueError("no entries")
    if start != "0" or found is None or int(found) != docs:
        raise ValueError(f"slice not in one page: start {start}, numFound {found}, entries {docs}")
    keys = con.execute(
        """
        select k, list(distinct json_type(d -> ('$."' || k || '"'))), count(*)
        from read_json_objects(?, maximum_object_size = 268435456) as t(j),
            unnest(cast(j -> '$.response.docs' as json[])) as u(d),
            unnest(json_keys(d)) as v(k)
        group by k
        """,
        [path],
    ).fetchall()
    unknown = sorted(k for k, _, _ in keys if k not in FIELDS)
    if unknown:
        raise ValueError(f"entries with fields outside the declared list: {unknown}")
    arrays = sorted(k for k, types, _ in keys if k not in MULTI and ({"ARRAY", "OBJECT"} & set(types)))
    if arrays:
        raise ValueError(f"several values where one is expected: {arrays}")
    present = {k: n for k, _, n in keys}
    missing = [k for k in REQUIRED if present.get(k, 0) != docs]
    if missing:
        raise ValueError(f"entries without {missing}")
    outside = con.execute(
        """
        select count(*) filter (where d ->> '$.type_s' <> 'parent'),
            count(*) filter (where d ->> '$.ae_homeMemberState' <> 'POLAND')
        from read_json_objects(?, maximum_object_size = 268435456) as t(j),
            unnest(cast(j -> '$.response.docs' as json[])) as u(d)
        """,
        [path],
    ).fetchall()[0]
    if any(outside):
        raise ValueError(
            f"entries outside the selection: not parent {outside[0]}, home member state not POLAND {outside[1]}"
        )


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    body.decode("utf-8")  # UnicodeDecodeError rejects the file
    prov = bulk_bronze.provenance(receipt)
    with tempfile.TemporaryDirectory(prefix="zohelo-esma-") as temporary:
        path = Path(temporary) / "data.json"
        path.write_bytes(body)
        con = duckdb.connect()
        try:
            _check(con, str(path))
            values = ", ".join(
                f"""cast(d -> '$."{f}"' as varchar) as {column(f)}"""
                if f in MULTI
                else f"""d ->> '$."{f}"' as {column(f)}"""
                for f in FIELDS
            )
            table = con.sql(
                f"""
                select
                    ? as entity,
                    {values},
                    ? as raw_sha256,
                    ? as source_version,
                    cast(? as timestamptz) as ingested_at,
                    cast(n as bigint) as source_row_number
                from read_json_objects(?, maximum_object_size = 268435456) as t(j),
                    unnest(cast(j -> '$.response.docs' as json[])) with ordinality as u(d, n)
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
    """budget_minutes is unused: one JSON answer of a few hundred kB."""
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "entries",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
    )
