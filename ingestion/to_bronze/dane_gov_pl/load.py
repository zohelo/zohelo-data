#!/usr/bin/env python3
"""dane.gov.pl catalogue bronze.load: newest Landing copy → bronze.dane_gov_pl_datasets and _distributions.

The Landing file holds one JSON line per dataset record of the data.europa.eu catalogue `dane-gov-pl`, already
reduced to the allowlist of ingestion/to_landing/dane_gov_pl/extract.py. Datasets: one Bronze row per line, every
value as source text, the categories as JSON text. Distributions: one Bronze row per item of a line's
``distributions`` list, with the dataset id. DuckDB reads the lines (no per-cell Python). A file with a line or a
distribution whose keys differ from the allowlist, a line without its id or an id on two lines is rejected
(bronze.dane_gov_pl_rejected_files). Shared code: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

from pathlib import Path
import tempfile
from typing import Any

import duckdb
import pyarrow as pa

from ingestion.common import bulk_bronze
from ingestion.to_landing import dane_gov_pl as landing

SOURCE = "dane_gov_pl"
REJECTED = "dane_gov_pl_rejected_files"
# Bronze column -> key of the reduced dataset record (extract.reduce)
DATASET = {
    "dataset_id": "id",
    "identifier": "identifier",
    "landing_page": "landing_page",
    "title_pl": "title_pl",
    "publisher_name": "publisher_name",
    "publisher_type": "publisher_type",
    "publisher_resource": "publisher_resource",
    "modified": "modified",
    "issued": "issued",
    "accrual_periodicity": "accrual_periodicity",
    "categories": "categories",
    "is_hvd": "is_hvd",
    "catalog_record_issued": "catalog_record_issued",
    "catalog_record_modified": "catalog_record_modified",
    "country": "country",
}
DATASET_KEYS = sorted([*DATASET.values(), "distributions"])
# Bronze column -> key of a reduced distribution (extract.reduce_distribution)
DISTRIBUTION = {
    "distribution_id": "id",
    "format": "format",
    "format_resource": "format_resource",
    "media_type": "media_type",
    "byte_size": "byte_size",
    "modified": "modified",
    "issued": "issued",
    "access_url": "access_url",
    "status": "status",
    "title_pl": "title_pl",
}
DISTRIBUTION_KEYS = sorted(DISTRIBUTION.values())
TABLES = {"dane_gov_pl_datasets": "datasets", "dane_gov_pl_distributions": "distributions"}


def columns(table: str) -> list[tuple[str, str]]:
    names = list(DATASET) if table == "dane_gov_pl_datasets" else ["dataset_id", *DISTRIBUTION]
    tail = [("source_row_number", "BIGINT")]
    if table == "dane_gov_pl_distributions":
        tail.append(("distribution_number", "BIGINT"))
    return [("entity", "VARCHAR"), *[(n, "VARCHAR") for n in names], *bulk_bronze.PROVENANCE, *tail]


def _value(obj: str, key: str) -> str:
    """A JSON value as source text: strings as they are, null as NULL, numbers, booleans and lists as JSON text."""
    path = f"{obj} -> '$.{key}'"
    return (
        f"case json_type({path}) when 'VARCHAR' then {obj} ->> '$.{key}' when 'NULL' then null "
        f"else cast({path} as varchar) end"
    )


def _lines(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    """Load and check the JSON lines into table ``lines`` (ValueError: an unknown layout rejects the file)."""
    con.execute(
        "create table lines as select j, row_number() over () as n "
        "from read_json_objects(?, format = 'newline_delimited', maximum_object_size = 268435456) as t(j)",
        [str(path)],
    )
    total, odd, keyless, repeated, odd_dist, dist_keyless = con.execute(
        """
        with d as (select unnest(cast(j -> '$.distributions' as json[])) as x from lines)
        select
            (select count(*) from lines),
            (select count(*) from lines where list_sort(json_keys(j)) <> ?::varchar[]),
            (select count(*) from lines where coalesce(j ->> '$.id', '') = ''),
            (select count(*) - count(distinct j ->> '$.id') from lines),
            (select count(*) from d where list_sort(json_keys(x)) <> ?::varchar[]),
            (select count(*) from d where coalesce(x ->> '$.id', '') = '')
        """,
        [DATASET_KEYS, DISTRIBUTION_KEYS],
    ).fetchall()[0]
    if not total:
        raise ValueError("no records")
    if odd or keyless or repeated or odd_dist or dist_keyless:
        raise ValueError(
            f"unknown layout: {odd} lines with other keys, {keyless} without id, {repeated} repeated id, "
            f"{odd_dist} distributions with other keys, {dist_keyless} distributions without id"
        )


def decoder(table: str):
    def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
        body.decode("utf-8")  # UnicodeDecodeError rejects the file
        prov = bulk_bronze.provenance(receipt)
        params = [prov["entity"], prov["raw_sha256"], prov["source_version"], prov["ingested_at"]]
        with tempfile.TemporaryDirectory(prefix="zohelo-dane-gov-pl-") as temporary:
            path = Path(temporary) / "catalogue.jsonl"
            path.write_bytes(body)
            con = duckdb.connect()
            try:
                _lines(con, path)
                if table == "dane_gov_pl_datasets":
                    values = ", ".join(f'{_value("j", key)} as "{name}"' for name, key in DATASET.items())
                    sql = f"""
                        select ? as entity, {values}, ? as raw_sha256, ? as source_version,
                            cast(? as timestamptz) as ingested_at, cast(n as bigint) as source_row_number
                        from lines order by n
                    """
                else:
                    values = ", ".join(f'{_value("x", key)} as "{name}"' for name, key in DISTRIBUTION.items())
                    sql = f"""
                        select ? as entity, j ->> '$.id' as dataset_id, {values}, ? as raw_sha256,
                            ? as source_version, cast(? as timestamptz) as ingested_at,
                            cast(n as bigint) as source_row_number, cast(k as bigint) as distribution_number
                        from lines, unnest(cast(j -> '$.distributions' as json[])) with ordinality as u(x, k)
                        order by n, k
                    """
                out = con.sql(sql, params=params).to_arrow_table()
            except duckdb.Error as exc:
                raise ValueError(f"not JSON lines: {str(exc)[:300]}") from exc
            finally:
                con.close()
        return out.cast(bulk_bronze.arrow_schema(columns(table)))

    return decode


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: one JSON-lines file of some hundred MB."""
    status = 0
    for table, folder in TABLES.items():
        status |= bulk_bronze.load(
            SOURCE,
            (namespace, table),
            (namespace, REJECTED),
            columns(table),
            decoder(table),
            folder,
            landing_root=landing_root,
            lake_root="02_bronze" if namespace == "bronze" else namespace,
            specs=landing.specs(),
        )
    return status
