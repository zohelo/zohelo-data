#!/usr/bin/env python3
"""dane.gov.pl catalogue bronze.load: newest Landing copy → bronze.dane_gov_pl_datasets and _distributions.

The Landing file holds one JSON line per dataset record of the data.europa.eu catalogue `dane-gov-pl`, already
reduced to the allowlist of ingestion/to_landing/dane_gov_pl/extract.py. Datasets: one Bronze row per line, every
value as source text, the categories as JSON text. Distributions: one Bronze row per item of a line's
``distributions`` list, with the dataset id. DuckDB reads the lines with a declared schema (typed read_json, no
per-cell Python; the untyped JSON functions needed more than 12 GB for the 672,460 distributions, Dev trial
45822510638). The layout is checked first, line by line: a line or a distribution whose keys differ from the
allowlist, a line without its id or an id on two lines rejects the file (bronze.dane_gov_pl_rejected_files). Shared
code: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

import json
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
DATASET_KEYS = {*DATASET.values(), "distributions"}
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
DISTRIBUTION_KEYS = set(DISTRIBUTION.values())
TABLES = {"dane_gov_pl_datasets": "datasets", "dane_gov_pl_distributions": "distributions"}


def columns(table: str) -> list[tuple[str, str]]:
    names = list(DATASET) if table == "dane_gov_pl_datasets" else ["dataset_id", *DISTRIBUTION]
    tail = [("source_row_number", "BIGINT")]
    if table == "dane_gov_pl_distributions":
        tail.append(("distribution_number", "BIGINT"))
    return [("entity", "VARCHAR"), *[(n, "VARCHAR") for n in names], *bulk_bronze.PROVENANCE, *tail]


def check_layout(body: bytes) -> int:
    """Lines whose keys are exactly the allowlist, each with its own id (ValueError: an unknown layout rejects the
    file). Only the keys are looked at; the values are read by DuckDB. Returns the number of lines."""
    seen: set[str] = set()
    for n, line in enumerate(body.splitlines(), 1):
        record = json.loads(line)
        if not isinstance(record, dict) or set(record) != DATASET_KEYS:
            raise ValueError(f"line {n}: keys {sorted(record) if isinstance(record, dict) else type(record)}")
        if not isinstance(record["id"], str) or not record["id"] or record["id"] in seen:
            raise ValueError(f"line {n}: missing or repeated id")
        seen.add(record["id"])
        items = record["distributions"]
        if not isinstance(items, list):
            raise ValueError(f"line {n}: distributions is not a list")
        for item in items:
            if not isinstance(item, dict) or set(item) != DISTRIBUTION_KEYS or not item.get("id"):
                keys = sorted(item) if isinstance(item, dict) else type(item).__name__
                raise ValueError(f"line {n}: a distribution with keys {keys} or without an id")
    if not seen:
        raise ValueError("no records")
    return len(seen)


def _schema() -> str:
    """The declared read_json schema: every value as text (numbers and booleans as printed), categories as JSON."""
    distribution = ", ".join(f'"{key}" VARCHAR' for key in sorted(DISTRIBUTION_KEYS))
    fields = {key: "VARCHAR" for key in DATASET.values()} | {
        "categories": "JSON",
        "distributions": f"STRUCT({distribution})[]",
    }
    return "{" + ", ".join(f"'{key}': '{kind}'" for key, kind in fields.items()) + "}"


def decoder(table: str):
    def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
        body.decode("utf-8")  # UnicodeDecodeError rejects the file
        lines = check_layout(body)
        prov = bulk_bronze.provenance(receipt)
        params = [prov["entity"], prov["raw_sha256"], prov["source_version"], prov["ingested_at"]]
        with tempfile.TemporaryDirectory(prefix="zohelo-dane-gov-pl-") as temporary:
            path = Path(temporary) / "catalogue.jsonl"
            path.write_bytes(body)
            con = duckdb.connect()
            con.execute("set threads = 2")
            con.execute(f"set temp_directory = '{temporary}'")
            try:
                con.execute(
                    f"""
                    create table lines as
                    select *, row_number() over () as n
                    from read_json(?, format = 'newline_delimited', records = true, columns = {_schema()},
                                   maximum_object_size = 268435456)
                    """,
                    [str(path)],
                )
                if con.execute("select count(*) from lines").fetchall()[0][0] != lines:
                    raise ValueError("DuckDB read another number of lines than the file holds")
                if table == "dane_gov_pl_datasets":
                    values = ", ".join(f'cast("{key}" as varchar) as "{name}"' for name, key in DATASET.items())
                    sql = f"""
                        select ? as entity, {values}, ? as raw_sha256, ? as source_version,
                            cast(? as timestamptz) as ingested_at, cast(n as bigint) as source_row_number
                        from lines order by n
                    """
                else:
                    values = ", ".join(f'x."{key}" as "{name}"' for name, key in DISTRIBUTION.items())
                    sql = f"""
                        select ? as entity, l.id as dataset_id, {values}, ? as raw_sha256,
                            ? as source_version, cast(? as timestamptz) as ingested_at,
                            cast(l.n as bigint) as source_row_number, cast(u.k as bigint) as distribution_number
                        from lines as l, unnest(l.distributions) with ordinality as u(x, k)
                        order by l.n, u.k
                    """
                out = con.sql(sql, params=params).to_arrow_table()
            except duckdb.Error as exc:
                raise ValueError(f"not JSON lines: {str(exc)[:300]}") from exc
            finally:
                con.close()
        return out.cast(bulk_bronze.arrow_schema(columns(table)))

    return decode


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: one JSON-lines file of about 340 MB."""
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
