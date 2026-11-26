#!/usr/bin/env python3
"""Crossref bronze.load: newest Landing copy of every request → bronze.crossref_facet_counts, bronze.crossref_funders.

Works files (``works_pl_*``, message-type ``work-list``, ``rows=0``): one row per facet value (facet, value, count
as published, the facet's ``value-count``) plus one ``_total`` row holding ``total-results``. A works file that
carries work items is rejected (the selection lands counts only). Funders file (``funders_pl``, ``funder-list``):
one row per funder (id, name, location, uri, alternative names, replaces, replaced-by; tokens are left out); a
file whose ``total-results`` exceeds its items (a second page would be needed) is rejected. Values stay text; DuckDB
reads the JSON. Unknown layouts go to bronze.crossref_rejected_files. Shared code: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

from pathlib import Path
import tempfile
from typing import Any

import duckdb
import pyarrow as pa

from ingestion.common import bulk_bronze
from ingestion.to_landing import crossref as landing

SOURCE = "crossref"
COUNTS_ID = ("bronze", "crossref_facet_counts")
FUNDERS_ID = ("bronze", "crossref_funders")
REJECTED_ID = ("bronze", "crossref_rejected_files")
COUNT_COLUMNS = [
    ("entity", "VARCHAR"),
    ("facet", "VARCHAR"),
    ("facet_value", "VARCHAR"),
    ("works", "VARCHAR"),
    ("facet_value_count", "VARCHAR"),
    ("total_results", "VARCHAR"),
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
FUNDER_COLUMNS = [
    ("entity", "VARCHAR"),
    ("funder_id", "VARCHAR"),
    ("name", "VARCHAR"),
    ("location", "VARCHAR"),
    ("uri", "VARCHAR"),
    ("alt_names", "VARCHAR"),
    ("replaces", "VARCHAR"),
    ("replaced_by", "VARCHAR"),
    ("total_results", "VARCHAR"),
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
JSON_READ = "read_json_objects(?, maximum_object_size = 268435456) as t(j)"


def _head(con, path: str) -> tuple:
    rows = con.execute(
        "select j ->> '$.status', j ->> '$.message-type', j ->> '$.message.total-results', "
        f"json_array_length(j -> '$.message.items') from {JSON_READ}",
        [path],
    ).fetchall()
    if len(rows) != 1:
        raise ValueError(f"expected one JSON object, found {len(rows)}")
    return rows[0]


def _decode(body: bytes, receipt: dict[str, Any], kind: str, columns: list[tuple[str, str]]) -> pa.Table:
    body.decode("utf-8")  # UnicodeDecodeError rejects the file
    prov = bulk_bronze.provenance(receipt)
    params = [prov["entity"], prov["raw_sha256"], prov["source_version"], prov["ingested_at"]]
    provenance = (
        "? as entity, {cols}, ? as raw_sha256, ? as source_version, cast(? as timestamptz) as ingested_at, "
        "source_row_number"
    )
    with tempfile.TemporaryDirectory(prefix="zohelo-crossref-") as temporary:
        path = str(Path(temporary) / "data.json")
        Path(path).write_bytes(body)
        con = duckdb.connect()
        try:
            status, message_type, total, items = _head(con, path)
            if status != "ok" or message_type != kind or total is None:
                raise ValueError(f"unknown layout: status {status!r}, message-type {message_type!r} (want {kind!r})")
            if kind == "work-list":
                if items:
                    raise ValueError(f"works file holds {items} work items; only counts are landed")
                cols = "facet, facet_value, works, facet_value_count, total_results"
                sql = f"""
                    select {provenance.format(cols=cols)} from (
                        select '_total' as facet, cast(null as varchar) as facet_value,
                            j ->> '$.message.total-results' as works, cast(null as varchar) as facet_value_count,
                            j ->> '$.message.total-results' as total_results, cast(0 as bigint) as source_row_number
                        from {JSON_READ}
                        union all
                        select f.key, w.unnest.key, w.unnest.value, f.value ->> '$.value-count',
                            j ->> '$.message.total-results', cast(w.ordinality as bigint)
                        from {JSON_READ},
                            unnest(map_entries(cast(coalesce(j -> '$.message.facets', '{{}}') as map(varchar, json))))
                                as u(f),
                            unnest(map_entries(cast(f.value -> '$.values' as map(varchar, varchar))))
                                with ordinality as w
                    ) order by facet, source_row_number
                """
                table = con.sql(sql, params=[*params, path, path]).to_arrow_table()
            else:
                if int(total) > items:
                    raise ValueError(f"funders file holds {items} of {total} funders (a second page is needed)")
                cols = "funder_id, name, location, uri, alt_names, replaces, replaced_by, total_results"
                sql = f"""
                    select {provenance.format(cols=cols)} from (
                        select i ->> '$.id' as funder_id, i ->> '$.name' as name, i ->> '$.location' as location,
                            i ->> '$.uri' as uri,
                            array_to_string(cast(i -> '$.alt-names' as varchar[]), ' | ') as alt_names,
                            array_to_string(cast(i -> '$.replaces' as varchar[]), ',') as replaces,
                            array_to_string(cast(i -> '$.replaced-by' as varchar[]), ',') as replaced_by,
                            j ->> '$.message.total-results' as total_results, cast(n as bigint) as source_row_number
                        from {JSON_READ}, unnest(cast(j -> '$.message.items' as json[])) with ordinality as u(i, n)
                    ) order by source_row_number
                """
                table = con.sql(sql, params=[*params, path]).to_arrow_table()
        except duckdb.Error as exc:
            raise ValueError(f"not a Crossref {kind} answer: {exc}") from exc
        finally:
            con.close()
    if not table.num_rows:
        raise ValueError("no rows")
    return table.cast(bulk_bronze.arrow_schema(columns))


def decode_counts(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    return _decode(body, receipt, "work-list", COUNT_COLUMNS)


def decode_funders(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    return _decode(body, receipt, "funder-list", FUNDER_COLUMNS)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: about 30 JSON answers of under 0.2 MB each."""
    specs = landing.specs()
    lake_root = "02_bronze" if namespace == "bronze" else namespace
    status = 0
    for table_id, columns, decode, folder, chosen in (
        (COUNTS_ID, COUNT_COLUMNS, decode_counts, "facet_counts", [s for s in specs if s["entity"] != "funders_pl"]),
        (FUNDERS_ID, FUNDER_COLUMNS, decode_funders, "funders", [s for s in specs if s["entity"] == "funders_pl"]),
    ):
        status |= bulk_bronze.load(
            SOURCE,
            (namespace, table_id[1]),
            (namespace, REJECTED_ID[1]),
            columns,
            decode,
            folder,
            landing_root=landing_root,
            lake_root=lake_root,
            specs=chosen,
        )
    return status
