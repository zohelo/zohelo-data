#!/usr/bin/env python3
"""ELI Poland bronze.load: newest Landing copy of every entity → bronze.eli_pl_publishers and bronze.eli_pl_acts.

publishers: one row per journal of /eli/acts (code, names, actsCount, the number of years listed). acts_<pub>_<year>:
one row per item of the year answer's ``items``, values as source text. DuckDB reads the JSON (no per-cell Python).
A year answer is rejected into bronze.eli_pl_rejected_files when ``count``, ``totalCount`` and the number of items
differ (a paged or cut answer is never loaded as complete), when an item has a key outside KNOWN_ACT_KEYS, or when an
item names another journal or year than its entity. Shared code: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

from pathlib import Path
import re
import tempfile
from typing import Any

import duckdb
import pyarrow as pa

from ingestion.common import bulk_bronze
from ingestion.to_landing import eli_pl as landing

SOURCE = "eli_pl"
REJECTED = "eli_pl_rejected_files"
KNOWN_PUBLISHER_KEYS = {"actsCount", "code", "name", "shortName", "years"}
KNOWN_ACT_KEYS = {
    "ELI", "address", "announcementDate", "changeDate", "displayAddress", "pos", "promulgation", "publisher",
    "status", "textHTML", "textPDF", "title", "type", "volume", "year",
}  # fmt: skip
PUBLISHER_COLUMNS = {
    "publisher": "i ->> '$.code'",
    "name": "i ->> '$.name'",
    "short_name": "i ->> '$.shortName'",
    "acts_count": "i ->> '$.actsCount'",
    "years_listed": "cast(json_array_length(i -> '$.years') as varchar)",
}
ACT_COLUMNS = {
    "eli_id": "i ->> '$.ELI'",
    "address": "i ->> '$.address'",
    "publisher": "i ->> '$.publisher'",
    "year": "i ->> '$.year'",
    "volume": "i ->> '$.volume'",
    "position": "i ->> '$.pos'",
    "display_address": "i ->> '$.displayAddress'",
    "title": "i ->> '$.title'",
    "act_type": "i ->> '$.type'",
    "status": "i ->> '$.status'",
    "promulgation": "i ->> '$.promulgation'",
    "announcement_date": "i ->> '$.announcementDate'",
    "change_date": "i ->> '$.changeDate'",
    "text_pdf": "i ->> '$.textPDF'",
    "text_html": "i ->> '$.textHTML'",
}
TABLES = {"eli_pl_publishers": PUBLISHER_COLUMNS, "eli_pl_acts": ACT_COLUMNS}


def columns(table: str) -> list[tuple[str, str]]:
    return [
        ("entity", "VARCHAR"),
        *[(name, "VARCHAR") for name in TABLES[table]],
        *bulk_bronze.PROVENANCE,
        ("source_row_number", "BIGINT"),
    ]


def table_of(entity: str) -> str:
    if entity == "publishers":
        return "eli_pl_publishers"
    if re.fullmatch(r"acts_(DU|MP)_\d{4}", entity):
        return "eli_pl_acts"
    raise ValueError(f"unknown entity {entity!r}")


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    body.decode("utf-8")  # UnicodeDecodeError rejects the file
    prov = bulk_bronze.provenance(receipt)
    table = table_of(prov["entity"])
    with tempfile.TemporaryDirectory(prefix="zohelo-eli-") as temporary:
        path = Path(temporary) / "data.json"
        path.write_bytes(body)
        con = duckdb.connect()
        try:
            source = f"read_json_objects('{path}', maximum_object_size = 536870912, format = 'unstructured') as t(j)"
            if table == "eli_pl_publishers":
                known, root = KNOWN_PUBLISHER_KEYS, "j"
                kind = con.execute(f"select json_type(j) from {source}").fetchall()
                if kind != [("ARRAY",)]:
                    raise ValueError(f"publishers: not one JSON list: {kind[:3]}")
            else:
                known, root = KNOWN_ACT_KEYS, "j -> '$.items'"
                head = con.execute(
                    f"select json_type(j), j ->> '$.count', j ->> '$.totalCount', j ->> '$.offset', "
                    f"json_array_length(j -> '$.items') from {source}"
                ).fetchall()
                if len(head) != 1 or head[0][0] != "OBJECT":
                    raise ValueError(f"year answer: not one JSON object: {head[:3]}")
                _t, count, total, offset, items = head[0]
                if items is None or str(items) != str(count) or str(count) != str(total) or str(offset) != "0":
                    raise ValueError(
                        f"year answer incomplete: count {count}, totalCount {total}, offset {offset}, items {items}"
                    )
            items = f"{source}, unnest(cast({root} as json[])) with ordinality as u(i, n)"
            unknown = con.execute(
                f"select distinct k from (select unnest(json_keys(i)) as k from {items}) where k not in "
                f"({', '.join(repr(k) for k in sorted(known))}) order by 1"
            ).fetchall()
            if unknown:
                raise ValueError(f"unknown item keys: {[k for (k,) in unknown][:10]}")
            if table == "eli_pl_acts":
                _a, publisher, year = prov["entity"].split("_")
                foreign = con.execute(
                    f"select count(*) from {items} where i ->> '$.publisher' is distinct from ? "
                    f"or i ->> '$.year' is distinct from ?",
                    [publisher, year],
                ).fetchall()[0][0]
                if foreign:
                    raise ValueError(f"{foreign} items of another journal or year than {prov['entity']}")
            out = con.sql(
                f"""
                select
                    ? as entity,
                    {", ".join(f"{expr} as {name}" for name, expr in TABLES[table].items())},
                    ? as raw_sha256,
                    ? as source_version,
                    cast(? as timestamptz) as ingested_at,
                    cast(n as bigint) as source_row_number
                from {items}
                order by n
                """,
                params=[prov["entity"], prov["raw_sha256"], prov["source_version"], prov["ingested_at"]],
            ).to_arrow_table()
        except duckdb.Error as exc:
            raise ValueError(f"not readable JSON: {str(exc)[:300]}") from exc
        finally:
            con.close()
    return out.cast(bulk_bronze.arrow_schema(columns(table)))


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: about 200 JSON answers, the largest about 1 MB."""
    specs = landing.specs(landing_root)
    status = 0
    for table in TABLES:
        mine = [s for s in specs if table_of(s["entity"]) == table]
        status = max(
            status,
            bulk_bronze.load(
                SOURCE,
                (namespace, table),
                (namespace, REJECTED),
                columns(table),
                decode,
                table.removeprefix("eli_pl_"),
                landing_root=landing_root,
                lake_root="02_bronze" if namespace == "bronze" else namespace,
                specs=mine,
            ),
        )
    return status
