#!/usr/bin/env python3
"""GIOŚ surface-water status bronze.load: newest Landing copy of each layer page → bronze.gios_surface_water_features.

A Landing file (ingestion/to_landing/gios_surface_water/) is one ArcGIS query answer in JSON (``f=json``, attributes
only) of one page of one status layer, entity ``<category>_<assessment>_p<nn>``. DuckDB's JSON functions read it: one
row per feature, every attribute as the text of its JSON value (strings as published, NULL as NULL), under ASCII column
names, the feature's position in the answer and provenance; nothing is interpreted here.

Rejected (bronze.gios_surface_water_rejected_files): an entity of no declared layer, an answer with ``error`` or with
``exceededTransferLimit`` (a page cut at maxRecordCount), a field list other than the layer's declared layout (a field
missing or unknown), no features, or object ids (FID) other than the ones the page asked for (``objectIds`` of the
receipt's URL).
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

SOURCE = "gios_surface_water"
TABLE = "gios_surface_water_features"
REJECTED = "gios_surface_water_rejected_files"
ATTRIBUTES = {k: k.lower() for k in ("FID", "KOD_JCWP", "NAZWA_JCWP", "STATUS", "S_P_EKO", "STAN_CHEM", "OCENA")}
BASE = {"FID", "KOD_JCWP", "NAZWA_JCWP", "STATUS"}
CLASS_FIELD = {"ecological": "S_P_EKO", "chemical": "STAN_CHEM", "overall": "OCENA"}
# one feature class with the three class fields in these services (Dev probe 45922872272)
ALL_THREE = ("reservoirs", "transitional", "coastal")
COLUMNS = [
    ("entity", "VARCHAR"),
    *[(c, "VARCHAR") for c in ATTRIBUTES.values()],
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def layout(entity: str) -> set[str]:
    """The declared fields of the entity's layer (Dev probe 45922872272)."""
    from ingestion.to_landing.gios_surface_water import LAYERS

    layer = re.sub(r"_p\d{2}$", "", entity)
    if layer not in LAYERS or layer == entity:
        raise ValueError(f"no declared layer for entity {entity!r}")
    category, kind = layer.split("_", 2)[:2]
    return BASE | (set(CLASS_FIELD.values()) if category in ALL_THREE else {CLASS_FIELD[kind]})


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    entity = receipt["entity"]
    fields = layout(entity)
    asked = parse_qs(urlsplit(receipt["source_url"]).query).get("objectIds", [""])[0].split(",")
    with tempfile.TemporaryDirectory(prefix="zohelo-gios-surface-water-") as temporary:
        path = Path(temporary) / "page.json"
        path.write_bytes(body)
        con = duckdb.connect()
        try:
            con.execute("CREATE TABLE doc AS SELECT content::JSON AS j FROM read_text(?)", [str(path)])
            error, cut = con.execute("SELECT j->>'error', j->>'exceededTransferLimit' FROM doc").fetchone()
            fields_sql = "SELECT unnest(from_json(j->'fields', '[\"JSON\"]'))->>'name' FROM doc"
            names = {r[0] for r in con.execute(fields_sql).fetchall()}
            if error is not None or cut == "true":
                raise ValueError(f"{entity}: error {str(error)[:200]!r} or exceededTransferLimit {cut!r}")
            if names != fields:
                raise ValueError(f"{entity}: fields missing {sorted(fields - names)}, unknown {sorted(names - fields)}")
            columns = ", ".join(f"a->>'{k}' AS {v}" for k, v in ATTRIBUTES.items())
            rows = con.sql(
                f"SELECT {columns}, n FROM (SELECT row_number() OVER () AS n, f->'attributes' AS a FROM "
                "(SELECT unnest(from_json(j->'features', '[\"JSON\"]')) AS f FROM doc)) ORDER BY n"
            ).to_arrow_table()
        except duckdb.Error as exc:  # an unreadable answer is a rejected file, not a failed run
            raise ValueError(f"{entity}: JSON read failed: {str(exc)[:300]}") from exc
        finally:
            con.close()
    count = rows.num_rows
    if count == 0:
        raise ValueError(f"{entity}: no features")
    if sorted(rows.column("fid").to_pylist(), key=int) != sorted(asked, key=int):
        raise ValueError(f"{entity}: {count} features, not the {len(asked)} object ids the page asked for")
    prov = bulk_bronze.provenance(receipt)
    data = {name: [prov[name]] * count for name in ("entity", "raw_sha256", "source_version", "ingested_at")}
    data.update({name: rows.column(name).cast(pa.string()) for name in ATTRIBUTES.values()})
    data["source_row_number"] = rows.column("n").cast(pa.int64())
    return pa.Table.from_pydict({name: data[name] for name, _ in COLUMNS}, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: about 30 answers of under 200 kB."""
    del budget_minutes
    from ingestion.to_landing import gios_surface_water as landing

    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE),
        (namespace, REJECTED),
        COLUMNS,
        decode,
        "features",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=[{"entity": e} for e in landing.held(landing_root)],
    )
