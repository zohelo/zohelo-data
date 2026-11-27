#!/usr/bin/env python3
"""Lasy Państwowe BDL bronze.load: newest Landing copy of each WFS layer → bronze.lasy_bdl_features.

Each Landing file is one GetFeature response in application/json (a GeoJSON FeatureCollection, ``crs``
urn:ogc:def:crs:EPSG::2180, coordinates easting, northing in metres) of one BDL feature type
(ingestion/sources/lasy_bdl.yaml): districts = BDL:Nadleśnictwa, regions = BDL:RDLP; the request time stamp
is removed before Landing (ingestion/to_landing/lasy_bdl/). DuckDB's JSON functions read it:
one row per feature, every property as the text of its JSON value (JSON null as NULL), the GeoServer feature id, the
feature's position in the response and provenance. The geometry is not stored (it stays in Landing and the Archive):
DuckDB spatial computes from it, per feature, its planar area in EPSG:2180 in m² (the layers carry no area
attribute), and its centroid and a point on its surface in degrees (EPSG:4326, lon/lat), each as text; an invalid
polygon is made valid first.

Rejected (bronze.lasy_bdl_rejected_files): a response that is not a FeatureCollection, a ``crs`` other than
EPSG:2180, no features, a property set that is not the declared layout of its layer (a key missing or unknown), a
feature id missing or repeated, or a geometry missing or not a (multi)polygon.
"""

from __future__ import annotations

from pathlib import Path
import tempfile
from typing import Any

import duckdb
import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "lasy_bdl"
TABLE_ID = ("bronze", "lasy_bdl_features")
REJECTED_ID = ("bronze", "lasy_bdl_rejected_files")
CRS = "urn:ogc:def:crs:EPSG::2180"
# DescribeFeatureType (probe 45896744643): every property of the two types, all nillable.
LAYOUTS = {  # entity → the property keys of its features
    "districts": {"adress_forest", "a_year", "inspectorate_name", "region_cd", "inspectorate_cd"},
    "regions": {"a_year", "region_cd", "region_name"},
}
NAMES = ["adress_forest", "a_year", "region_cd", "region_name", "inspectorate_cd", "inspectorate_name"]
COMPUTED = ["area_m2", "centroid_lon", "centroid_lat", "point_lon", "point_lat"]
COLUMNS = [
    ("entity", "VARCHAR"),
    ("feature_id", "VARCHAR"),
    *[(name, "VARCHAR") for name in NAMES],
    *[(name, "VARCHAR") for name in COMPUTED],
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    entity = receipt["entity"]
    if entity not in LAYOUTS:
        raise ValueError(f"no declared layout for entity {entity!r}")
    layout = LAYOUTS[entity]
    with tempfile.TemporaryDirectory(prefix="zohelo-lasy-bdl-") as temporary:
        path = Path(temporary) / "layer.json"
        path.write_bytes(body)
        con = duckdb.connect()
        con.execute("SET memory_limit = '4GB'; SET preserve_insertion_order = true")
        con.execute("INSTALL spatial; LOAD spatial;")
        try:
            con.execute(f"CREATE TABLE doc AS SELECT content::JSON AS j FROM read_text('{path}')")
            kind, crs = con.execute("SELECT j->>'type', j->'crs'->'properties'->>'name' FROM doc").fetchone()
            if kind != "FeatureCollection" or crs != CRS:
                raise ValueError(f"{entity}: not a FeatureCollection in EPSG:2180 (type {kind!r}, crs {crs!r})")
            con.execute(
                "CREATE TABLE f AS SELECT row_number() OVER () AS n, x AS f FROM "
                "(SELECT unnest(from_json(j->'features', '[\"JSON\"]')) AS x FROM doc)"
            )
            keys = {r[0] for r in con.execute("SELECT DISTINCT unnest(json_keys(f->'properties')) FROM f").fetchall()}
            if keys != layout:
                raise ValueError(
                    f"{entity}: properties are not the declared layout: missing {sorted(layout - keys)}, "
                    f"unknown {sorted(keys - layout)}"
                )
            shapes = {r[0] for r in con.execute("SELECT DISTINCT f->'geometry'->>'type' FROM f").fetchall()}
            if not shapes <= {"MultiPolygon", "Polygon"}:
                raise ValueError(f"{entity}: geometry missing or not a polygon: {sorted(map(str, shapes))}")
            columns = [
                f"f->'properties'->>'{name}' AS {name}" if name in layout else f"NULL::VARCHAR AS {name}"
                for name in NAMES
            ]
            rows = con.sql(
                "WITH r AS (SELECT n, f, st_geomfromgeojson(f->>'geometry') AS g FROM f), "
                "p AS (SELECT n, f, CASE WHEN st_isvalid(g) THEN g ELSE st_makevalid(g) END AS geom FROM r), "
                "q AS (SELECT *, st_transform(st_centroid(geom), 'EPSG:2180', 'EPSG:4326', always_xy := true) AS c, "
                "st_transform(st_pointonsurface(geom), 'EPSG:2180', 'EPSG:4326', always_xy := true) AS s FROM p) "
                f"SELECT f->>'id' AS feature_id, {', '.join(columns)}, "
                "printf('%.2f', st_area(geom)) AS area_m2, printf('%.6f', st_x(c)) AS centroid_lon, "
                "printf('%.6f', st_y(c)) AS centroid_lat, printf('%.6f', st_x(s)) AS point_lon, "
                "printf('%.6f', st_y(s)) AS point_lat, n FROM q ORDER BY n"
            ).to_arrow_table()
        except duckdb.Error as exc:  # an unreadable response is a rejected file, not a failed run
            raise ValueError(f"{entity}: JSON or geometry read failed: {str(exc)[:300]}") from exc
        finally:
            con.close()
    count = rows.num_rows
    if count == 0:
        raise ValueError(f"{entity}: no features")
    ids = rows.column("feature_id").to_pylist()
    if None in ids or len(set(ids)) != count:
        raise ValueError(f"{entity}: feature id missing or repeated in the response")
    prov = bulk_bronze.provenance(receipt)
    data = {name: [prov[name]] * count for name in ("entity", "raw_sha256", "source_version", "ingested_at")}
    data.update({name: rows.column(name).cast(pa.string()) for name in ["feature_id", *NAMES, *COMPUTED]})
    data["source_row_number"] = rows.column("n").cast(pa.int64())
    return pa.Table.from_pydict({name: data[name] for name, _ in COLUMNS}, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: two responses of 11-50 MB, decoded in seconds each."""
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "features",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
    )
