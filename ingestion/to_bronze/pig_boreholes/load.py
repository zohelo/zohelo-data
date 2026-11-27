#!/usr/bin/env python3
"""PIG-PIB CBDG boreholes bronze.load: every page of the newest complete download → bronze.pig_boreholes_features.

Each Landing file is one GEOJSON page (a FeatureCollection, ``crs`` EPSG:4326) of the WFS feature type
``cbdg_otwory:Otwory_wiertnicze_CBDG`` (ingestion/to_landing/pig_boreholes/). DuckDB's JSON functions read it: one row
per feature, every property as the text of its JSON value (JSON null as NULL, the producer's literal text "null" kept),
under ASCII column names (ATTRIBUTES), the point's longitude and latitude as the text of its coordinates, the page
(the receipt's part), the feature's position in the page and provenance. ``GmlID`` (layer name + OBJECTID) is not
kept.

Rejected (bronze.pig_boreholes_rejected_files): a page that is not a FeatureCollection in EPSG:4326, no features, a
property set other than the declared layout (a key missing or unknown), a geometry other than a Point (or none), or an
OBJECTID missing or repeated within the page. A newest download whose parts are not all landed is not loaded (the run
fails; the next Landing run lands a complete one).
"""

from __future__ import annotations

from pathlib import Path
import tempfile
from typing import Any

import duckdb
import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "pig_boreholes"
ENTITY = "boreholes"
TABLE_ID = ("bronze", "pig_boreholes_features")
REJECTED_ID = ("bronze", "pig_boreholes_rejected_files")
# WFS property → Bronze column (GEOJSON keys, Dev probe 45898682643); Polish and English labels as published.
ATTRIBUTES = {
    "OBJECTID": "objectid",
    "ID_CBDG": "id_cbdg",
    "Nazwa": "nazwa",
    "Głębokość__m_": "glebokosc_m",
    "Stratygrafia": "stratygrafia",
    "Stratigraphy": "stratigraphy",
    "Cel_wiercenia": "cel_wiercenia",
    "Drilling_purpose": "drilling_purpose",
    "Rok_wiercenia": "rok_wiercenia",
    "Rzędna__m_n.p.m._": "rzedna_m_npm",
    "Długość_geograficzna": "dlugosc_geograficzna",
    "Szerokość_geograficzna": "szerokosc_geograficzna",
    "X_PL-1992": "x_pl1992",
    "Y_PL-1992": "y_pl1992",
    "Podstawa_lokalizacji": "podstawa_lokalizacji",
    "Location_basis": "location_basis",
    "Miejscowość": "miejscowosc",
    "Gmina": "gmina",
    "Powiat": "powiat",
    "Województwo": "wojewodztwo",
    "Archiwum": "archiwum",
    "Nr_inwentarzowy": "nr_inwentarzowy",
    "Rdzeń": "rdzen",
    "Geofizyka": "geofizyka",
    "Geophysics": "geophysics",
    "Analizy": "analizy",
    "Analyses": "analyses",
    "Badania": "badania",
    "Research": "research",
}
LAYOUT = set(ATTRIBUTES)
NAMES = list(ATTRIBUTES.values())
COLUMNS = [
    ("entity", "VARCHAR"),
    *[(name, "VARCHAR") for name in NAMES],
    ("point_lon", "VARCHAR"),
    ("point_lat", "VARCHAR"),
    *bulk_bronze.PROVENANCE,
    ("page", "BIGINT"),
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def _sql(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    if receipt["entity"] != ENTITY:
        raise ValueError(f"no declared layout for entity {receipt['entity']!r}")
    page = int(receipt["part"]["index"])
    with tempfile.TemporaryDirectory(prefix="zohelo-pig-boreholes-") as temporary:
        path = Path(temporary) / "page.json"
        path.write_bytes(body)
        con = duckdb.connect()
        con.execute("SET memory_limit = '4GB'; SET preserve_insertion_order = true")
        try:
            con.execute(f"CREATE TABLE doc AS SELECT content::JSON AS j FROM read_text('{path}')")
            kind, crs = con.execute("SELECT j->>'type', j->'crs'->'properties'->>'name' FROM doc").fetchall()[0]
            if kind != "FeatureCollection" or crs != "EPSG:4326":
                raise ValueError(f"page {page}: not a FeatureCollection in EPSG:4326 (type {kind!r}, crs {crs!r})")
            con.execute(
                "CREATE TABLE f AS SELECT row_number() OVER () AS n, x AS f FROM "
                "(SELECT unnest(from_json(j->'features', '[\"JSON\"]')) AS x FROM doc)"
            )
            keys = {r[0] for r in con.execute("SELECT DISTINCT unnest(json_keys(f->'properties')) FROM f").fetchall()}
            keys.discard("GmlID")
            if keys != LAYOUT:
                raise ValueError(
                    f"page {page}: properties are not the declared layout: missing {sorted(LAYOUT - keys)}, "
                    f"unknown {sorted(keys - LAYOUT)}"
                )
            shapes = con.execute(
                "SELECT count(*) FROM f WHERE coalesce(f->'geometry'->>'type', '') <> 'Point' "
                "OR json_array_length(f->'geometry'->'coordinates') <> 2"
            ).fetchall()[0][0]
            if shapes:
                raise ValueError(f"page {page}: {shapes} features without a two-coordinate Point geometry")
            columns = [f"f->'properties'->>{_sql(k)} AS {v}" for k, v in ATTRIBUTES.items()]
            rows = con.sql(
                f"SELECT {', '.join(columns)}, f->'geometry'->'coordinates'->>0 AS point_lon, "
                f"f->'geometry'->'coordinates'->>1 AS point_lat, n FROM f ORDER BY n"
            ).to_arrow_table()
        except duckdb.Error as exc:  # an unreadable page is a rejected file, not a failed run
            raise ValueError(f"page {page}: JSON read failed: {str(exc)[:300]}") from exc
        finally:
            con.close()
    count = rows.num_rows
    if count == 0:
        raise ValueError(f"page {page}: no features")
    ids = rows.column("objectid").to_pylist()
    if None in ids or len(set(ids)) != count:
        raise ValueError(f"page {page}: OBJECTID missing or repeated in the page")
    prov = bulk_bronze.provenance(receipt)
    data: dict[str, Any] = {
        name: [prov[name]] * count for name in ("entity", "raw_sha256", "source_version", "ingested_at")
    }
    data.update({name: rows.column(name).cast(pa.string()) for name in [*NAMES, "point_lon", "point_lat"]})
    data["page"] = [page] * count
    data["source_row_number"] = rows.column("n").cast(pa.int64())
    return pa.Table.from_pydict({name: data[name] for name, _ in COLUMNS}, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: about 14 pages of 24 MB, decoded in seconds each."""
    from ingestion.common.r2 import req, s3
    from ingestion.to_landing.patterns import bulk_file

    held = bulk_file.landed(s3(), req("STORAGE_LANDING_BUCKET_NAME"), bulk_file.prefix(SOURCE, ENTITY, landing_root))
    newest = [r for r in held if held and r["ingested_at"] == held[-1]["ingested_at"]]
    if newest and sorted(r["part"]["index"] for r in newest) != list(range(newest[0]["part"]["total"])):
        raise RuntimeError(
            f"{SOURCE}: newest download {newest[0]['ingested_at']} has {len(newest)} of {newest[0]['part']['total']} "
            "parts; not loaded"
        )
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "features",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=[{"entity": ENTITY}],
    )
