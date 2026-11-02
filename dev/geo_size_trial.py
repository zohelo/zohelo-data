"""Dev trial for #2327: powiat and gmina GeoJSON sizes per simplification tolerance; writes nothing to R2."""

from __future__ import annotations

import gzip
import json

import duckdb
from pyiceberg.expressions import In

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate
from warehouse import publish

LEVELS = ["powiat", "gmina"]
TOLERANCES = [0.002, 0.005, 0.01, 0.02]


def main() -> None:
    cat = catalog()
    units = (
        cat.load_table(("silver", "prg_units"))
        .scan(row_filter=In("unit_level", LEVELS), selected_fields=("unit_level", "teryt_code", "geometry_wkb"))
        .to_arrow()
    )
    names = {
        row["geography_key"]: row["name_pl"]
        for row in cat.load_table(("gold", "dim_geography"))
        .scan(row_filter=In("level_name", LEVELS), selected_fields=("geography_key", "name_pl"))
        .to_arrow()
        .to_pylist()
    }
    con = duckdb.connect()
    con.sql("INSTALL spatial; LOAD spatial;")
    con.register("units", units)
    for level in LEVELS:
        sizes: dict[str, list[int]] = {}
        count = 0
        unmatched: list[str] = []
        for tolerance in TOLERANCES:
            rows = con.sql(
                """
                select 'TERC:' || teryt_code as geography_key,
                    ST_AsGeoJSON(ST_SimplifyPreserveTopology(ST_GeomFromWKB(geometry_wkb), ?)) as geometry
                from units where unit_level = ? order by 1
                """,
                params=[tolerance, level],
            ).fetchall()
            collection = []
            for key, geometry in rows:
                shape = json.loads(geometry)
                name = names.get(key, "")
                properties = {"geography_key": key, "name_pl": name, "name_en": name, "level": level}
                geom = {"type": shape["type"], "coordinates": publish._round(shape["coordinates"])}
                collection.append({"type": "Feature", "properties": properties, "geometry": geom})
            body = json.dumps(
                {"type": "FeatureCollection", "features": collection}, ensure_ascii=False, separators=(",", ":")
            ).encode()
            sizes[str(tolerance)] = [len(body), len(gzip.compress(body))]
            count = len(collection)
            unmatched = sorted(key for key, _ in rows if key not in names)
        annotate(
            "notice",
            f"geo size: {level}",
            {"features": count, "unmatched": len(unmatched), "first_unmatched": unmatched[:5], "sizes": sizes},
        )


if __name__ == "__main__":
    main()
