#!/usr/bin/env python3
"""PIG-PIB MIDAS bronze.load: newest Landing copy of each WFS layer → bronze.pig_midas_features.

Each Landing file is one GetFeature response in GEOJSON (a FeatureCollection, ``crs`` EPSG:4326) of one MIDAS feature
type (ingestion/sources/pig_midas.yaml). DuckDB's JSON functions read it: one row per feature, every property as the
text of its JSON value (numbers as written, JSON null as NULL, the producer's literal text "null" kept), under ASCII
column names (ATTRIBUTES; the OG/TG variants of the area and terrain layers share one column), the geometry as its
GeoJSON text, the feature's position in the response and provenance. ``GmlID`` (layer name + OBJECTID) is not kept.

Rejected (bronze.pig_midas_rejected_files): a response that is not a FeatureCollection, a ``crs`` other than EPSG:4326,
no features, a property set that is not the declared layout of its layer (a key missing or unknown), or an OBJECTID
missing or repeated within the response.
"""

from __future__ import annotations

from pathlib import Path
import tempfile
from typing import Any

import duckdb
import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "pig_midas"
TABLE_ID = ("bronze", "pig_midas_features")
REJECTED_ID = ("bronze", "pig_midas_rejected_files")
# WFS property → Bronze column (DescribeFeatureType, probe 45817073166).
ATTRIBUTES = {
    "OBJECTID": "objectid",
    "Kod": "kod",
    "Numer_złoża": "numer_zloza",
    "Nazwa_złoża": "nazwa_zloza",
    "Kopalina": "kopalina",
    "Wszystkie_kopaliny_w_złożu": "wszystkie_kopaliny",
    "Commodity": "commodity",
    "Pole_złoża": "pole_zloza",
    "Numer_dokumentu": "numer_dokumentu",
    "Informacja_o_aktualizacji_granic_wg_dokumentu": "aktualizacja_granic",
    "Informacja_o_wykreśleniu_z_bilansu_zmianie_granic": "wykreslenie_z_bilansu",
    "Uwagi": "uwagi",
    "Informacja_o_dokumentowaniu_w_granicach_innego_złoża": "w_granicach_innego_zloza",
    "Lokalizacja": "lokalizacja",
    "Lokalizacja_-_oznaczenie_załącznika_tabeli": "lokalizacja_zalacznik",
    "Układ_współrzędnych": "uklad_wspolrzednych",
    "Nadzór_górniczy": "nadzor_gorniczy",
    "ID_konturu": "id_konturu",
    "Nazwa_OG": "nazwa",
    "Nazwa_TG": "nazwa",
    "Status": "status",
    "Numer_w_rejestrze": "numer_w_rejestrze",
    "Data_ustanowienia": "data_ustanowienia",
    "Data_ważności": "data_waznosci",
    "Decyzja_wyznaczająca": "decyzja_wyznaczajaca",
    "Wydawca_decyzji_wyznaczającej": "wydawca_decyzji_wyznaczajacej",
    "Powierzchnia_OG__m2_": "powierzchnia_m2",
    "Powierzchnia_TG__m2_": "powierzchnia_m2",
    "Nadzór_OUG": "nadzor_oug",
    "Komentarz": "komentarz",
    "Decyzja_znosząca": "decyzja_znoszaca",
    "Wydawca_decyzji_znoszącej": "wydawca_decyzji_znoszacej",
    "Data_zniesienia_OG": "data_zniesienia",
    "Data_zniesienia_TG": "data_zniesienia",
    "Arkusz_mapy_1_50_000_PL1992": "arkusz_mapy",
    "Shape_Area__m2_": "shape_area_m2",
}
_DEPOSIT = [
    "OBJECTID",
    "Kod",
    "Numer_złoża",
    "Nazwa_złoża",
    "Kopalina",
    "Commodity",
    "Pole_złoża",
    "Numer_dokumentu",
    "Informacja_o_aktualizacji_granic_wg_dokumentu",
    "Uwagi",
    "Informacja_o_dokumentowaniu_w_granicach_innego_złoża",
    "Lokalizacja",
    "Lokalizacja_-_oznaczenie_załącznika_tabeli",
    "Układ_współrzędnych",
    "Nadzór_górniczy",
    "Shape_Area__m2_",
]
_AREA = [
    "OBJECTID",
    "ID_konturu",
    "Nazwa_{k}",
    "Status",
    "Numer_w_rejestrze",
    "Data_ustanowienia",
    "Data_ważności",
    "Decyzja_wyznaczająca",
    "Wydawca_decyzji_wyznaczającej",
    "Powierzchnia_{k}__m2_",
    "Nadzór_OUG",
    "Komentarz",
    "Numer_złoża",
    "Nazwa_złoża",
    "Kopalina",
    "Układ_współrzędnych",
    "Arkusz_mapy_1_50_000_PL1992",
    "Shape_Area__m2_",
]
_ABOLISHED = ["Decyzja_znosząca", "Wydawca_decyzji_znoszącej", "Data_zniesienia_{k}"]
LAYOUTS = {  # entity → the property keys of its features (besides GmlID)
    "deposits": set(_DEPOSIT) | {"Wszystkie_kopaliny_w_złożu"},
    "deposits_removed": set(_DEPOSIT) | {"Informacja_o_wykreśleniu_z_bilansu_zmianie_granic"},
    "mining_areas": {a.format(k="OG") for a in _AREA},
    "mining_terrains": {a.format(k="TG") for a in _AREA},
    "mining_areas_archival": {a.format(k="OG") for a in _AREA + _ABOLISHED},
    "mining_terrains_archival": {a.format(k="TG") for a in _AREA + _ABOLISHED},
}
NAMES = list(dict.fromkeys(ATTRIBUTES.values()))
COLUMNS = [
    ("entity", "VARCHAR"),
    *[(name, "VARCHAR") for name in NAMES],
    ("geometry_geojson", "VARCHAR"),
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def _sql(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    entity = receipt["entity"]
    if entity not in LAYOUTS:
        raise ValueError(f"no declared layout for entity {entity!r}")
    layout = LAYOUTS[entity]
    with tempfile.TemporaryDirectory(prefix="zohelo-pig-midas-") as temporary:
        path = Path(temporary) / "layer.json"
        path.write_bytes(body)
        con = duckdb.connect()
        con.execute("SET memory_limit = '4GB'; SET preserve_insertion_order = true")
        try:
            con.execute(f"CREATE TABLE doc AS SELECT content::JSON AS j FROM read_text('{path}')")
            kind, crs = con.execute("SELECT j->>'type', j->'crs'->'properties'->>'name' FROM doc").fetchone()
            if kind != "FeatureCollection" or crs != "EPSG:4326":
                raise ValueError(f"{entity}: not a FeatureCollection in EPSG:4326 (type {kind!r}, crs {crs!r})")
            con.execute(
                "CREATE TABLE f AS SELECT row_number() OVER () AS n, x AS f FROM "
                "(SELECT unnest(from_json(j->'features', '[\"JSON\"]')) AS x FROM doc)"
            )
            keys = {r[0] for r in con.execute("SELECT DISTINCT unnest(json_keys(f->'properties')) FROM f").fetchall()}
            keys.discard("GmlID")
            if keys != layout:
                raise ValueError(
                    f"{entity}: properties are not the declared layout: missing {sorted(layout - keys)}, "
                    f"unknown {sorted(keys - layout)}"
                )
            columns = []
            for name in NAMES:
                found = [k for k, v in ATTRIBUTES.items() if v == name and k in layout]
                columns.append(f"f->'properties'->>{_sql(found[0])} AS {name}" if found else f"NULL::VARCHAR AS {name}")
            rows = con.sql(
                f"SELECT {', '.join(columns)}, (f->'geometry')::VARCHAR AS geometry_geojson, n FROM f ORDER BY n"
            ).to_arrow_table()
        except duckdb.Error as exc:  # an unreadable response is a rejected file, not a failed run
            raise ValueError(f"{entity}: JSON read failed: {str(exc)[:300]}") from exc
        finally:
            con.close()
    count = rows.num_rows
    if count == 0:
        raise ValueError(f"{entity}: no features")
    ids = rows.column("objectid").to_pylist()
    if None in ids or len(set(ids)) != count:
        raise ValueError(f"{entity}: OBJECTID missing or repeated in the response")
    prov = bulk_bronze.provenance(receipt)
    data = {name: [prov[name]] * count for name in ("entity", "raw_sha256", "source_version", "ingested_at")}
    data.update({name: rows.column(name).cast(pa.string()) for name in [*NAMES, "geometry_geojson"]})
    data["source_row_number"] = rows.column("n").cast(pa.int64())
    return pa.Table.from_pydict({name: data[name] for name, _ in COLUMNS}, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: six responses of 5-30 MB, decoded in seconds each."""
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
