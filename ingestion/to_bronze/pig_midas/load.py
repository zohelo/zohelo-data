#!/usr/bin/env python3
"""PIG-PIB MIDAS bronze.load: newest Landing copy of each WFS layer → bronze.pig_midas_features.

Each Landing file is the server's GML 3.2 gzip answer to one GetFeature of one layer (ingestion/sources/
pig_midas.yaml). One Bronze row per ``wfs:member`` feature, every attribute as source text under an English
column name (ATTRIBUTES), the geometry left out (it stays in Landing and the Archive; only the geometry element's
type is kept). An attribute a layer is not known to carry (LAYERS), a feature of another type, a feature without
its key attribute, or a collection whose numberMatched, numberReturned and member count disagree (a truncated
layer) rejects the file into bronze.pig_midas_rejected_files. Shared loader: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

import gzip
import io
from typing import Any
import xml.etree.ElementTree as ET

import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "pig_midas"
TABLE_ID = ("bronze", "pig_midas_features")
REJECTED_ID = ("bronze", "pig_midas_rejected_files")
GML = "{http://www.opengis.net/gml/3.2}"

# Source attribute → Bronze column (several layers name one meaning differently: _OG/_TG, Uwagi/Komentarz).
ATTRIBUTES = {
    "OBJECTID": "object_id",
    "Numer_złoża": "deposit_number",
    "Nazwa_złoża": "deposit_name",
    "Pole_złoża": "deposit_field",
    "Kod": "mineral_code",
    "Kopalina": "mineral_name",
    "Commodity": "commodity_en",
    "Wszystkie_kopaliny_w_złożu": "all_minerals",
    "Numer_dokumentu": "document_number",
    "Lokalizacja": "location_method",
    "Lokalizacja_-_oznaczenie_załącznika_tabeli": "location_annex",
    "Informacja_o_aktualizacji_granic_wg_dokumentu": "boundary_update_note",
    "Informacja_o_dokumentowaniu_w_granicach_innego_złoża": "other_deposit_note",
    "Informacja_o_wykreśleniu_z_bilansu_zmianie_granic": "removal_note",
    "Uwagi": "remarks",
    "Komentarz": "remarks",
    "Nadzór_górniczy": "mining_office",
    "Nadzór_OUG": "mining_office",
    "Układ_współrzędnych": "coordinate_system",
    "ID_konturu": "contour_id",
    "Numer_w_rejestrze": "register_number",
    "Nazwa_OG": "area_name",
    "Nazwa_TG": "area_name",
    "Status": "status",
    "Data_ustanowienia": "established_on",
    "Data_ważności": "valid_until",
    "Data_zniesienia_OG": "abolished_on",
    "Data_zniesienia_TG": "abolished_on",
    "Decyzja_wyznaczająca": "establishing_decision",
    "Wydawca_decyzji_wyznaczającej": "establishing_authority",
    "Decyzja_znosząca": "abolishing_decision",
    "Wydawca_decyzji_znoszącej": "abolishing_authority",
    "Arkusz_mapy_1_50_000_PL1992": "map_sheet",
    "Powierzchnia_OG__m2_": "declared_area_m2",
    "Powierzchnia_TG__m2_": "declared_area_m2",
    "Shape_Area__m2_": "shape_area_m2",
}
GEOMETRY = {"SHAPE", "Shape"}

_DEPOSIT = {
    "OBJECTID", "Numer_złoża", "Nazwa_złoża", "Pole_złoża", "Kod", "Kopalina", "Commodity", "Numer_dokumentu",
    "Lokalizacja", "Lokalizacja_-_oznaczenie_załącznika_tabeli", "Informacja_o_aktualizacji_granic_wg_dokumentu",
    "Informacja_o_dokumentowaniu_w_granicach_innego_złoża", "Uwagi", "Nadzór_górniczy", "Układ_współrzędnych",
    "Shape_Area__m2_",
}  # fmt: skip
_CONTOUR = {
    "OBJECTID", "ID_konturu", "Numer_złoża", "Nazwa_złoża", "Kopalina", "Numer_w_rejestrze", "Status",
    "Data_ustanowienia", "Data_ważności", "Decyzja_wyznaczająca", "Wydawca_decyzji_wyznaczającej", "Nadzór_OUG",
    "Arkusz_mapy_1_50_000_PL1992", "Komentarz", "Układ_współrzędnych", "Shape_Area__m2_",
}  # fmt: skip
_ABOLISHED = {"Decyzja_znosząca", "Wydawca_decyzji_znoszącej"}

# entity → (feature type local name, attributes the layer may carry (Dev probe 43286265906), key attribute)
LAYERS: dict[str, tuple[str, set[str], str]] = {
    "documented_deposits": (
        "MIDAS_-_udokumentowane_złoża_kopalin", _DEPOSIT | {"Wszystkie_kopaliny_w_złożu"}, "OBJECTID"),
    "removed_deposits": (
        "MIDAS___złoża_wybilansowane__wyselekcjonowane_dane_archiwalne_",
        _DEPOSIT | {"Informacja_o_wykreśleniu_z_bilansu_zmianie_granic"}, "OBJECTID"),
    "mining_areas": ("MIDAS_-_obszary_górnicze", _CONTOUR | {"Nazwa_OG", "Powierzchnia_OG__m2_"}, "ID_konturu"),
    "mining_terrains": ("MIDAS_-_tereny_górnicze", _CONTOUR | {"Nazwa_TG", "Powierzchnia_TG__m2_"}, "ID_konturu"),
    "archival_mining_areas": (
        "MIDAS_-_archiwalne_obszary_górnicze__wyselekcjonowane_dane_archiwalne_",
        _CONTOUR | _ABOLISHED | {"Nazwa_OG", "Powierzchnia_OG__m2_", "Data_zniesienia_OG"}, "ID_konturu"),
    "archival_mining_terrains": (
        "MIDAS_-_archiwalne_tereny_górnicze__wyselekcjonowane_dane_archiwalne_",
        _CONTOUR | _ABOLISHED | {"Nazwa_TG", "Powierzchnia_TG__m2_", "Data_zniesienia_TG"}, "ID_konturu"),
}  # fmt: skip

VALUE_COLUMNS = list(dict.fromkeys(ATTRIBUTES.values()))
COLUMNS = [
    ("entity", "VARCHAR"),
    ("feature_id", "VARCHAR"),  # gml:id
    *[(c, "VARCHAR") for c in VALUE_COLUMNS],
    ("geometry_type", "VARCHAR"),  # e.g. MultiSurface; the coordinates stay in Landing/Archive
    ("served_at", "VARCHAR"),  # the collection's timeStamp (the request time, not a data version)
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    entity = receipt["entity"]
    if entity not in LAYERS:
        raise ValueError(f"unknown entity {entity!r}")
    type_name, allowed, key = LAYERS[entity]
    try:
        xml = gzip.decompress(body)
    except (OSError, EOFError) as exc:
        raise ValueError(f"not a gzip file: {exc}") from exc
    base = bulk_bronze.provenance(receipt)
    rows: list[dict[str, Any]] = []
    root_attrs: dict[str, str] = {}
    depth = 0
    for event, el in ET.iterparse(io.BytesIO(xml), events=("start", "end")):
        if event == "start":
            depth += 1
            if depth == 1:
                if local(el.tag) != "FeatureCollection":
                    raise ValueError(f"unexpected root element {local(el.tag)!r}")
                root_attrs = {local(k): v for k, v in el.attrib.items()}
            continue
        depth -= 1
        if depth != 1:
            continue
        if local(el.tag) != "member":
            raise ValueError(f"unexpected element <{local(el.tag)}> in the feature collection")
        for feature in el:
            if local(feature.tag) != type_name:
                raise ValueError(f"feature type {local(feature.tag)!r} is not {type_name!r}")
            row: dict[str, Any] = {**base, "feature_id": feature.attrib.get(f"{GML}id"), "geometry_type": None}
            for prop in feature:
                name = local(prop.tag)
                if name in GEOMETRY:
                    kids = list(prop)
                    row["geometry_type"] = local(kids[0].tag) if kids else None
                    continue
                if name not in allowed:
                    raise ValueError(f"unknown attribute {name!r} in layer {entity}")
                if row.get(ATTRIBUTES[name]) is not None:
                    raise ValueError(f"attribute {name!r} repeated in feature {row['feature_id']}")
                row[ATTRIBUTES[name]] = prop.text
            if not row.get(ATTRIBUTES[key]):
                raise ValueError(f"feature {row['feature_id']} has no {key}")
            rows.append({**row, "served_at": root_attrs.get("timeStamp"), "source_row_number": len(rows) + 1})
        el.clear()
    matched, returned = root_attrs.get("numberMatched"), root_attrs.get("numberReturned")
    if not rows or matched != returned or returned != str(len(rows)):
        raise ValueError(f"incomplete layer: numberMatched {matched}, numberReturned {returned}, members {len(rows)}")
    return pa.Table.from_pylist(rows, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: six layers, about 18 MB of gzip in all."""
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
