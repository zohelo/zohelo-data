#!/usr/bin/env python3
"""IMGW public data API bronze.load: every landed snapshot → bronze.imgw_api_synop and bronze.imgw_api_meteo.

The API serves the current state only, so each landed response is a snapshot and every copy is loaded (accumulating
Bronze, ``all_versions``), one Parquet file per Landing file (ingestion/common/bulk_bronze.py):

- ``synop`` (https://danepubliczne.imgw.pl/api/data/synop): a JSON list, one object per synoptic station with
  id_stacji, stacja, data_pomiaru, godzina_pomiaru, temperatura, predkosc_wiatru, kierunek_wiatru,
  wilgotnosc_wzgledna, suma_opadu, cisnienie (62 stations, about 14 KB; Dev probe 43336937240);
- ``meteo`` (https://danepubliczne.imgw.pl/api/data/meteo/): one object per automatic station with its code, name,
  coordinates, altitude, founding year and the newest 10-minute readings with their times (788 stations, about
  520 KB); landed once a day for the station dimension.

One row per object, every value as the text the API sends (JSON null stays null). A response that is not a list of
objects, an object with other fields, or an empty list is rejected (bronze.imgw_api_rejected_files).
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "imgw_api"
REJECTED_ID = ("bronze", "imgw_api_rejected_files")
FIELDS = {
    "synop": [
        "id_stacji",
        "stacja",
        "data_pomiaru",
        "godzina_pomiaru",
        "temperatura",
        "predkosc_wiatru",
        "kierunek_wiatru",
        "wilgotnosc_wzgledna",
        "suma_opadu",
        "cisnienie",
    ],
    "meteo": [
        "kod_stacji",
        "nazwa_stacji",
        "lon",
        "lat",
        "rok_zalozenia_stacji",
        "wysokosc_npm",
        "temperatura_gruntu",
        "temperatura_gruntu_data",
        "temperatura_powietrza",
        "temperatura_powietrza_data",
        "wiatr_kierunek",
        "wiatr_kierunek_data",
        "wiatr_srednia_predkosc",
        "wiatr_srednia_predkosc_data",
        "wiatr_predkosc_maksymalna",
        "wiatr_predkosc_maksymalna_data",
        "wilgotnosc_wzgledna",
        "wilgotnosc_wzgledna_data",
        "wiatr_poryw_10min",
        "wiatr_poryw_10min_data",
        "opad_10min",
        "opad_10min_data",
    ],
}


def columns(entity: str) -> list[tuple[str, str]]:
    return [
        ("entity", "VARCHAR"),
        *[(name, "VARCHAR") for name in FIELDS[entity]],
        *bulk_bronze.PROVENANCE,
        ("source_row_number", "BIGINT"),
    ]


def decoder(entity: str):
    fields = FIELDS[entity]
    schema = bulk_bronze.arrow_schema(columns(entity))

    def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
        items = json.loads(body.decode("utf-8"), parse_int=str, parse_float=str)  # numbers as published text
        if not isinstance(items, list) or not items or not all(isinstance(i, dict) for i in items):
            raise ValueError(f"not a non-empty list of objects: {str(items)[:200]}")
        unknown = {k for i in items for k in i} ^ set(fields)
        if unknown:
            raise ValueError(f"fields differ from the known {entity} layout: {sorted(unknown)}")
        prov = bulk_bronze.provenance(receipt)
        rows = [
            {
                **{k: None if i.get(k) is None else str(i[k]) for k in fields},
                **prov,
                "source_row_number": n,
            }
            for n, i in enumerate(items, start=1)
        ]
        return pa.Table.from_pylist(rows, schema=schema)

    return decode


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: snapshots of 14 KB and 520 KB."""
    status = 0
    for entity in FIELDS:
        status |= bulk_bronze.load(
            SOURCE,
            (namespace, f"imgw_api_{entity}"),
            (namespace, REJECTED_ID[1]),
            columns(entity),
            decoder(entity),
            entity,
            landing_root=landing_root,
            lake_root="02_bronze" if namespace == "bronze" else namespace,
            all_versions=True,
            specs=[{"entity": entity}],
        )
    return status
