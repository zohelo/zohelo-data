#!/usr/bin/env python3
"""GUGiK BDOT10k bronze.load: newest Landing copy of the package index → bronze.bdot10k_packages.

The Landing file is the WFS 1.0.0 answer of GUGiK's PobieranieBDOT10k service for feature type BDOT10k_powiaty
without geometry (ingestion/sources/gugik_bdot10k.yaml): a ``wfs:FeatureCollection`` of ``gml:featureMember``
elements, each one ``ms:BDOT10k_powiaty`` with exactly the children ``ms:TERYT``, ``ms:NAZWA_POWIATU``,
``ms:Data_aktualizacji`` and ``ms:URL_GML``. One Bronze row per feature, every value as the text read, the position
of the feature in the answer, and provenance.

Rejected (bronze.bdot10k_rejected_files): an answer that is not XML, an exception report or any root other than
``wfs:FeatureCollection``, a member that is not a ``ms:BDOT10k_powiaty`` with exactly the declared children, or no
feature at all. Code shared with other small bulk sources: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

from typing import Any
import xml.etree.ElementTree as ET

import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "gugik_bdot10k"
TABLE_ID = ("bronze", "bdot10k_packages")
REJECTED_ID = ("bronze", "bdot10k_rejected_files")
WFS = "{http://www.opengis.net/wfs}"
GML = "{http://www.opengis.net/gml}"
MS = "{http://mapserver.gis.umn.edu/mapserver}"
# Child element of ms:BDOT10k_powiaty (26 Nov 2026) → Bronze column, in the order the service writes them.
ATTRIBUTES = {
    "TERYT": "teryt",
    "NAZWA_POWIATU": "nazwa_powiatu",
    "Data_aktualizacji": "data_aktualizacji",
    "URL_GML": "url_gml",
}
COLUMNS = [
    ("entity", "VARCHAR"),
    *[(name, "VARCHAR") for name in ATTRIBUTES.values()],
    *bulk_bronze.PROVENANCE,
    ("feature_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    if receipt["entity"] != "powiat_packages":
        raise ValueError(f"no declared layout for entity {receipt['entity']!r}")
    root = ET.fromstring(body)  # ET.ParseError: rejected by bulk_bronze
    if root.tag != WFS + "FeatureCollection":
        raise ValueError(f"root is {root.tag}, not wfs:FeatureCollection: {ET.tostring(root)[:300]!r}")
    rows: dict[str, list] = {column: [] for column in ATTRIBUTES.values()}
    for number, member in enumerate(root.findall(GML + "featureMember"), start=1):
        features = list(member)
        if len(features) != 1 or features[0].tag != MS + "BDOT10k_powiaty":
            raise ValueError(f"feature {number}: not one ms:BDOT10k_powiaty: {[f.tag for f in features]}")
        children = [child.tag.removeprefix(MS) for child in features[0]]
        if children != list(ATTRIBUTES):
            raise ValueError(f"feature {number}: children are not the declared layout: {children}")
        for child in features[0]:
            rows[ATTRIBUTES[child.tag.removeprefix(MS)]].append(child.text)
    count = len(rows["teryt"])
    if count == 0:
        raise ValueError("no features")
    others = {child.tag for child in root} - {GML + "featureMember", GML + "boundedBy"}
    if others:
        raise ValueError(f"unexpected elements in the collection: {sorted(others)}")
    prov = bulk_bronze.provenance(receipt)
    data = {name: [prov[name]] * count for name in ("entity", "raw_sha256", "source_version", "ingested_at")}
    data.update(rows)
    data["feature_number"] = list(range(1, count + 1))
    return pa.Table.from_pydict({name: data[name] for name, _ in COLUMNS}, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: one answer of about 144 KB, decoded in well under a second."""
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "packages",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
    )
