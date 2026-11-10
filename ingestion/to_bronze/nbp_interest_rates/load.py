#!/usr/bin/env python3
"""NBP interest rates bronze.load: newest Landing copy of both XML files → bronze.nbp_interest_rate_items.

One row per ``<pozycja>`` (rate) or ``<odnosnik>`` (footnote) element of either file, attributes as source
text (rates keep the decimal comma):

  rates_archive  ``<pozycje obowiazuje_od=…>`` groups (one per MPC change since 1998-02-26) of
                 ``<pozycja id=… oprocentowanie=…/>``; effective_from = the group's date.
  rates_current  ``<tabela id=…>`` groups (stoproc: NBP base rates; required-reserve rates) with names
                 (nazwa PL, name EN), trend, footnote reference and the item's own obowiazuje_od, and
                 ``<odnosniki>`` with ``<odnosnik id odnosnik note>`` footnotes (text PL / EN).

An attribute the decoder does not know rejects the file (bronze.nbp_interest_rates_rejected_files), so a
change of the NBP format is never loaded silently. Code shared with other small bulk sources:
ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

from typing import Any
import xml.etree.ElementTree as ET

import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "nbp_interest_rates"
TABLE_ID = ("bronze", "nbp_interest_rate_items")
REJECTED_ID = ("bronze", "nbp_interest_rates_rejected_files")
ITEM_ATTRIBUTES = ("id", "oprocentowanie", "obowiazuje_od", "nazwa", "name", "odnosnik", "note", "trend")
GROUP_ATTRIBUTES = {"obowiazuje_od", "id", "naglowek", "header"}
COLUMNS = [
    ("entity", "VARCHAR"),
    ("published_on", "VARCHAR"),  # data_publikacji of the root element
    ("group_tag", "VARCHAR"),  # pozycje | tabela | odnosniki
    ("group_id", "VARCHAR"),  # tabela id
    ("group_header", "VARCHAR"),  # tabela header (EN)
    ("effective_from", "VARCHAR"),  # obowiazuje_od of the item, else of its group
    ("item_id", "VARCHAR"),
    ("rate_text", "VARCHAR"),  # oprocentowanie, e.g. "3,75"
    ("name_pl", "VARCHAR"),
    ("name_en", "VARCHAR"),
    ("footnote", "VARCHAR"),  # odnosnik: reference on a rate, text on a footnote
    ("note_en", "VARCHAR"),
    ("trend", "VARCHAR"),
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    root = ET.fromstring(body)
    if root.tag not in ("stopy_procentowe_archiwum", "stopy_procentowe"):
        raise ValueError(f"unexpected root element {root.tag!r}")
    base = {**bulk_bronze.provenance(receipt), "published_on": root.attrib.get("data_publikacji")}
    rows: list[dict[str, Any]] = []
    for group in root:
        unknown = set(group.attrib) - GROUP_ATTRIBUTES
        if unknown:
            raise ValueError(f"unknown attributes {sorted(unknown)} on <{group.tag}>")
        for item in group:
            if item.tag not in ("pozycja", "odnosnik"):
                raise ValueError(f"unexpected element <{item.tag}> in <{group.tag}>")
            unknown = set(item.attrib) - set(ITEM_ATTRIBUTES)
            if unknown:
                raise ValueError(f"unknown attributes {sorted(unknown)} on <pozycja id={item.attrib.get('id')!r}>")
            a = item.attrib
            rows.append(
                {
                    **base,
                    "group_tag": group.tag,
                    "group_id": group.attrib.get("id"),
                    "group_header": group.attrib.get("header"),
                    "effective_from": a.get("obowiazuje_od") or group.attrib.get("obowiazuje_od"),
                    "item_id": a.get("id"),
                    "rate_text": a.get("oprocentowanie"),
                    "name_pl": a.get("nazwa"),
                    "name_en": a.get("name"),
                    "footnote": a.get("odnosnik"),
                    "note_en": a.get("note"),
                    "trend": a.get("trend"),
                    "source_row_number": len(rows) + 1,
                }
            )
    if not rows:
        raise ValueError("no <pozycja> or <odnosnik> elements")
    return pa.Table.from_pylist(rows, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: two files of a few kB."""
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "items",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
    )
