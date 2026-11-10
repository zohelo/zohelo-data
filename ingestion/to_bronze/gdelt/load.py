#!/usr/bin/env python3
"""GDELT bronze.load: every landed copy → bronze.gdelt_timeline (one row per series point).

Each Landing file is one DOC 2.0 API timeline response for one topic query and mode (entity
``<topic>__volraw`` or ``<topic>__tone``): ``{"timeline": [{"series": …, "data": [{"date", "value", "norm"?}]}]}``.
Values stay source text. An empty object (no article matched) loads as no rows; anything else that is not a
timeline rejects the file. Copies overlap (2-week windows): every copy not yet held is loaded and Silver keeps
the newest value per topic and day. Shared code: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze
from ingestion.to_landing import gdelt as landing

SOURCE = "gdelt"
TABLE_ID = ("bronze", "gdelt_timeline")
REJECTED_ID = ("bronze", "gdelt_rejected_files")
COLUMNS = [
    ("entity", "VARCHAR"),
    ("topic_key", "VARCHAR"),
    ("mode", "VARCHAR"),  # volraw | tone
    ("series", "VARCHAR"),  # e.g. Article Count, Average Tone
    ("date_text", "VARCHAR"),  # e.g. 20261109T000000Z
    ("value_text", "VARCHAR"),
    ("norm_text", "VARCHAR"),  # volraw: all monitored articles that day
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    text = body.decode("utf-8").strip()
    if not text.startswith("{"):
        raise ValueError(f"not a JSON object: {text[:80]!r}")
    data = json.loads(text)
    topic, _, mode = receipt["entity"].rpartition("__")
    if mode not in landing.MODES:
        raise ValueError(f"unknown mode in entity {receipt['entity']!r}")
    if data and "timeline" not in data:
        raise ValueError(f"no timeline: keys {sorted(data)[:5]}")
    base = bulk_bronze.provenance(receipt)
    rows = []
    for series in data.get("timeline") or []:
        for point in series.get("data") or []:
            rows.append(
                {
                    **base,
                    "topic_key": topic,
                    "mode": mode,
                    "series": series.get("series"),
                    "date_text": str(point["date"]),
                    "value_text": str(point["value"]),
                    "norm_text": None if point.get("norm") is None else str(point["norm"]),
                    "source_row_number": len(rows) + 1,
                }
            )
    return pa.Table.from_pylist(rows, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: some 70 small JSON files a day."""
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "timeline",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        all_versions=True,
        specs=landing.specs(check_held=False),
    )
