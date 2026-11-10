#!/usr/bin/env python3
"""Wikimedia pageviews bronze.load: every landed copy → bronze.wiki_pageviews (one row per article and day).

Each Landing file is one REST API response ``{"items": [{project, article, granularity, timestamp, access,
agent, views}]}`` for one article of warehouse/seeds/topic_signal.csv. Copies overlap (14-day windows), so
every copy not yet held is loaded and Silver keeps the newest value per article and day. A response that is not
daily user views rejects the file. Shared code: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze
from ingestion.to_landing import wikimedia_pageviews as landing

SOURCE = "wikimedia_pageviews"
TABLE_ID = ("bronze", "wiki_pageviews")
REJECTED_ID = ("bronze", "wikimedia_pageviews_rejected_files")
COLUMNS = [
    ("entity", "VARCHAR"),
    ("project", "VARCHAR"),  # e.g. pl.wikipedia
    ("article", "VARCHAR"),  # title with underscores, as the API returns it
    ("day", "VARCHAR"),  # YYYYMMDD (timestamp YYYYMMDD00)
    ("views", "BIGINT"),
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    items = json.loads(body)["items"]
    base = bulk_bronze.provenance(receipt)
    rows = []
    for n, item in enumerate(items, start=1):
        if item.get("granularity") != "daily" or item.get("agent") != "user":
            raise ValueError(f"item {n}: not daily user views")
        rows.append(
            {
                **base,
                "project": item["project"],
                "article": item["article"],
                "day": str(item["timestamp"])[:8],
                "views": int(item["views"]),
                "source_row_number": n,
            }
        )
    if not rows:
        raise ValueError("no items")
    return pa.Table.from_pylist(rows, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: some 70 small JSON files a day."""
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "pageviews",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        all_versions=True,
        specs=landing.specs(check_held=False),
    )
