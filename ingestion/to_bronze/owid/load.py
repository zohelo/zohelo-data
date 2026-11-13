#!/usr/bin/env python3
"""Our World in Data bronze.load: newest Landing copy of each declared chart file → Bronze.

Chart CSV (``<slug>.csv?csvType=full&useColumnShortNames=true``): columns entity, code, year, then the chart's
value and attribute columns by short name. Bronze ``owid_values`` keeps one row per CSV row × non-id column whose
cell is not empty (an empty cell is a missing value, never a zero): the OWID entity name, its code (ISO 3166-1
alpha-3, ``OWID_…`` for OWID's own areas, empty for regions and income groups), the year text, the column short
name and the cell text as published. The header of each chart is declared (HEADERS): another header, another row
width or an empty file is rejected into ``bronze.owid_rejected_files``.

Chart metadata (``<slug>.metadata.json``): Bronze ``owid_charts`` keeps one row per chart column with the chart's
title and citation and the column's short name, unit, lastUpdated, nextUpdate, citationShort and indicator id, as
text. A document without ``chart`` and ``columns``, or a column that is not an object, is rejected; a column
without ``shortName`` (an attribute column such as Maddison's annotations) keeps its metadata.json key as short_name.
Code shared with other small bulk sources: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

import csv
import io
import json
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze
from ingestion.to_landing.patterns import bulk_file

SOURCE = "owid"
VALUES_ID = ("bronze", "owid_values")
CHARTS_ID = ("bronze", "owid_charts")
REJECTED_ID = ("bronze", "owid_rejected_files")
ID_COLUMNS = ["entity", "code", "year"]
# Declared header of each chart CSV (Dev probe of 12 Nov 2026); a change is reviewed before it is loaded.
HEADERS = {
    "consumption_co2_per_capita": [*ID_COLUMNS, "consumption_emissions_per_capita"],
    "electoral_democracy_index": [*ID_COLUMNS, "electdem_vdem__estimate_best", "owid_region"],
    "gdp_per_capita_maddison": [*ID_COLUMNS, "gdp_per_capita", "gdp_per_capita__annotations"],
}
METADATA_SUFFIX = "_metadata"

VALUE_COLUMNS = [
    ("entity", "VARCHAR"),  # Landing entity = chart
    ("entity_name", "VARCHAR"),  # OWID entity (country, region, historical state)
    ("code", "VARCHAR"),  # ISO 3166-1 alpha-3, OWID_… or empty
    ("year", "VARCHAR"),
    ("column_name", "VARCHAR"),  # value or attribute column short name
    ("value", "VARCHAR"),  # cell text as published
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
CHART_FIELDS = {  # Bronze column → key of a metadata.json column
    "title_short": "titleShort",
    "unit": "unit",
    "short_unit": "shortUnit",
    "last_updated": "lastUpdated",
    "next_update": "nextUpdate",
    "timespan": "timespan",
    "value_type": "type",
    "owid_variable_id": "owidVariableId",
    "citation_short": "citationShort",
    "full_metadata_url": "fullMetadata",
}
CHART_COLUMNS = [
    ("entity", "VARCHAR"),  # Landing entity = <chart>_metadata
    ("chart", "VARCHAR"),
    ("chart_title", "VARCHAR"),
    ("chart_citation", "VARCHAR"),
    ("original_chart_url", "VARCHAR"),
    ("column_title", "VARCHAR"),  # the key of the column in metadata.json
    ("short_name", "VARCHAR"),
    *[(name, "VARCHAR") for name in CHART_FIELDS],
    ("date_downloaded", "VARCHAR"),
    *bulk_bronze.PROVENANCE,
]


def decode_values(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    expected = HEADERS.get(receipt["entity"])
    if expected is None:
        raise ValueError(f"no declared header for entity {receipt['entity']}")
    reader = csv.reader(io.StringIO(body.decode("utf-8-sig"), newline=""))
    header = next(reader, None)
    if header != expected:
        raise ValueError(f"header {header[:8] if header else header} is not the declared {expected}")
    base = bulk_bronze.provenance(receipt)
    data: dict[str, list[Any]] = {name: [] for name, _ in VALUE_COLUMNS}
    for number, row in enumerate(reader, start=2):
        if not row:
            continue
        if len(row) != len(header):
            raise ValueError(f"line {number}: {len(row)} fields, header has {len(header)}")
        for name, value in zip(header[3:], row[3:], strict=True):
            if value == "":
                continue
            data["entity_name"].append(row[0])
            data["code"].append(row[1])
            data["year"].append(row[2])
            data["column_name"].append(name)
            data["value"].append(value)
            data["source_row_number"].append(number)
    count = len(data["value"])
    if not count:
        raise ValueError("no values")
    for name in ("entity", "raw_sha256", "source_version", "ingested_at"):
        data[name] = [base[name]] * count
    return pa.Table.from_pydict(data, schema=bulk_bronze.arrow_schema(VALUE_COLUMNS))


def text(value: Any) -> str | None:
    return None if value is None else str(value)


def decode_chart(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    document = json.loads(body)
    if not isinstance(document, dict) or not isinstance(document.get("chart"), dict):
        raise ValueError("metadata.json without a chart object")
    columns = document.get("columns")
    if not isinstance(columns, dict) or not columns:
        raise ValueError("metadata.json without columns")
    chart = document["chart"]
    base = bulk_bronze.provenance(receipt)
    rows = []
    for title, column in columns.items():
        if not isinstance(column, dict):
            raise ValueError(f"column {title!r} is not an object")
        # An attribute column can come without shortName (Maddison: '900793-annotations', Dev run 43340826282):
        # its key in metadata.json stands in, so the chart's other columns still load.
        short_name = column.get("shortName") or title
        rows.append(
            {
                **base,
                "chart": receipt["entity"].removesuffix(METADATA_SUFFIX),
                "chart_title": text(chart.get("title")),
                "chart_citation": text(chart.get("citation")),
                "original_chart_url": text(chart.get("originalChartUrl")),
                "column_title": title,
                "short_name": str(short_name),
                **{name: text(column.get(key)) for name, key in CHART_FIELDS.items()},
                "date_downloaded": text(document.get("dateDownloaded")),
            }
        )
    return pa.Table.from_pylist(rows, schema=bulk_bronze.arrow_schema(CHART_COLUMNS))


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: six files of about 1.8 MB in total."""
    specs = bulk_file.declaration(SOURCE)
    charts = [s for s in specs if s["entity"].endswith(METADATA_SUFFIX)]
    values = [s for s in specs if not s["entity"].endswith(METADATA_SUFFIX)]
    lake_root = "02_bronze" if namespace == "bronze" else namespace
    common = {"landing_root": landing_root, "lake_root": lake_root}
    rejected = (namespace, REJECTED_ID[1])
    status = bulk_bronze.load(
        SOURCE, (namespace, VALUES_ID[1]), rejected, VALUE_COLUMNS, decode_values, "values", specs=values, **common
    )
    return max(
        status,
        bulk_bronze.load(
            SOURCE, (namespace, CHARTS_ID[1]), rejected, CHART_COLUMNS, decode_chart, "charts", specs=charts, **common
        ),
    )
