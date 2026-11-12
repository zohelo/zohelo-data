#!/usr/bin/env python3
"""BIS bronze.load: newest Landing copy of every declared SDMX selection → bronze.bis_observations.

The generic SDMX Bronze format of the OECD onboarding (bronze.oecd_observations), for the BIS SDMX REST
API's ``format=sdmx-csv`` (SDMX-CSV 2.0): STRUCTURE, STRUCTURE_ID (e.g. ``BIS:WS_CBPOL(1.0)``), ACTION, the
dimension columns, TIME_PERIOD, OBS_VALUE, then the series and observation attributes. One row per
observation as published; ``dataflow`` is STRUCTURE_ID, ``series_dimensions`` / ``series_key`` hold the
dimension ids and codes in file order (comma- and dot-separated), ``attribute_names`` / ``attribute_values``
the attribute ids and values (comma- and ``|``-separated); values stay source text. A file with another
header, a row of another width, or a code or attribute containing a separator is rejected
(bronze.bis_rejected_files). Code shared with other small bulk sources: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

import csv
import io
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "bis"
TABLE_ID = ("bronze", "bis_observations")
REJECTED_ID = ("bronze", "bis_rejected_files")
COLUMNS = [
    ("entity", "VARCHAR"),
    ("dataflow", "VARCHAR"),  # e.g. BIS:WS_CBPOL(1.0)
    ("series_dimensions", "VARCHAR"),  # FREQ,REF_AREA
    ("series_key", "VARCHAR"),  # M.PL
    ("time_period", "VARCHAR"),
    ("obs_value", "VARCHAR"),
    ("attribute_names", "VARCHAR"),  # UNIT_MEASURE,UNIT_MULT,…,OBS_STATUS,OBS_CONF,OBS_PRE_BREAK
    ("attribute_values", "VARCHAR"),  # 368|0|…|A|F|
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)
LEAD = ["STRUCTURE", "STRUCTURE_ID", "ACTION"]


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    reader = csv.reader(io.StringIO(body.decode("utf-8-sig"), newline=""))
    header = next(reader, None)
    if not header or header[:3] != LEAD or "TIME_PERIOD" not in header or "OBS_VALUE" not in header:
        raise ValueError(f"not an SDMX-CSV 2.0 header: {header[:6] if header else header}")
    time_at, value_at = header.index("TIME_PERIOD"), header.index("OBS_VALUE")
    if value_at != time_at + 1:
        raise ValueError("OBS_VALUE does not follow TIME_PERIOD")
    dimensions, attributes = header[3:time_at], header[value_at + 1 :]
    base = {
        **bulk_bronze.provenance(receipt),
        "series_dimensions": ",".join(dimensions),
        "attribute_names": ",".join(attributes),
    }
    columns: dict[str, list[Any]] = {
        name: []
        for name in ("dataflow", "series_key", "time_period", "obs_value", "attribute_values", "source_row_number")
    }
    for number, row in enumerate(reader, start=2):
        if not row:
            continue
        if len(row) != len(header):
            raise ValueError(f"line {number}: {len(row)} fields, header has {len(header)}")
        codes = row[3:time_at]
        if any("." in c for c in codes) or any("|" in v for v in row[value_at + 1 :]):
            raise ValueError(f"line {number}: a code or attribute contains a separator")
        columns["dataflow"].append(row[1])
        columns["series_key"].append(".".join(codes))
        columns["time_period"].append(row[time_at])
        columns["obs_value"].append(row[value_at])
        columns["attribute_values"].append("|".join(row[value_at + 1 :]))
        columns["source_row_number"].append(number)
    count = len(columns["dataflow"])
    if not count:
        raise ValueError("no observations")
    data = {name: [base[name]] * count for name in ("entity", "series_dimensions", "attribute_names")}
    data.update(columns)
    data.update({name: [base[name]] * count for name in ("raw_sha256", "source_version", "ingested_at")})
    return pa.Table.from_pydict({name: data[name] for name, _ in COLUMNS}, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: a few files of tens of MB."""
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "observations",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
    )
