#!/usr/bin/env python3
"""ILOSTAT bronze.load: newest Landing copy of every declared bulk indicator file → bronze.ilostat_observations.

The generic SDMX Bronze format of the OECD onboarding (bronze.oecd_observations), for ILOSTAT's bulk
indicator files (``https://rplumber.ilo.org/data/indicator?id=<indicator>_<freq>&format=.csv.gz``): the
dimension columns ref_area, source, indicator, sex, classif1[, classif2], then time, obs_value and the
attributes obs_status, note_classif, note_indicator, note_source. One row per observation as published;
``dataflow`` is the indicator id, ``series_dimensions`` / ``series_key`` hold the dimension column names and
codes in file order (comma- and dot-separated), ``attribute_names`` / ``attribute_values`` the attribute
names and values (comma- and ``|``-separated); values stay source text. A file that is not gzip, a header
without ref_area first or without time followed by obs_value, a row of another width, or a code or attribute
containing a separator is rejected (bronze.ilostat_rejected_files). Code shared with other small bulk
sources: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

import csv
import gzip
import io
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "ilostat"
TABLE_ID = ("bronze", "ilostat_observations")
REJECTED_ID = ("bronze", "ilostat_rejected_files")
COLUMNS = [
    ("entity", "VARCHAR"),
    ("dataflow", "VARCHAR"),  # indicator, e.g. UNE_DEAP_SEX_AGE_RT
    ("series_dimensions", "VARCHAR"),  # ref_area,source,indicator,sex,classif1
    ("series_key", "VARCHAR"),  # POL.BA:432.UNE_DEAP_SEX_AGE_RT.SEX_T.AGE_YTHADULT_YGE15
    ("time_period", "VARCHAR"),  # 2025, 2025Q2, 2025M07
    ("obs_value", "VARCHAR"),
    ("attribute_names", "VARCHAR"),  # obs_status,note_classif,note_indicator,note_source
    ("attribute_values", "VARCHAR"),  # |||R1:3513
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    try:
        text = gzip.decompress(body).decode("utf-8-sig")
    except (OSError, EOFError) as exc:
        raise ValueError(f"not a gzip file: {exc}") from exc
    reader = csv.reader(io.StringIO(text, newline=""))
    header = next(reader, None)
    if not header or header[0] != "ref_area" or "time" not in header or "indicator" not in header:
        raise ValueError(f"not an ILOSTAT bulk header: {header[:6] if header else header}")
    time_at = header.index("time")
    if header[time_at + 1 : time_at + 2] != ["obs_value"]:
        raise ValueError("obs_value does not follow time")
    value_at, indicator_at = time_at + 1, header.index("indicator")
    dimensions, attributes = header[:time_at], header[value_at + 1 :]
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
        codes = row[:time_at]
        if any("." in c for c in codes) or any("|" in v for v in row[value_at + 1 :]):
            raise ValueError(f"line {number}: a code or attribute contains a separator")
        columns["dataflow"].append(row[indicator_at])
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
    """budget_minutes is unused: a few gzip files of a few MB."""
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
