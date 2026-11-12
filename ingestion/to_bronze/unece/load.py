#!/usr/bin/env python3
"""UNECE bronze.load: newest Landing copy of every table (JSON-stat 2.0) → bronze.unece_observations, unece_codes.

Observations use the generic SDMX Bronze format of the OECD onboarding (bronze.oecd_observations): one row per
cell with a value, ``dataflow`` = the table title, ``series_dimensions`` / ``series_key`` the non-time variable
codes and value codes (comma- and ``|``-separated: PXWeb value codes contain dots, e.g. ``ME.1041``),
``time_period`` the time variable's label as published (1990, 2025Q2, 2025M07), ``obs_value`` as text,
``attribute_names`` / ``attribute_values`` the cell status. The PXWeb value codes are opaque (``ME.47``,
``616``, ``c0000803``), so bronze.unece_codes keeps each
file's variable and value labels (one row per variable × value). A file that is not JSON-stat 2.0, whose value
count does not match its sizes, or with a code containing ``|`` is rejected (bronze.unece_rejected_files).
Entities are the table folders in Landing (``to_landing/unece/``, a table above the API cell limit in parts
``<table>__p<value code>``). Code shared with other small bulk sources: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

from itertools import product
import json
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze
from ingestion.common.r2 import req, s3

SOURCE = "unece"
TABLE_ID = ("bronze", "unece_observations")
CODES_ID = ("bronze", "unece_codes")
REJECTED_ID = ("bronze", "unece_rejected_files")
COLUMNS = [
    ("entity", "VARCHAR"),
    ("dataflow", "VARCHAR"),  # table title
    ("series_dimensions", "VARCHAR"),  # Variable,Measurement,Country
    ("series_key", "VARCHAR"),  # ME.47|c0000803|616
    ("time_period", "VARCHAR"),  # 2025
    ("obs_value", "VARCHAR"),
    ("attribute_names", "VARCHAR"),  # status
    ("attribute_values", "VARCHAR"),
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),  # cell position in the JSON-stat value array
]
CODE_COLUMNS = [
    ("entity", "VARCHAR"),
    ("dimension", "VARCHAR"),
    ("dimension_label", "VARCHAR"),
    ("code", "VARCHAR"),
    ("label", "VARCHAR"),
    ("is_time", "VARCHAR"),  # true / false
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]


def parse(body: bytes) -> tuple[dict, list[str], list[list[str]], str]:
    """The JSON-stat dataset, its dimension ids, each dimension's codes in index order, and the time dimension."""
    data = json.loads(body)
    if data.get("class") != "dataset" or not data.get("id") or "value" not in data:
        raise ValueError("not a JSON-stat 2.0 dataset")
    ids, sizes = data["id"], data["size"]
    codes = []
    for dim, size in zip(ids, sizes, strict=True):
        index = data["dimension"][dim]["category"]["index"]
        ordered = list(index) if isinstance(index, list) else sorted(index, key=index.get)
        if len(ordered) != size:
            raise ValueError(f"dimension {dim}: {len(ordered)} codes, size {size}")
        codes.append(ordered)
    time_ids = (data.get("role") or {}).get("time") or [ids[-1]]
    return data, ids, codes, time_ids[0]


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    data, ids, codes, time_dim = parse(body)
    values, status = data["value"], data.get("status") or {}
    cells = 1
    for c in codes:
        cells *= len(c)
    if isinstance(values, list) and len(values) != cells:
        raise ValueError(f"{len(values)} values for {cells} cells")
    t = ids.index(time_dim)
    labels = data["dimension"][time_dim]["category"].get("label") or {}
    others = [i for i in range(len(ids)) if i != t]
    if any("|" in code for i in others for code in codes[i]):
        raise ValueError("a value code contains the separator |")
    base = bulk_bronze.provenance(receipt)
    out: dict[str, list[Any]] = {name: [] for name, _ in COLUMNS}
    for position, combo in enumerate(product(*codes)):
        value = values[position] if isinstance(values, list) else values.get(str(position))
        if value is None:
            continue
        flag = status[position] if isinstance(status, list) else status.get(str(position), "")
        out["series_key"].append("|".join(combo[i] for i in others))
        out["time_period"].append(labels.get(combo[t], combo[t]))
        out["obs_value"].append(str(value))
        out["attribute_values"].append(str(flag or "").replace("|", "/"))
        out["source_row_number"].append(position)
    count = len(out["obs_value"])
    if not count:
        raise ValueError("no values")
    fixed = {
        "entity": base["entity"],
        "dataflow": data.get("label", ""),
        "series_dimensions": ",".join(ids[i] for i in others),
        "attribute_names": "status",
        "raw_sha256": base["raw_sha256"],
        "source_version": base["source_version"],
        "ingested_at": base["ingested_at"],
    }
    for name, value in fixed.items():
        out[name] = [value] * count
    return pa.Table.from_pydict(out, schema=bulk_bronze.arrow_schema(COLUMNS))


def decode_codes(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    data, ids, codes, time_dim = parse(body)
    base = bulk_bronze.provenance(receipt)
    out: dict[str, list[Any]] = {name: [] for name, _ in CODE_COLUMNS}
    row = 0
    for dim, ordered in zip(ids, codes, strict=True):
        info = data["dimension"][dim]
        labels = info["category"].get("label") or {}
        for code in ordered:
            out["dimension"].append(dim)
            out["dimension_label"].append(info.get("label", dim))
            out["code"].append(code)
            out["label"].append(labels.get(code, code))
            out["is_time"].append("true" if dim == time_dim else "false")
            out["source_row_number"].append(row)
            row += 1
    for name in ("entity", "raw_sha256", "source_version", "ingested_at"):
        out[name] = [base[name]] * row
    return pa.Table.from_pydict(out, schema=bulk_bronze.arrow_schema(CODE_COLUMNS))


def specs(landing_root: str) -> list[dict[str, str]]:
    """One spec per entity folder in Landing (tables are discovered by the extractor, not declared)."""
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    root = f"{landing_root}/{SOURCE}/snapshot/data/"
    found = set()
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=root, Delimiter="/"):
        found |= {p["Prefix"][len(root) :].strip("/") for p in page.get("CommonPrefixes", [])}
    return [{"entity": e} for e in sorted(found)]


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: about forty tables of at most a few MB."""
    entities = specs(landing_root)
    lake = "02_bronze" if namespace == "bronze" else namespace
    status = bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "observations",
        landing_root=landing_root,
        lake_root=lake,
        specs=entities,
    )
    return status or bulk_bronze.load(
        SOURCE,
        (namespace, CODES_ID[1]),
        (namespace, REJECTED_ID[1]),
        CODE_COLUMNS,
        decode_codes,
        "codes",
        landing_root=landing_root,
        lake_root=lake,
        specs=entities,
    )
