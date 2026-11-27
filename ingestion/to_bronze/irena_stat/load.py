#!/usr/bin/env python3
"""IRENASTAT bronze.load: newest Landing copy of each table → bronze.irena_stat_electricity.

The Landing file (ingestion/to_landing/irena_stat/) is a ZIP of exact PxWeb answers: ``metadata.json`` (the table's
variables) and ``data_<nnn>.json``, JSON-stat 2.0 answers for parts of the countries. One Bronze row per cell with a
value or a status: country, technology, data type, grid connection and year as published (the PxWeb value code and
its label; IRENA's technology, data type and year codes are positions in the release, ``0`` … ``25``, so Silver keys
on the labels), the value as its JSON text, the cell status. A cell without a value whose status is ``..`` (PxWeb's
"no data" sign) is not a record and is not stored; a cell without a value and with another status, or without a
status, is stored with value null.

A file is rejected into bronze.irena_stat_rejected_files (never guessed) when: a member is not ``metadata.json`` or
``data_<nnn>.json``; an answer is not a JSON-stat 2.0 dataset; its variables are not exactly DIMENSIONS in that order;
its value count does not match its sizes; a value is not a number; an answer's codes or labels differ from the
metadata's; or the parts do not hold every country of the metadata exactly once.
"""

from __future__ import annotations

import io
from itertools import product
import json
import re
from typing import Any
import zipfile

import pyarrow as pa

from ingestion.common import bulk_bronze
from ingestion.to_landing.irena_stat import extract

SOURCE = "irena_stat"
TABLE = "irena_stat_electricity"
REJECTED = "irena_stat_rejected_files"
DIMENSIONS = ["Country/area", "Technology", "Data Type", "Grid connection", "Year"]
NAMES = ["country", "technology", "data_type", "grid", "year"]  # column stems, in DIMENSIONS order
VALUE_COLUMNS = ["table_id", *[f"{n}_{part}" for n in NAMES for part in ("code", "label")], "value", "status", "member"]
COLUMNS = [
    ("entity", "VARCHAR"),
    *[(c, "VARCHAR") for c in VALUE_COLUMNS],
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),  # position of the cell in its answer's value array
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)
MEMBER = re.compile(r"data_\d{3}\.json")
NO_DATA = ".."


def text(value: Any) -> str | None:
    """A published value as text: numbers as their JSON text, null as null."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"value is not a number: {str(value)[:80]}")
    return json.dumps(value)


def ordered_codes(dimension: dict[str, Any]) -> list[str]:
    index = dimension["category"]["index"]
    return list(index) if isinstance(index, list) else sorted(index, key=index.get)


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        names = archive.namelist()
        unknown = [n for n in names if n != "metadata.json" and not MEMBER.fullmatch(n)]
        if unknown or "metadata.json" not in names:
            raise ValueError(f"members unknown {unknown[:5]} or metadata.json missing")
        meta = json.loads(archive.read("metadata.json"))
        parts = {n: json.loads(archive.read(n)) for n in sorted(names) if n != "metadata.json"}
    variables = {v["code"]: dict(zip(v["values"], v["valueTexts"], strict=True)) for v in meta["variables"]}
    if list(variables) != DIMENSIONS:
        raise ValueError(f"variables {list(variables)}, expected {DIMENSIONS}")
    if not parts:
        raise ValueError("no data member")
    table_id = str(receipt.get("table_id", "")).removesuffix(".px")
    rows: list[dict[str, Any]] = []
    seen_areas: list[str] = []
    for name, data in parts.items():
        if data.get("class") != "dataset" or data.get("id") != DIMENSIONS or "value" not in data:
            raise ValueError(f"{name}: not a JSON-stat 2.0 dataset with variables {DIMENSIONS} ({data.get('id')})")
        codes = [ordered_codes(data["dimension"][d]) for d in DIMENSIONS]
        if [len(c) for c in codes] != list(data["size"]):
            raise ValueError(f"{name}: codes {[len(c) for c in codes]} do not match size {data['size']}")
        labels = []
        for d, ordered in zip(DIMENSIONS, codes, strict=True):
            label = data["dimension"][d]["category"].get("label") or {}
            for code in ordered:
                if code not in variables[d] or label.get(code, code) != variables[d][code]:
                    raise ValueError(f"{name}: {d} code {code!r} label {label.get(code)!r} not as in the metadata")
            labels.append(label)
        seen_areas += codes[0]
        values, status = data["value"], data.get("status") or {}
        cells = 1
        for c in codes:
            cells *= len(c)
        if isinstance(values, list) and len(values) != cells:
            raise ValueError(f"{name}: {len(values)} values for {cells} cells")
        for position, combo in enumerate(product(*codes)):
            value = values[position] if isinstance(values, list) else values.get(str(position))
            flag = status[position] if isinstance(status, list) else status.get(str(position))
            if value is None and flag == NO_DATA:
                continue
            row: dict[str, Any] = {"value": text(value), "status": flag or None, "member": name}
            for stem, code, label in zip(NAMES, combo, labels, strict=True):
                row[f"{stem}_code"] = code
                row[f"{stem}_label"] = label.get(code, code)
            row["source_row_number"] = position
            rows.append(row)
    expected = list(variables[DIMENSIONS[0]])
    if sorted(seen_areas) != sorted(expected):
        raise ValueError(
            f"parts hold {len(seen_areas)} areas ({len(set(seen_areas))} distinct), metadata {len(expected)}"
        )
    if not rows:
        raise ValueError("no cell with a value")
    base = bulk_bronze.provenance(receipt)
    data = {c: [base[c]] * len(rows) for c in ("entity", "raw_sha256", "source_version", "ingested_at")}
    data["table_id"] = [table_id] * len(rows)
    for column in (*VALUE_COLUMNS[1:], "source_row_number"):
        data[column] = [r.get(column) for r in rows]
    return pa.Table.from_pydict({n: data[n] for n, _ in COLUMNS}, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: one table release of a few MB."""
    del budget_minutes
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE),
        (namespace, REJECTED),
        COLUMNS,
        decode,
        "electricity",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=[{"entity": e} for e in extract.TABLES],
    )
