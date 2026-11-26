#!/usr/bin/env python3
"""GUS STRATEG bronze.load: newest Landing copy of the SOR selection → bronze.gus_strateg_records.

The Landing file (ingestion/to_landing/gus_strateg/) is a ZIP of exact API answers: the SOR indicator list in Polish
and English, ``indicator_<id>.json`` per indicator and ``data_<id>_<dimension>_level<L>.json`` per level read. Two
kinds of Bronze rows, values as published text:

- ``indicator``: one per indicator dimension of the SOR list (names and unit in Polish and English, the objective,
  the indicator's territorial levels and years as JSON text, and the strategy's base, intermediate and target
  values as their JSON text: targets are not observations);
- ``value``: one per year x territory code of a ``real_values`` answer (level, period and territory as published).

An answer that is not the documented shape (a list of objectives with ``indicators``, an indicator object, a
``real_values`` object of objects), a data file of an indicator the list does not name, an indicator without its
``indicator_<id>.json``, or a member name outside these patterns rejects the file into
bronze.gus_strateg_rejected_files (never guessed).
"""

from __future__ import annotations

import io
import json
import re
from typing import Any
import zipfile

import pyarrow as pa

from ingestion.common import bulk_bronze
from ingestion.to_landing.gus_strateg import extract

SOURCE = "gus_strateg"
TABLE = "gus_strateg_records"
REJECTED = "gus_strateg_rejected_files"
VALUE_COLUMNS = [
    "record_kind",
    "indicator_id",
    "dimension_id",
    "level_id",
    "period",
    "territory_code",
    "value",
    "indicator_name_pl",
    "indicator_name_en",
    "unit_pl",
    "unit_en",
    "objective_pl",
    "territory_levels_json",
    "years_json",
    "base_value_json",
    "intermediate_values_json",
    "target_value_json",
    "member",
]
COLUMNS = [
    ("entity", "VARCHAR"),
    *[(c, "VARCHAR") for c in VALUE_COLUMNS],
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),  # position of the row in its member, from 1
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)
STRATEGY_PL = f"strategy_{extract.STRATEGY}_pl.json"
STRATEGY_EN = f"strategy_{extract.STRATEGY}_en.json"
INDICATOR = re.compile(r"indicator_(\d+)\.json")
DATA = re.compile(r"data_(\d+)_(\d+)_level(\w+)\.json")
LIST_KEYS = {
    "base_value",
    "dimension_id",
    "indicator_id",
    "indicator_name",
    "intermediate_values",
    "target_value",
    "territory_code",
    "territory_name",
    "unit",
}


def text(value: Any) -> str | None:
    """A published value as text: strings unchanged, numbers as their JSON text, objects as JSON, null as null."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def strategy_rows(answer: Any, member: str) -> dict[tuple[str, str], dict[str, Any]]:
    """(indicator, dimension) → the list item with its objective; a pair listed twice fails."""
    if not isinstance(answer, list):
        raise ValueError(f"{member}: not a list")
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for group in answer:
        if not isinstance(group, dict) or not isinstance(group.get("indicators"), list):
            raise ValueError(f"{member}: objective without indicators: {str(group)[:200]}")
        objective = " | ".join(str(group[k]) for k in sorted(group) if k.startswith("objective"))
        for item in group["indicators"]:
            if not isinstance(item, dict) or set(item) - LIST_KEYS:
                raise ValueError(f"{member}: unexpected indicator item {str(item)[:200]}")
            key = (text(item["indicator_id"]), text(item["dimension_id"]))
            if key in out:
                raise ValueError(f"{member}: indicator dimension {key} listed twice")
            out[key] = {**item, "objective": objective}
    return out


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        members = {name: json.loads(archive.read(name)) for name in archive.namelist()}
    for name in members:
        if name not in (STRATEGY_PL, STRATEGY_EN) and not INDICATOR.fullmatch(name) and not DATA.fullmatch(name):
            raise ValueError(f"unexpected member {name}")
    listed = strategy_rows(members.get(STRATEGY_PL), STRATEGY_PL)
    english = strategy_rows(members.get(STRATEGY_EN), STRATEGY_EN)
    if not listed:
        raise ValueError("no indicators in the SOR list")
    rows: list[dict[str, Any]] = []
    for n, ((indicator, dimension), item) in enumerate(listed.items(), start=1):
        info = members.get(f"indicator_{indicator}.json")
        if not isinstance(info, dict):
            raise ValueError(f"indicator {indicator}: no indicator answer")
        en = english.get((indicator, dimension), {})
        rows.append(
            {
                "record_kind": "indicator",
                "indicator_id": indicator,
                "dimension_id": dimension,
                "territory_code": text(item.get("territory_code")),
                "indicator_name_pl": text(item.get("indicator_name")),
                "indicator_name_en": text(en.get("indicator_name")),
                "unit_pl": text(item.get("unit")),
                "unit_en": text(en.get("unit")),
                "objective_pl": item["objective"] or None,
                "territory_levels_json": text(info.get("territory_levels")),
                "years_json": text(info.get("years")),
                "base_value_json": text(item.get("base_value")),
                "intermediate_values_json": text(item.get("intermediate_values")),
                "target_value_json": text(item.get("target_value")),
                "member": STRATEGY_PL,
                "source_row_number": n,
            }
        )
    for name, answer in members.items():
        match = DATA.fullmatch(name)
        if not match:
            continue
        indicator, dimension, level = match.groups()
        if (indicator, dimension) not in listed:
            raise ValueError(f"{name}: indicator dimension not in the SOR list")
        real = answer.get("real_values") if isinstance(answer, dict) else None
        if not isinstance(real, dict) or set(answer) != {"real_values"}:
            raise ValueError(f"{name}: not a real_values answer: {str(answer)[:200]}")
        n = 0
        for period, by_territory in real.items():
            if not isinstance(by_territory, dict):
                raise ValueError(f"{name}: period {period} is not an object")
            for territory, value in by_territory.items():
                n += 1
                rows.append(
                    {
                        "record_kind": "value",
                        "indicator_id": indicator,
                        "dimension_id": dimension,
                        "level_id": level,
                        "period": period,
                        "territory_code": territory,
                        "value": text(value),
                        "member": name,
                        "source_row_number": n,
                    }
                )
    base = bulk_bronze.provenance(receipt)
    data = {c: [base[c]] * len(rows) for c in ("entity", "raw_sha256", "source_version", "ingested_at")}
    for column in (*VALUE_COLUMNS, "source_row_number"):
        data[column] = [r.get(column) for r in rows]
    return pa.Table.from_pydict({n: data[n] for n, _ in COLUMNS}, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: one file of under 1 MB."""
    del budget_minutes
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE),
        (namespace, REJECTED),
        COLUMNS,
        decode,
        "records",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=[{"entity": extract.ENTITY}],
    )
