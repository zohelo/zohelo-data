#!/usr/bin/env python3
"""NFZ services bronze.load: newest Landing copy of every entity → two Bronze tables.

Each Landing file is a ZIP of exact API answers (ingestion/to_landing/nfz_services/):

- ``benefits_<catalog>`` (members ``benefits_p<n>.json``) → ``bronze.nfz_services_benefits``: one row per
  dictionary entry (catalog, code, name), page and position kept;
- ``<catalog>_<code>`` (``index.json`` and ``basic_<table id>_<pl|branch>_p<n>.json``) →
  ``bronze.nfz_services_basic_data``: one row per record of a basic-data answer (``record = 'value'``: the branch
  code, null for Poland, and the eleven values as the exact JSON number text, null where the API gives null), one
  ``'empty_table'`` row for a table the index lists whose answer has no record, and one ``'no_table'`` row when
  the index lists no basic-data table (the API's ``data: null`` or years without one), so every file is a row.

A member set that does not match the index, a record with an unknown key, an answer for another product or
table, or a split by hospital type rejects the file (bronze.nfz_services_rejected_files), never guessed.
"""

from __future__ import annotations

import io
import json
import re
from typing import Any
import zipfile

import pyarrow as pa

from ingestion.common import bulk_bronze
from ingestion.common.r2 import req, s3
from ingestion.to_landing.nfz_services import extract

SOURCE = "nfz_services"
BENEFITS = "nfz_services_benefits"
BASIC = "nfz_services_basic_data"
REJECTED = "nfz_services_rejected_files"
VALUES = [  # API field → Bronze column (text)
    ("number-of-patients", "number_of_patients"),
    ("number-of-hospitalizations", "number_of_hospitalizations"),
    ("ratio-of-rehospitalizations", "ratio_of_rehospitalizations"),
    ("percentage", "percentage"),
    ("percentage-of-sections", "percentage_of_sections"),
    ("duration-of-hospitalization-mediana", "duration_median"),
    ("duration-of-hospitalization-mode", "duration_mode"),
    ("average-value-of-hospitalization", "average_value_of_hospitalization"),
    ("average-value-of-hospitalization-points", "average_value_of_hospitalization_points"),
    ("average-value-of-drg", "average_value_of_drg"),
    ("average-value-of-drg-points", "average_value_of_drg_points"),
]
RECORD_KEYS = {"branch", "hospital-types", *(k for k, _ in VALUES)}
BENEFIT_COLUMNS = [
    ("entity", "VARCHAR"),
    ("catalog", "VARCHAR"),
    ("benefit_code", "VARCHAR"),
    ("benefit_name", "VARCHAR"),
    ("page", "BIGINT"),
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),  # position in the dictionary, from 1
]
BASIC_COLUMNS = [
    ("entity", "VARCHAR"),
    ("catalog", "VARCHAR"),
    ("benefit_code", "VARCHAR"),  # product code of the index answer
    ("benefit_name", "VARCHAR"),  # product name of the index answer
    ("record", "VARCHAR"),  # value | empty_table | no_table
    ("year", "VARCHAR"),
    ("period_from", "VARCHAR"),  # null for a whole-year table
    ("period_to", "VARCHAR"),
    ("table_id", "VARCHAR"),
    ("scope", "VARCHAR"),  # pl (branch=false) | branch (branch=true)
    ("branch", "VARCHAR"),  # NFZ regional branch code 01-16; null in the Poland answer
    *[(column, "VARCHAR") for _, column in VALUES],
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),  # position of the record in its answer, from 1
]
MEMBER = re.compile(r"basic_(?P<table>.+)_(?P<scope>pl|branch)_p(?P<page>[0-9]+)\.json")


def _text(value: Any) -> str | None:
    return None if value is None else str(value)


def members(body: bytes) -> dict[str, Any]:
    """The ZIP's answers by member name, parsed with numbers kept as their exact text."""
    try:
        with zipfile.ZipFile(io.BytesIO(body)) as archive:
            return {n: json.loads(archive.read(n), parse_float=str, parse_int=str) for n in archive.namelist()}
    except zipfile.BadZipFile as exc:
        raise ValueError(f"not a ZIP: {exc}") from exc


def _table(columns: list[tuple[str, str]], rows: list[dict[str, Any]], receipt: dict[str, Any]) -> pa.Table:
    base = bulk_bronze.provenance(receipt)
    data = {n: [r.get(n) for r in rows] for n, _ in columns}
    for n in ("entity", "raw_sha256", "source_version", "ingested_at"):
        data[n] = [base[n]] * len(rows)
    return pa.Table.from_pydict(data, schema=bulk_bronze.arrow_schema(columns))


def decode_benefits(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    catalog = receipt["entity"].removeprefix("benefits_")
    answers = members(body)
    rows, position = [], 0
    for page in range(1, len(answers) + 1):
        answer = answers.get(f"benefits_p{page}.json")
        if answer is None:
            raise ValueError(f"members {sorted(answers)}: benefits_p{page}.json missing")
        for item in answer.get("data") or []:
            if set(item) != {"code", "name"}:
                raise ValueError(f"dictionary entry with keys {sorted(item)}")
            position += 1
            rows.append(
                {
                    "catalog": catalog,
                    "benefit_code": item["code"],
                    "benefit_name": item["name"],
                    "page": page,
                    "source_row_number": position,
                }
            )
    count = int((answers["benefits_p1.json"].get("meta") or {}).get("count") or -1)
    if not rows or len(rows) != count:
        raise ValueError(f"{len(rows)} dictionary entries, meta.count {count}")
    return _table(BENEFIT_COLUMNS, rows, receipt)


def decode_basic(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    answers = members(body)
    index = answers.get("index.json")
    if index is None:
        raise ValueError("index.json missing")
    attributes = (index.get("data") or {}).get("attributes") or {}
    catalog = receipt["entity"].split("_", 1)[0]
    code, name = attributes.get("product-code"), attributes.get("product-name")
    if index.get("data") is not None and code != receipt.get("benefit_code"):
        raise ValueError(f"index for product {code!r}, receipt names {receipt.get('benefit_code')!r}")
    code = code or receipt.get("benefit_code")
    name = name or receipt.get("benefit_name")
    tables = {t[1]: t for t in extract.basic_tables(index)}
    found: dict[str, dict[str, list]] = {}
    for member, answer in answers.items():
        if member == "index.json":
            continue
        match = MEMBER.fullmatch(member)
        if not match or match["table"] not in tables:
            raise ValueError(f"member {member} is not a basic-data table of the index")
        found.setdefault(match["table"], {}).setdefault(match["scope"], []).append((int(match["page"]), answer))
    if set(found) != set(tables) or any(set(v) != {"pl", "branch"} for v in found.values()):
        raise ValueError(f"answers for tables {sorted(found)}, index lists {sorted(tables)}")
    base = {"catalog": catalog, "benefit_code": code, "benefit_name": name}
    rows: list[dict[str, Any]] = []
    for table_id, (year, _id, start, end) in sorted(tables.items(), key=lambda kv: (kv[1][0], kv[1][2] or "")):
        where = {**base, "year": str(year), "period_from": start, "period_to": end, "table_id": table_id}
        before = len(rows)
        for scope in ("pl", "branch"):
            number = 0
            for _page, answer in sorted(found[table_id][scope], key=lambda x: x[0]):
                data = answer.get("data") or {}
                head = data.get("attributes") or {}
                if str(data.get("id")) != table_id or head.get("code") != code:
                    raise ValueError(f"{table_id} {scope}: answer for {data.get('id')} / {head.get('code')}")
                for record in head.get("data") or []:
                    unknown = set(record) - RECORD_KEYS
                    if unknown:
                        raise ValueError(f"{table_id} {scope}: unknown keys {sorted(unknown)}")
                    if record.get("hospital-types") is not None:
                        raise ValueError(f"{table_id} {scope}: split by hospital type")
                    if (record.get("branch") is None) != (scope == "pl"):
                        raise ValueError(f"{table_id} {scope}: branch {record.get('branch')!r}")
                    number += 1
                    rows.append(
                        {
                            **where,
                            "record": "value",
                            "scope": scope,
                            "branch": record.get("branch"),
                            **{column: _text(record.get(key)) for key, column in VALUES},
                            "source_row_number": number,
                        }
                    )
        if len(rows) == before:
            rows.append({**where, "record": "empty_table"})
    if not tables:
        rows.append({**base, "record": "no_table"})
    return _table(BASIC_COLUMNS, rows, receipt)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: one small ZIP per entity (a few hundred kB at most)."""
    held = extract.landed(s3(), req("STORAGE_LANDING_BUCKET_NAME"), landing_root)
    lake_root = "02_bronze" if namespace == "bronze" else namespace
    dictionaries = sorted(e for e in held if e.startswith("benefits_"))
    benefits = sorted(e for e in held if not e.startswith("benefits_"))
    status = 0
    for table, columns, decode, entities in (
        (BENEFITS, BENEFIT_COLUMNS, decode_benefits, dictionaries),
        (BASIC, BASIC_COLUMNS, decode_basic, benefits),
    ):
        status |= bulk_bronze.load(
            SOURCE,
            (namespace, table),
            (namespace, REJECTED),
            columns,
            decode,
            table,
            landing_root=landing_root,
            lake_root=lake_root,
            specs=[{"entity": e} for e in entities],
        )
    return status
