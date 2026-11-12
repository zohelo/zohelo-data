#!/usr/bin/env python3
"""NFZ contracts bronze.load: newest Landing copy of every year and branch → bronze.nfz_contracts_agreements.

Each Landing file is a ZIP of the exact pages of ``/agreements?year=<y>&branch=<nn>`` of the NFZ contracts API
(ingestion/to_landing/nfz_contracts/). One Bronze row per agreement record: its id and every attribute as the
exact JSON text (numbers as published, null as SQL NULL), with the page member and the record's position. A file
rejects (bronze.nfz_contracts_rejected_files) when a member is not an agreement list page, a record has an
attribute other than the known ones or lacks one, a record's year or branch differs from the entity's, or the
pages do not hold ``meta.count`` agreements with distinct ids. Entities are the folders found in Landing.
"""

from __future__ import annotations

import io
import json
from typing import Any
import zipfile

import pyarrow as pa

from ingestion.common import bulk_bronze
from ingestion.common.r2 import req, s3
from ingestion.to_landing.nfz_contracts import extract

SOURCE = "nfz_contracts"
TABLE = "nfz_contracts_agreements"
REJECTED = "nfz_contracts_rejected_files"
ATTRIBUTES = [  # the API's attribute → Bronze column
    ("code", "code"),
    ("technical-code", "technical_code"),
    ("origin-code", "origin_code"),
    ("service-type", "service_type"),
    ("service-name", "service_name"),
    ("amount", "amount_text"),
    ("updated-at", "updated_at"),
    ("provider-code", "provider_code"),
    ("provider-nip", "provider_nip"),
    ("provider-regon", "provider_regon"),
    ("provider-registry-number", "provider_registry_number"),
    ("provider-name", "provider_name"),
    ("provider-place", "provider_place"),
    ("year", "year_text"),
    ("branch", "branch"),
]
COLUMNS = [
    ("entity", "VARCHAR"),
    ("agreement_id", "VARCHAR"),
    *[(column, "VARCHAR") for _, column in ATTRIBUTES],
    *bulk_bronze.PROVENANCE,
    ("member", "VARCHAR"),  # ZIP member = page, p<n>.json
    ("source_row_number", "BIGINT"),  # position of the record in the entity (pages in order), from 1
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def text(value: Any) -> str | None:
    """A JSON value as text: strings as they are, numbers as their JSON text, null as None."""
    return value if value is None or isinstance(value, str) else json.dumps(value)


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    _, year, branch = receipt["entity"].split("_")
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        members = sorted(archive.namelist(), key=lambda m: int(m[1:-5]) if m[:1] == "p" else -1)
        if [m for m in members if not (m.startswith("p") and m.endswith(".json") and m[1:-5].isdigit())]:
            raise ValueError(f"unknown members {members[:5]}")
        pages = [(m, json.loads(archive.read(m), parse_float=str, parse_int=str)) for m in members]
    columns: dict[str, list[Any]] = {
        n: [] for n, _ in COLUMNS if n not in ("entity", "raw_sha256", "source_version", "ingested_at")
    }
    counts = set()
    for member, page in pages:
        counts.add(str(page["meta"]["count"]))
        for record in (page.get("data") or {}).get("agreements") or []:
            attributes = record["attributes"]
            if record.get("type") != "agreement" or set(attributes) != {a for a, _ in ATTRIBUTES}:
                raise ValueError(f"{member}: record {record.get('id')}: attributes {sorted(attributes)}")
            if text(attributes["year"]) != year or attributes["branch"] != branch:
                raise ValueError(f"{member}: record {record['id']} is {attributes['year']}/{attributes['branch']}")
            columns["agreement_id"].append(record["id"])
            for name, column in ATTRIBUTES:
                columns[column].append(text(attributes[name]))
            columns["member"].append(member)
            columns["source_row_number"].append(len(columns["member"]))
    count = len(columns["agreement_id"])
    if counts != {str(count)} or len(set(columns["agreement_id"])) != count:
        raise ValueError(f"{count} records ({len(set(columns['agreement_id']))} ids), meta.count {sorted(counts)}")
    base = bulk_bronze.provenance(receipt)
    data = {n: [base[n]] * count for n in ("entity", "raw_sha256", "source_version", "ingested_at")}
    data.update(columns)
    return pa.Table.from_pydict({n: data[n] for n, _ in COLUMNS}, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: a few hundred ZIPs of under 1 MB each."""
    held = extract.landed(s3(), req("STORAGE_LANDING_BUCKET_NAME"), landing_root)
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE),
        (namespace, REJECTED),
        COLUMNS,
        decode,
        "agreements",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=[{"entity": e} for e in sorted(held)],
    )
