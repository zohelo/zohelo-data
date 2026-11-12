#!/usr/bin/env python3
"""filings.xbrl.org bronze.load: the newest Landing copy of the filing list and of every facts document.

bronze.xbrl_filings_filings  one row per filing of the list (JSON:API ``data``), every attribute as source text,
                             with the issuer (``included`` entity: LEI and name) the filing points to. A list
                             with an attribute this loader does not know, without filings, or whose ``meta.count``
                             differs from its rows (it did not fit one page) is rejected.
bronze.xbrl_filings_facts    one row per fact of the selected concepts of one filing's reduced xBRL-JSON document
                             (ingestion/to_landing/xbrl_filings/): value and decimals as text, the core dimensions
                             (concept, entity, period, unit, language) as columns and every other dimension as
                             JSON text, so dimensional facts stay apart from the entity-level ones. A document
                             without a fact of the selected concepts is rejected with that reason.
Rejections go to bronze.xbrl_filings_rejected_files. Shared code: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze
from ingestion.to_landing import xbrl_filings as landing

SOURCE = "xbrl_filings"
FILINGS_ID = ("bronze", "xbrl_filings_filings")
FACTS_ID = ("bronze", "xbrl_filings_facts")
REJECTED_ID = ("bronze", "xbrl_filings_rejected_files")
ATTRIBUTES = [
    *["fxo_id", "period_end", "country", "date_added", "processed", "published", "sha256"],
    *["viewer_url", "package_url", "report_url", "json_url"],
    *["error_count", "warning_count", "inconsistency_count"],
]
FILING_COLUMNS = [
    ("entity", "VARCHAR"),
    ("filing_id", "VARCHAR"),
    *[(name, "VARCHAR") for name in ATTRIBUTES],
    ("entity_id", "VARCHAR"),
    ("entity_identifier", "VARCHAR"),
    ("entity_name", "VARCHAR"),
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
CORE_DIMENSIONS = ("concept", "entity", "period", "unit", "language")
FACT_COLUMNS = [
    ("entity", "VARCHAR"),
    ("fxo_id", "VARCHAR"),
    ("fact_id", "VARCHAR"),
    ("concept", "VARCHAR"),
    ("value", "VARCHAR"),
    ("decimals", "VARCHAR"),
    ("period", "VARCHAR"),
    ("unit", "VARCHAR"),
    ("entity_scheme_id", "VARCHAR"),
    ("language", "VARCHAR"),
    ("extra_dimensions", "VARCHAR"),
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]


def text(value: Any) -> str | None:
    """A JSON scalar as source text (numbers and booleans in their JSON spelling); null stays null."""
    if value is None:
        return None
    return value if isinstance(value, str) else json.dumps(value)


def decode_filings(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    document = json.loads(body.decode("utf-8"))
    data = document["data"]
    if not data:
        raise ValueError("no filings in the list")
    if int(document["meta"]["count"]) != len(data):
        raise ValueError(f"the list holds {len(data)} of {document['meta']['count']} filings: it needs another page")
    issuers = {item["id"]: item["attributes"] for item in document.get("included", []) if item["type"] == "entity"}
    prov = bulk_bronze.provenance(receipt)
    rows = []
    for n, item in enumerate(data, start=1):
        attributes = item["attributes"]
        if item["type"] != "filing" or set(attributes) - set(ATTRIBUTES):
            raise ValueError(f"unknown filing layout: {item['type']} {sorted(set(attributes) - set(ATTRIBUTES))}")
        issuer_id = (item["relationships"]["entity"].get("data") or {}).get("id")
        issuer = issuers.get(issuer_id, {})
        rows.append(
            {
                **prov,
                "filing_id": text(item["id"]),
                **{name: text(attributes.get(name)) for name in ATTRIBUTES},
                "entity_id": text(issuer_id),
                "entity_identifier": text(issuer.get("identifier")),
                "entity_name": text(issuer.get("name")),
                "source_row_number": n,
            }
        )
    return pa.Table.from_pylist(rows, schema=bulk_bronze.arrow_schema(FILING_COLUMNS))


def decode_facts(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    document = json.loads(body.decode("utf-8"))
    if receipt["entity"] != landing.facts_entity(document["fxo_id"]):
        raise ValueError(f"document of {document['fxo_id']} under entity {receipt['entity']}")
    prov = bulk_bronze.provenance(receipt)
    rows = []
    for n, (fact_id, fact) in enumerate(document["facts"].items(), start=1):
        dimensions = fact["dimensions"]
        if dimensions["concept"] not in document["concepts"]:
            raise ValueError(f"fact {fact_id} of a concept outside the selection: {dimensions['concept']}")
        rows.append(
            {
                **prov,
                "fxo_id": document["fxo_id"],
                "fact_id": fact_id,
                "concept": dimensions["concept"],
                "value": text(fact.get("value")),
                "decimals": text(fact.get("decimals")),
                "period": text(dimensions.get("period")),
                "unit": text(dimensions.get("unit")),
                "entity_scheme_id": text(dimensions.get("entity")),
                "language": text(dimensions.get("language")),
                "extra_dimensions": json.dumps(
                    {k: v for k, v in dimensions.items() if k not in CORE_DIMENSIONS}, sort_keys=True
                ),
                "source_row_number": n,
            }
        )
    if not rows:
        raise ValueError("no facts of the selected concepts")
    return pa.Table.from_pylist(rows, schema=bulk_bronze.arrow_schema(FACT_COLUMNS))


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: one list of about 1 MB and a few KB per filing."""
    from ingestion.common.r2 import req, s3

    held = sorted(landing.held_versions(s3(), req("STORAGE_LANDING_BUCKET_NAME"), landing_root))
    lake_root = "02_bronze" if namespace == "bronze" else namespace
    rejected_id = (namespace, REJECTED_ID[1])
    status = bulk_bronze.load(
        SOURCE,
        (namespace, FILINGS_ID[1]),
        rejected_id,
        FILING_COLUMNS,
        decode_filings,
        "filings",
        landing_root=landing_root,
        lake_root=lake_root,
        specs=[{"entity": entity} for entity in held if entity == landing.LIST_ENTITY],
    )
    return status | bulk_bronze.load(
        SOURCE,
        (namespace, FACTS_ID[1]),
        rejected_id,
        FACT_COLUMNS,
        decode_facts,
        "facts",
        landing_root=landing_root,
        lake_root=lake_root,
        specs=[{"entity": entity} for entity in held if entity.startswith("facts_")],
    )
