"""Eurostat structure metadata → bronze.eurostat_structure_items.

Landing ``metadata`` holds SDMX 2.1 structure messages (``source_kind``
eurostat_structure_sdmx, ``*.structure.xml``): the bulk messages of all dataflows, data
structures, code lists and concept schemes (entities ESTAT_ALL_*, to_landing/eurostat/
structure.py), and legacy per-dataset messages with all descendants (not loaded: the
bulk messages supersede them). ``dataset_id`` is
the Landing entity (dataset code or ESTAT_ALL_*); ``artefact_id`` identifies the
artefact. One Bronze row per artefact or item, as published:

  artefact           dataflow | data_structure | dimension | time_dimension | attribute |
                     measure | codelist | code | concept_scheme | concept
  agency_id, artefact_id, artefact_version   the maintainable artefact the row belongs to
  item_id, parent_id                         code / concept / component id and its parent
  position                                   dimension position
  references_json    {"ConceptIdentity": {...Ref attributes}, "Enumeration": {...}, ...}
  names_json, descriptions_json              {"en": ..., "de": ..., "fr": ...}
  annotations_json   [{"type", "title", "text": {lang: ...}, "url"}]

Code lists repeat across datasets (GEO, UNIT …); Silver keeps one copy per
(agency, code list, version). One Parquet file per Landing file, partitioned by dataset;
the file list is the checkpoint; rejections go to bronze.eurostat_structure_rejected_files.
Other metadata files (ESMS reference metadata ZIPs, inventory TSVs) are not loaded here.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from hashlib import sha256
import json
import os
from pathlib import Path
import tempfile
from typing import Any
import xml.etree.ElementTree as ET

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import archive_key, catalog, get_with_backoff, raw_chunks, req, s3

METADATA = "01_landing/eurostat/snapshot/metadata/"
TABLE_ID = ("bronze", "eurostat_structure_items")
BULK_PREFIX = "ESTAT_ALL_"
REJECTED_ID = ("bronze", "eurostat_structure_rejected_files")
COLUMNS = [
    ("dataset_id", "VARCHAR"),
    ("artefact", "VARCHAR"),
    ("agency_id", "VARCHAR"),
    ("artefact_id", "VARCHAR"),
    ("artefact_version", "VARCHAR"),
    ("item_id", "VARCHAR"),
    ("parent_id", "VARCHAR"),
    ("position", "VARCHAR"),
    ("references_json", "VARCHAR"),
    ("names_json", "VARCHAR"),
    ("descriptions_json", "VARCHAR"),
    ("annotations_json", "VARCHAR"),
    ("raw_sha256", "VARCHAR"),
    ("source_version", "VARCHAR"),
    ("ingested_at", "TIMESTAMPTZ"),
    ("source_row_number", "BIGINT"),
]
XML_LANG = "{http://www.w3.org/XML/1998/namespace}lang"
MAINTAINABLE = {
    "Dataflow": "dataflow",
    "DataStructure": "data_structure",
    "Codelist": "codelist",
    "ConceptScheme": "concept_scheme",
}
ITEMS = {
    "Code": "code",
    "Concept": "concept",
    "Dimension": "dimension",
    "TimeDimension": "time_dimension",
    "Attribute": "attribute",
    "PrimaryMeasure": "measure",
}


def local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def texts(element: ET.Element, name: str) -> str | None:
    found = {child.get(XML_LANG, ""): (child.text or "") for child in element if local(child.tag) == name}
    return json.dumps(found, ensure_ascii=False, sort_keys=True) if found else None


def annotations(element: ET.Element) -> str | None:
    holder = next((c for c in element if local(c.tag) == "Annotations"), None)
    if holder is None:
        return None
    out = []
    for annotation in holder:
        entry: dict[str, Any] = {}
        for part in annotation:
            name = local(part.tag)
            if name == "AnnotationText":
                entry.setdefault("text", {})[part.get(XML_LANG, "")] = part.text or ""
            else:
                entry[
                    {"AnnotationType": "type", "AnnotationTitle": "title", "AnnotationURL": "url"}.get(name, name)
                ] = part.text or ""
        out.append(entry)
    return json.dumps(out, ensure_ascii=False)


def references(element: ET.Element) -> str | None:
    """Ref elements below the component (concept identity, enumeration, …), keyed by their wrapper."""
    found: dict[str, Any] = {}
    for wrapper in element.iter():
        for child in wrapper:
            if local(child.tag) == "Ref" and child.attrib:
                found[local(wrapper.tag)] = dict(child.attrib)
    return json.dumps(found, sort_keys=True) if found else None


def parent_of(element: ET.Element) -> str | None:
    for child in element:
        if local(child.tag) == "Parent":
            ref = next((r for r in child if local(r.tag) == "Ref"), None)
            return ref.get("id") if ref is not None else (child.text or None)
    return None


def rows_for(receipt: dict[str, Any], body: bytes) -> list[dict[str, Any]]:
    root = ET.fromstring(body)
    if local(root.tag) != "Structure":
        raise ValueError(f"not an SDMX structure message: {local(root.tag)}")
    ingested = datetime.fromisoformat(str(receipt["ingested_at"]).replace("Z", "+00:00"))
    if ingested.tzinfo is None:
        ingested = ingested.replace(tzinfo=UTC)
    base = {
        "dataset_id": receipt["dataset_code"],
        "raw_sha256": receipt["sha256"],
        "source_version": receipt.get("eurostat_version") or receipt.get("version_token"),
        "ingested_at": ingested,
    }
    rows: list[dict[str, Any]] = []

    def add(element: ET.Element, artefact: str, owner: ET.Element, item: bool) -> None:
        rows.append(
            {
                **base,
                "artefact": artefact,
                "agency_id": owner.get("agencyID"),
                "artefact_id": owner.get("id"),
                "artefact_version": owner.get("version"),
                "item_id": element.get("id") if item else None,
                "parent_id": parent_of(element) if item else None,
                "position": element.get("position") if item else None,
                "references_json": references(element)
                if artefact in ("dimension", "time_dimension", "attribute", "measure", "dataflow")
                else None,
                "names_json": texts(element, "Name"),
                "descriptions_json": texts(element, "Description"),
                "annotations_json": annotations(element),
                "source_row_number": len(rows) + 1,
            }
        )

    def walk(element: ET.Element, owner: ET.Element | None) -> None:
        for child in element:
            name = local(child.tag)
            if name in MAINTAINABLE:
                add(child, MAINTAINABLE[name], child, item=False)
                walk(child, child)
            elif name in ITEMS and owner is not None:
                add(child, ITEMS[name], owner, item=True)
                if name == "Code" or name == "Concept":
                    continue
                walk(child, owner)
            elif name not in ("Annotations", "Name", "Description", "Header"):
                walk(child, owner)

    walk(root, None)
    if not any(r["item_id"] is None for r in rows):
        raise ValueError("structure message without a maintainable artefact")
    return rows


def arrow_schema() -> pa.Schema:
    kinds = {"VARCHAR": pa.string(), "BIGINT": pa.int64(), "TIMESTAMPTZ": pa.timestamp("us", tz="UTC")}
    return pa.schema([(name, kinds[kind]) for name, kind in COLUMNS])


def load(budget_minutes: float = 120) -> dict[str, Any]:
    started = datetime.now(UTC)
    client, cat = s3(), bf.retry(catalog, "catalog")
    lake, landing = req("R2_LAKEHOUSE_BUCKET"), req("R2_LANDING_BUCKET")
    table = bf.retry(lambda: bf.ensure_table(cat, TABLE_ID, COLUMNS, "dataset_id"), "table")
    done = bf.loaded_hashes(table) | bf.rejected_hashes(cat, REJECTED_ID)
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=landing, Prefix=METADATA):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".receipt.json")]
    with ThreadPoolExecutor(max_workers=16) as pool:
        receipts = list(pool.map(lambda k: json.loads(get_with_backoff(client, landing, k)), keys))
    # Only the bulk messages: the legacy per-dataset messages repeat the same artefacts
    # (15 GB, half the datasets) and are superseded by them.
    todo = [
        r
        for r in receipts
        if r.get("source_kind") == "eurostat_structure_sdmx"
        and str(r.get("dataset_code", "")).startswith(BULK_PREFIX)
        and r["sha256"] not in done
    ]

    def fetch(receipt: dict[str, Any]) -> tuple[dict[str, Any], bytes]:
        key = receipt["object"]["key"]
        try:
            client.head_object(Bucket=landing, Key=key)
        except client.exceptions.ClientError:
            key = archive_key("eurostat", receipt["sha256"], key)
        body = b"".join(raw_chunks(client, landing, key))
        if sha256(body).hexdigest() != receipt["sha256"]:
            raise RuntimeError(f"Eurostat structure bytes do not match their receipt: {key}")
        return receipt, body

    uris, rejected, count, files, stopped = [], [], 0, 0, False
    with (
        tempfile.TemporaryDirectory(prefix="zohelo-eurostat-structure-") as temporary,
        ThreadPoolExecutor(max_workers=8) as pool,
    ):
        # One message at a time: the code list message alone is ~175 MB of XML.
        chunks = ([r] for r in todo)
        for receipt, body in (pair for chunk in chunks for pair in pool.map(fetch, chunk)):
            if (datetime.now(UTC) - started).total_seconds() > budget_minutes * 60:
                stopped = True
                break
            try:
                rows = rows_for(receipt, body)
            except (ET.ParseError, ValueError, KeyError, TypeError) as exc:
                rejected.append(
                    {
                        "entity": receipt.get("dataset_code"),
                        "raw_sha256": receipt["sha256"],
                        "landing_key": receipt["object"]["key"],
                        "error": f"{type(exc).__name__}: {exc}"[:1000],
                        "code_sha": os.environ.get("GITHUB_SHA", ""),
                    }
                )
                continue
            path = Path(temporary) / f"{receipt['sha256']}.parquet"
            pq.write_table(pa.Table.from_pylist(rows, schema=arrow_schema()), path, compression="zstd")
            out_key = f"02_bronze/eurostat/structure_items/{receipt['dataset_code']}/{receipt['sha256']}.parquet"
            uris.append(bf.upload(client, lake, out_key, path, receipt["sha256"]))
            path.unlink()
            count += len(rows)
            files += 1
            if len(uris) >= 200:
                bf.commit(cat, TABLE_ID, uris, ["dataset_id", "raw_sha256"], "eurostat-structure-bronze")
                uris = []
        if uris:
            bf.commit(cat, TABLE_ID, uris, ["dataset_id", "raw_sha256"], "eurostat-structure-bronze")
    bf.record_rejections(cat, REJECTED_ID, rejected)
    return {
        "structure_files_pending": len(todo),
        "loaded_files": files,
        "rows": count,
        "rejected_files": len(rejected),
        "stopped_on_budget": stopped,
    }
