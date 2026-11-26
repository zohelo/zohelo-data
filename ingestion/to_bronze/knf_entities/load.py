#!/usr/bin/env python3
"""KNF supervised entities bronze.load: newest ESMA registers answer in Landing → bronze.knf_entities_esma_entries.

The file is the JSON answer of the ESMA registers Solr core esma_registers_upreg for the parent entries whose home
member state is Poland (see the source declaration): ``{"response": {"numFound": n, "start": 0, "numFoundExact": true,
"docs": [...]}}``, one document per register entry, with the fields named in FIELDS (the request's field list; no
addresses, websites or comments are requested). One Bronze row per document, every value as published text (a
multi-valued field joined with '|'; ae_dbId, a number in the JSON, as its decimal text). The layout is checked before
anything is loaded: another top-level shape, a field outside FIELDS, a document without one of REQUIRED, a start other
than 0, or numFound different from the documents returned (the slice outgrew the request's rows) rejects the file into
bronze.knf_entities_rejected_files. DuckDB reads the documents with a declared type for every field (no per-cell
Python). Code shared with other small bulk sources: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

import json
from pathlib import Path
import tempfile
from typing import Any

import duckdb
import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "knf_entities"
TABLE_ID = ("bronze", "knf_entities_esma_entries")
REJECTED_ID = ("bronze", "knf_entities_rejected_files")
# (Solr field, DuckDB type in the answer, Bronze column)
FIELDS: list[tuple[str, str, str]] = [
    ("id", "VARCHAR", "entry_id"),
    ("ae_dbId", "BIGINT", "esma_db_id"),
    ("ae_entityName", "VARCHAR", "entity_name"),
    ("ae_commercialName", "VARCHAR", "commercial_name"),
    ("ae_entityTypeCode", "VARCHAR", "entity_type_code"),
    ("ae_entityTypeLabel", "VARCHAR", "entity_type_label"),
    ("ae_lei", "VARCHAR", "lei"),
    ("ae_headOfficeLei", "VARCHAR", "head_office_lei"),
    ("ae_competentAuthority", "VARCHAR", "competent_authority"),
    ("ae_homeMemberState", "VARCHAR", "home_member_state"),
    ("ae_hostMemberStates", "VARCHAR[]", "host_member_states"),
    ("ae_officeType", "VARCHAR", "office_type"),
    ("ae_status", "VARCHAR", "status"),
    ("ae_authorisationNotificationDate", "VARCHAR", "authorisation_date"),
    ("ae_authorisationWithdrawalEndDate", "VARCHAR", "withdrawal_end_date"),
    ("ae_lastUpdate", "VARCHAR", "last_update"),
    ("ae_legalform", "VARCHAR", "legal_form"),
    ("ae_isSme", "VARCHAR", "is_sme"),
    ("no_of_funds", "VARCHAR", "funds_count"),
]
REQUIRED = {
    "id",
    "ae_dbId",
    "ae_entityName",
    "ae_entityTypeCode",
    "ae_officeType",
    "ae_status",
    "ae_authorisationNotificationDate",
    "ae_lastUpdate",
}
COLUMNS = [
    ("entity", "VARCHAR"),
    *[(column, "VARCHAR") for _, _, column in FIELDS],
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def check_layout(body: bytes) -> int:
    """Raise ValueError when the answer is not the expected layout; return the number of documents."""
    try:
        answer = json.loads(body)
    except ValueError as exc:
        raise ValueError(f"not JSON: {str(exc)[:120]}") from exc
    if not isinstance(answer, dict) or set(answer) != {"response"}:
        raise ValueError(f"unknown top-level keys: {sorted(answer) if isinstance(answer, dict) else type(answer)}")
    response = answer["response"]
    if set(response) - {"numFound", "start", "numFoundExact", "docs"} or not isinstance(response.get("docs"), list):
        raise ValueError(f"unknown response keys: {sorted(response)}")
    docs = response["docs"]
    if response.get("start") != 0 or response.get("numFound") != len(docs):
        raise ValueError(
            f"incomplete answer: start {response.get('start')}, numFound {response.get('numFound')}, "
            f"{len(docs)} documents returned (raise rows or add a page)"
        )
    known = {field for field, _, _ in FIELDS}
    extra = sorted({k for d in docs for k in d} - known)
    if extra:
        raise ValueError(f"unknown fields: {extra}")
    missing = sorted({k for d in docs for k in REQUIRED if k not in d})
    if missing:
        raise ValueError(f"documents without required fields: {missing}")
    if not docs:
        raise ValueError("no documents")
    return len(docs)


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    expected = check_layout(body)
    prov = bulk_bronze.provenance(receipt)
    doc_type = "STRUCT(" + ", ".join(f'"{field}" {kind}' for field, kind, _ in FIELDS) + ")"
    picked = ", ".join(
        (f"array_to_string(d.\"{field}\", '|')" if kind.endswith("[]") else f'cast(d."{field}" as varchar)')
        + f" as {column}"
        for field, kind, column in FIELDS
    )
    with tempfile.TemporaryDirectory(prefix="zohelo-knf-entities-") as temporary:
        path = Path(temporary) / "answer.json"
        path.write_bytes(body)
        con = duckdb.connect()
        con.execute("set threads = 1")
        try:
            table = con.sql(
                f"""
                with answer as (
                    select response.docs as docs
                    from read_json(?, format = 'unstructured', maximum_object_size = 268435456,
                                   columns = {{'response': 'STRUCT(docs {doc_type}[])'}})
                ),

                docs as (
                    select t.n, a.docs[t.n] as d
                    from answer as a, range(1, len(a.docs) + 1) as t (n)
                )
                select
                    ? as entity,
                    {picked},
                    ? as raw_sha256,
                    ? as source_version,
                    cast(? as timestamptz) as ingested_at,
                    cast(n as bigint) as source_row_number
                from docs
                order by n
                """,
                params=[str(path), prov["entity"], prov["raw_sha256"], prov["source_version"], prov["ingested_at"]],
            ).to_arrow_table()
        except duckdb.Error as exc:
            raise ValueError(f"JSON not readable with the declared types: {str(exc).splitlines()[0][:300]}") from exc
        finally:
            con.close()
    if table.num_rows != expected:
        raise ValueError(f"{table.num_rows} rows decoded from {expected} documents")
    return table.cast(ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: one JSON answer of about 0.4 MB."""
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "esma_entries",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
    )
