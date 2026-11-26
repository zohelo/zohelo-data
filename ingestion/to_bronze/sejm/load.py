#!/usr/bin/env python3
"""Sejm API bronze.load: newest Landing copy of every entity → one Bronze table per kind of answer.

One row per item of the answer's JSON list, values as source text (numbers, booleans and dates as printed in the
JSON; lists of strings joined with '|'), with the term, entity and provenance columns. DuckDB reads the JSON (no
per-cell Python). A file with an item key outside its table's KNOWN set, or not a JSON list, is rejected into
bronze.sejm_rejected_files (an unknown layout is never guessed). Not loaded, though landed: the proceedings'
agenda, schedule and current-affairs HTML, the votings' links and per-option results of list votes
(votingOptions), the prints' attachment file names. Shared code: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

from pathlib import Path
import re
import tempfile
from typing import Any

import duckdb
import pyarrow as pa

from ingestion.common import bulk_bronze
from ingestion.to_landing import sejm as landing

SOURCE = "sejm"
REJECTED = "sejm_rejected_files"


def _s(path: str) -> str:
    return f"i ->> '$.{path}'"


def _joined(path: str) -> str:
    return f"array_to_string(cast(i -> '$.{path}' as varchar[]), '|')"


# table → (entity pattern, item keys the API sends, Bronze column → DuckDB expression over the item i)
TABLES: dict[str, tuple[str, set[str], dict[str, str]]] = {
    "sejm_terms": (
        r"terms",
        {"current", "from", "num", "prints", "to"},
        {
            "term_num": _s("num"),
            "date_from": _s("from"),
            "date_to": _s("to"),
            "is_current": _s("current"),
            "prints_count": _s("prints.count"),
            "prints_last_changed": _s("prints.lastChanged"),
        },
    ),
    "sejm_proceedings": (
        r"term\d+_proceedings",
        {"agenda", "current", "currentAffairs", "dates", "number", "schedule", "title", "votings"},
        {"sitting_number": _s("number"), "title": _s("title"), "is_current": _s("current"), "dates": _joined("dates")},
    ),
    "sejm_voting_days": (
        r"term\d+_voting_days",
        {"date", "proceeding", "votingsNum"},
        {"sitting_number": _s("proceeding"), "voting_date": _s("date"), "votings_num": _s("votingsNum")},
    ),
    "sejm_votings": (
        r"term\d+_votings_\d+",
        {
            "abstain",
            "againstAll",
            "date",
            "description",
            "kind",
            "links",
            "majorityType",
            "majorityVotes",
            "no",
            "notParticipating",
            "present",
            "sitting",
            "sittingDay",
            "term",
            "title",
            "topic",
            "totalVoted",
            "votingNumber",
            "votingOptions",
            "yes",
        },
        {
            "item_term": _s("term"),
            "sitting_number": _s("sitting"),
            "sitting_day": _s("sittingDay"),
            "voting_number": _s("votingNumber"),
            "voted_at": _s("date"),
            "kind": _s("kind"),
            "majority_type": _s("majorityType"),
            "majority_votes": _s("majorityVotes"),
            "title": _s("title"),
            "topic": _s("topic"),
            "description": _s("description"),
            "yes": _s("yes"),
            "no": _s("no"),
            "abstain": _s("abstain"),
            "not_participating": _s("notParticipating"),
            "total_voted": _s("totalVoted"),
            "present": _s("present"),
            "against_all": _s("againstAll"),
            "voting_options": "cast(json_array_length(i -> '$.votingOptions') as varchar)",
        },
    ),
    "sejm_prints": (
        r"term\d+_prints",
        {
            "additionalPrints",
            "attachments",
            "changeDate",
            "deliveryDate",
            "documentDate",
            "number",
            "processPrint",
            "term",
            "title",
        },
        {
            "item_term": _s("term"),
            "print_number": _s("number"),
            "title": _s("title"),
            "document_date": _s("documentDate"),
            "delivery_date": _s("deliveryDate"),
            "change_date": _s("changeDate"),
            "process_prints": _joined("processPrint"),
            "additional_prints": "cast(json_array_length(i -> '$.additionalPrints') as varchar)",
        },
    ),
    "sejm_mps": (
        r"term\d+_mps",
        set(landing.MP_FIELDS),
        {
            "mp_id": _s("id"),
            "is_active": _s("active"),
            "club": _s("club"),
            "district_num": _s("districtNum"),
            "district_name": _s("districtName"),
            "voivodeship": _s("voivodeship"),
        },
    ),
}


def columns(table: str) -> list[tuple[str, str]]:
    return [
        ("entity", "VARCHAR"),
        ("term", "VARCHAR"),
        *[(name, "VARCHAR") for name in TABLES[table][2]],
        *bulk_bronze.PROVENANCE,
        ("source_row_number", "BIGINT"),
    ]


def table_of(entity: str) -> str:
    found = [t for t, (pattern, _k, _c) in TABLES.items() if re.fullmatch(pattern, entity)]
    if len(found) != 1:
        raise ValueError(f"entity {entity!r} matches tables {found}")
    return found[0]


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    body.decode("utf-8")  # UnicodeDecodeError rejects the file
    prov = bulk_bronze.provenance(receipt)
    table = table_of(prov["entity"])
    _pattern, known, exprs = TABLES[table]
    term = re.match(r"term(\d+)_", prov["entity"])
    with tempfile.TemporaryDirectory(prefix="zohelo-sejm-") as temporary:
        path = Path(temporary) / "data.json"
        path.write_bytes(body)
        con = duckdb.connect()
        try:
            source = f"read_json_objects('{path}', maximum_object_size = 268435456, format = 'unstructured') as t(j)"
            kind = con.execute(f"select json_type(j) from {source}").fetchall()
            if len(kind) != 1 or kind[0][0] != "ARRAY":
                raise ValueError(f"not one JSON list: {kind[:3]}")
            items = f"{source}, unnest(cast(j as json[])) with ordinality as u(i, n)"
            unknown = con.execute(
                f"select distinct k from (select unnest(json_keys(i)) as k from {items}) where k not in "
                f"({', '.join(repr(k) for k in sorted(known))}) order by 1"
            ).fetchall()
            if unknown:
                raise ValueError(f"unknown item keys: {[k for (k,) in unknown][:10]}")
            out = con.sql(
                f"""
                select
                    ? as entity,
                    ? as term,
                    {", ".join(f"{expr} as {name}" for name, expr in exprs.items())},
                    ? as raw_sha256,
                    ? as source_version,
                    cast(? as timestamptz) as ingested_at,
                    cast(n as bigint) as source_row_number
                from {items}
                order by n
                """,
                params=[
                    prov["entity"],
                    term.group(1) if term else None,
                    prov["raw_sha256"],
                    prov["source_version"],
                    prov["ingested_at"],
                ],
            ).to_arrow_table()
        except duckdb.Error as exc:
            raise ValueError(f"not readable JSON: {str(exc)[:300]}") from exc
        finally:
            con.close()
    return out.cast(bulk_bronze.arrow_schema(columns(table)))


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: about 160 JSON answers, the largest about 2 MB."""
    specs = landing.specs(landing_root)
    status = 0
    for table in TABLES:
        mine = [s for s in specs if table_of(s["entity"]) == table]
        status = max(
            status,
            bulk_bronze.load(
                SOURCE,
                (namespace, table),
                (namespace, REJECTED),
                columns(table),
                decode,
                table.removeprefix("sejm_"),
                landing_root=landing_root,
                lake_root="02_bronze" if namespace == "bronze" else namespace,
                specs=mine,
            ),
        )
    return status
