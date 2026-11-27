#!/usr/bin/env python3
"""OpenAlex bronze.load: newest Landing copy per entity → bronze.openalex_works_counts, bronze.openalex_institutions.

The Landing files (ingestion/to_landing/openalex/) are ZIPs of exact API answers, one member per page:

- ``works_pl``: ``works_by_year__pNNN.json``, ``works_oa_by_year__pNNN.json``, ``works_by_year_field_<id>__pNNN.json``
  (group_by publication_year answers) and ``fields__pNNN.json`` (the field list). One Bronze row per group
  (record_kind ``group``: request, field id of the request, group key and display name, count, the answer's
  ``groups_count`` and ``count``) and per field (record_kind ``field``: OpenAlex field and domain id and name).
- ``institutions_pl``: ``institutions__pNNN.json``. One Bronze row per institution: the selected root fields as
  published text (geo flattened to city, region, country, coordinates; counts_by_year and lineage as JSON text).

Values stay text. A member outside these names, an answer whose groups or results do not add up to its ``meta``
(a page lost) or a full page of groups (the list may go on), a key twice in one request, a field of the list
without its request (or a request of a field the list does not name), or an institution with an attribute outside
the selection rejects the file into bronze.openalex_rejected_files (never guessed).
"""

from __future__ import annotations

import io
import json
import re
from typing import Any
import zipfile

import pyarrow as pa

from ingestion.common import bulk_bronze
from ingestion.to_landing.openalex import extract

SOURCE = "openalex"
COUNTS = "openalex_works_counts"
INSTITUTIONS = "openalex_institutions"
REJECTED = "openalex_rejected_files"
COUNT_VALUES = [
    "record_kind",
    "request",
    "field_id",
    "group_key",
    "group_key_display_name",
    "works",
    "groups_count",
    "meta_count",
    "field_name",
    "domain_id",
    "domain_name",
    "member",
]
INSTITUTION_VALUES = [
    "institution_id",
    "ror",
    "display_name",
    "institution_type",
    "country_code",
    "geo_city",
    "geo_region",
    "geo_country",
    "geo_country_code",
    "latitude",
    "longitude",
    "works_count",
    "cited_by_count",
    "counts_by_year_json",
    "lineage_json",
    "updated_date",
    "meta_count",
    "member",
]
COUNT_COLUMNS = [
    ("entity", "VARCHAR"),
    *[(c, "VARCHAR") for c in COUNT_VALUES],
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),  # position of the row in its request (pages in order), from 1
]
INSTITUTION_COLUMNS = [
    ("entity", "VARCHAR"),
    *[(c, "VARCHAR") for c in INSTITUTION_VALUES],
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
MEMBER = re.compile(r"(works_by_year|works_oa_by_year|works_by_year_field_(\d+)|fields|institutions)__p(\d{3})\.json")
DOMAIN_ID = re.compile(r"https://openalex\.org/domains/(\d+)")
INSTITUTION_KEYS = set(extract.INSTITUTION_FIELDS.split(","))


def text(value: Any) -> str | None:
    """A published value as text: strings unchanged, numbers as their JSON text, objects as JSON, null as null."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def requests_of(body: bytes, expected: set[str]) -> dict[str, list[Any]]:
    """The ZIP's answers grouped by request, pages in order; checks the member names and the page sequence."""
    out: dict[str, list[tuple[int, Any]]] = {}
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        for name in archive.namelist():
            match = MEMBER.fullmatch(name)
            if not match or (match.group(1) if not match.group(2) else "works_by_year_field") not in expected:
                raise ValueError(f"unexpected member {name}")
            answer = json.loads(archive.read(name))
            if not isinstance(answer, dict) or not isinstance(answer.get("meta"), dict):
                raise ValueError(f"{name}: not an OpenAlex answer")
            out.setdefault(match.group(1), []).append((int(match.group(3)), answer))
    pages: dict[str, list[Any]] = {}
    for request, numbered in out.items():
        numbered.sort(key=lambda p: p[0])
        if [n for n, _ in numbered] != list(range(1, len(numbered) + 1)):
            raise ValueError(f"{request}: pages {[n for n, _ in numbered]} are not 1..n")
        pages[request] = [a for _, a in numbered]
    return pages


def items(request: str, answers: list[Any], key: str) -> tuple[list[Any], Any]:
    """All items of one request's pages and its first page's meta. Results must add up to meta.count over the pages;
    a group_by answer is one page whose groups equal its meta.groups_count (the groups of that page) and is not
    full (200 groups would mean a second page, which the extractor cannot read without a key)."""
    found = [i for a in answers for i in (a.get(key) or [])]
    meta = answers[0]["meta"]
    if key == "group_by":
        if len(answers) != 1 or meta.get("groups_count") is None or int(meta["groups_count"]) != len(found):
            raise ValueError(
                f"{request}: {len(answers)} pages, {len(found)} groups, groups_count {meta.get('groups_count')}"
            )
        if len(found) >= extract.PER_PAGE:
            raise ValueError(f"{request}: a full page of {len(found)} groups (more may exist)")
    elif meta.get("count") is None or int(meta["count"]) != len(found):
        raise ValueError(f"{request}: {len(found)} results, meta.count {meta.get('count')}")
    return found, meta


def decode_counts(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    pages = requests_of(body, {"works_by_year", "works_oa_by_year", "works_by_year_field", "fields"})
    for need in ("works_by_year", "works_oa_by_year", "fields"):
        if need not in pages:
            raise ValueError(f"no {need} answer")
    rows: list[dict[str, Any]] = []
    fields, fields_meta = items("fields", pages["fields"], "results")
    listed: set[str] = set()
    for n, field in enumerate(fields, start=1):
        match = extract.FIELD_ID.fullmatch(str(field.get("id")))
        domain = field.get("domain") or {}
        domain_match = DOMAIN_ID.fullmatch(str(domain.get("id")))
        if not match or not domain_match or not field.get("display_name"):
            raise ValueError(f"fields: unexpected item {str(field)[:200]}")
        if match.group(1) in listed:
            raise ValueError(f"fields: field {match.group(1)} listed twice")
        listed.add(match.group(1))
        rows.append(
            {
                "record_kind": "field",
                "request": "fields",
                "field_id": match.group(1),
                "field_name": text(field["display_name"]),
                "domain_id": domain_match.group(1),
                "domain_name": text(domain.get("display_name")),
                "meta_count": text(fields_meta.get("count")),
                "member": "fields",
                "source_row_number": n,
            }
        )
    requested = {r.removeprefix("works_by_year_field_") for r in pages if r.startswith("works_by_year_field_")}
    if requested != listed:
        raise ValueError(
            f"field requests {sorted(requested - listed)} not listed, {sorted(listed - requested)} missing"
        )
    for request, answers in pages.items():
        if request == "fields":
            continue
        groups, meta = items(request, answers, "group_by")
        seen: set[str] = set()
        for n, group in enumerate(groups, start=1):
            key = text(group.get("key"))
            if key is None or key in seen:
                raise ValueError(f"{request}: group key {key!r} missing or twice")
            seen.add(key)
            rows.append(
                {
                    "record_kind": "group",
                    "request": request,
                    "field_id": request.removeprefix("works_by_year_field_") if "_field_" in request else None,
                    "group_key": key,
                    "group_key_display_name": text(group.get("key_display_name")),
                    "works": text(group.get("count")),
                    "groups_count": text(meta.get("groups_count")),
                    "meta_count": text(meta.get("count")),
                    "member": request,
                    "source_row_number": n,
                }
            )
    return table(rows, receipt, COUNT_VALUES, COUNT_COLUMNS)


def decode_institutions(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    pages = requests_of(body, {"institutions"})
    if "institutions" not in pages:
        raise ValueError("no institutions answer")
    found, meta = items("institutions", pages["institutions"], "results")
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for n, item in enumerate(found, start=1):
        if not isinstance(item, dict) or set(item) - INSTITUTION_KEYS:
            raise ValueError(f"institution with unexpected attributes {sorted(set(item) - INSTITUTION_KEYS)}")
        ident = text(item.get("id"))
        if not ident or ident in seen:
            raise ValueError(f"institution id {ident!r} missing or twice")
        seen.add(ident)
        geo = item.get("geo") or {}
        rows.append(
            {
                "institution_id": ident,
                "ror": text(item.get("ror")),
                "display_name": text(item.get("display_name")),
                "institution_type": text(item.get("type")),
                "country_code": text(item.get("country_code")),
                "geo_city": text(geo.get("city")),
                "geo_region": text(geo.get("region")),
                "geo_country": text(geo.get("country")),
                "geo_country_code": text(geo.get("country_code")),
                "latitude": text(geo.get("latitude")),
                "longitude": text(geo.get("longitude")),
                "works_count": text(item.get("works_count")),
                "cited_by_count": text(item.get("cited_by_count")),
                "counts_by_year_json": text(item.get("counts_by_year")),
                "lineage_json": text(item.get("lineage")),
                "updated_date": text(item.get("updated_date")),
                "meta_count": text(meta.get("count")),
                "member": "institutions",
                "source_row_number": n,
            }
        )
    return table(rows, receipt, INSTITUTION_VALUES, INSTITUTION_COLUMNS)


def table(rows: list[dict[str, Any]], receipt: dict[str, Any], values: list[str], columns) -> pa.Table:
    if not rows:
        raise ValueError("no rows")
    base = bulk_bronze.provenance(receipt)
    data = {c: [base[c]] * len(rows) for c in ("entity", "raw_sha256", "source_version", "ingested_at")}
    for column in (*values, "source_row_number"):
        data[column] = [r.get(column) for r in rows]
    return pa.Table.from_pydict({n: data[n] for n, _ in columns}, schema=bulk_bronze.arrow_schema(columns))


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: two files of well under 1 MB."""
    del budget_minutes
    lake_root = "02_bronze" if namespace == "bronze" else namespace
    status = 0
    for table_name, columns, decode, folder, entity in (
        (COUNTS, COUNT_COLUMNS, decode_counts, "works_counts", "works_pl"),
        (INSTITUTIONS, INSTITUTION_COLUMNS, decode_institutions, "institutions", "institutions_pl"),
    ):
        status |= bulk_bronze.load(
            SOURCE,
            (namespace, table_name),
            (namespace, REJECTED),
            columns,
            decode,
            folder,
            landing_root=landing_root,
            lake_root=lake_root,
            specs=[{"entity": entity}],
        )
    return status
