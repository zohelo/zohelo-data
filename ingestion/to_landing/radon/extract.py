"""RAD-on open-data API: POL-on institutions and study programmes, reduced to non-person fields before Landing.

Route (Dev probes 43328619935 and the second probe on test/447-radon, 12 Nov 2026): the public REST API of RAD-on
(OPI PIB), ``https://radon.nauka.gov.pl/opendata/polon/<resource>?resultNumbers=<n>[&token=<cursor>]``, no key.
An answer is ``{"results": [...], "pagination": {"maxCount": <records>, "token": <cursor of the next page>},
"version": <API version>}``; a run reads every page of each resource, one request at a time with a pause, until a
page comes back empty or without a new cursor. No limit is published (help centre read 12 Nov 2026).

Personal data: an institution record names its head (``managerName``, ``managerSurname`` …) and carries contact
details. ``reduce`` copies only the allowlisted fields of each resource into a new record (nested lists only with
their own allowlist); everything else is dropped in memory and never written (Landing, Archive, logs). Researcher,
employee, doctoral-student, project and publication resources are not read at all.

Completeness: the cursor follows the records' refresh time, so a record refreshed while a run pages can come
twice; the copy with the newest ``lastRefresh`` is kept (the newest version of that record) and the repeats are
counted in the receipt. A resource lands only when the distinct records equal the ``maxCount`` of its last page;
otherwise the run fails for that resource and Landing keeps its previous copy.

Landing: one entity per resource (``institutions``, ``courses``), each file the complete current state as JSON
lines sorted by the resource's UUID with sorted keys, so unchanged data gives the same bytes and is not landed
again (receipt after the file). Version = download time (the API gives no version of the data).
"""

from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
import json
import os
import time
from typing import Any

import requests

from ingestion.common.r2 import req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.patterns import bulk_file
from ingestion.to_landing.radon import RESOURCES, SOURCE, specs

API = "https://radon.nauka.gov.pl/opendata/polon/{resource}"
PAGE_SIZE = 100
PAUSE_SECONDS = 2.0  # between requests; no limit is published
MAX_PAGES = 1000  # a guard against a cursor that never ends (courses: about 120 pages)
RETRY_WAITS = (15, 45, 120)  # seconds before the second, third and fourth attempt
RETRY_STATUSES = {408, 429, 500, 502, 503, 504}

# The allowlists: entity -> (UUID key, top-level fields kept, nested lists kept with their own fields).
KEEP: dict[str, tuple[str, tuple[str, ...], dict[str, tuple[str, ...]]]] = {
    "institutions": (
        "institutionUuid",
        (
            "institutionUuid", "institutionUid", "id", "regon", "nip", "krs", "name",
            "iKindCd", "iKindName", "uTypeCd", "uTypeName", "siTypeCd", "siTypeName", "statusCode", "status",
            "iStartDT", "iLiqStartDT", "iLiqDT", "voivodeshipCode", "voivodeship", "city", "postalCd", "country",
            "countryCd", "www", "supervisingInstitutionID", "supervisingInstitutionName", "ministryNumber",
            "eunNumber", "panNumber", "pib", "yearPib", "federationNumber", "dataSource", "lastRefresh",
        ),
        {},
    ),
    "courses": (
        "courseUuid",
        (
            "courseUuid", "courseCode", "courseOldCode", "courseName", "levelCode", "levelName", "profileCode",
            "profileName", "iscedCode", "iscedName", "currentStatusCode", "currentStatusName", "creationDate",
            "liquidationDate", "terminationInitializationDate", "mainInstitutionUuid", "mainInstitutionName",
            "mainInstitutionKindCode", "mainInstitutionKind", "leadingInstitutionUuid", "leadingInstitutionName",
            "leadingInstitutionVoivodeshipCode", "leadingInstitutionVoivodeship", "leadingInstitutionCity",
            "leadingInstitutionIsForeign", "supervisingInstitutionUuid", "supervisingInstitutionName", "coLed",
            "teacherTraining", "philological", "legalBasisTypeCode", "dataSource", "lastRefresh",
        ),
        {
            "disciplines": ("disciplineCode", "disciplineName", "disciplinePercentageShare", "disciplineLeading"),
            "courseInstances": (
                "courseInstanceUuid", "courseInstanceCode", "formCode", "formName", "titleCode", "titleName",
                "languageCode", "educationStartDate", "numberOfSemesters", "ects", "dual", "statusCode",
                "statusName", "liquidationDate",
            ),
        },
    ),
}  # fmt: skip


class Stop(RuntimeError):
    """The API refused or kept failing for a resource: nothing of it is landed."""


def reduce(entity: str, record: Any) -> dict[str, Any]:
    """The allowlisted record; raises ValueError when the UUID is missing."""
    key, fields, nested = KEEP[entity]
    if not isinstance(record, dict) or not isinstance(record.get(key), str) or not record[key]:
        raise ValueError(f"{entity}: record without {key}")
    out = {name: record.get(name) for name in fields}
    for name, sub in nested.items():
        items = record.get(name)
        out[name] = [{f: item.get(f) for f in sub} for item in items if isinstance(item, dict)] if items else []
    return out


def encode(records: dict[str, dict[str, Any]]) -> bytes:
    """One JSON line per record, sorted by UUID and by key: the same records give the same bytes."""
    lines = [json.dumps(records[k], ensure_ascii=False, sort_keys=True, separators=(",", ":")) for k in sorted(records)]
    return ("\n".join(lines) + "\n").encode("utf-8") if lines else b""


class Client:
    def __init__(self, pause_seconds: float = PAUSE_SECONDS) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": bulk_file.USER_AGENT, "Accept": "application/json"})
        self.pause, self.requests = pause_seconds, 0

    def page(self, resource: str, size: int, token: str | None) -> dict[str, Any]:
        params: dict[str, Any] = {"resultNumbers": size}
        if token:
            params["token"] = token
        last = ""
        for wait in (0, *RETRY_WAITS):
            time.sleep(self.pause + wait)
            self.requests += 1
            try:
                answer = self.session.get(API.format(resource=resource), params=params, timeout=(15, 120))
            except requests.RequestException as exc:
                last = f"{type(exc).__name__}: {str(exc)[:160]}"
                continue
            if answer.status_code in RETRY_STATUSES:
                last = f"HTTP {answer.status_code}"
                continue
            if answer.status_code != 200:
                raise Stop(f"{resource}: HTTP {answer.status_code} {answer.text[:160]}")
            body = answer.json()
            if not isinstance(body, dict) or not isinstance(body.get("results"), list):
                raise Stop(
                    f"{resource}: unknown answer layout {sorted(body) if isinstance(body, dict) else type(body)}"
                )
            return body
        raise Stop(f"{resource}: no answer after {len(RETRY_WAITS) + 1} attempts ({last})")

    def resource(self, entity: str, deadline: float) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
        """Every record of one resource, reduced; with the paging facts for the receipt."""
        records: dict[str, dict[str, Any]] = {}
        token, tokens, pages, repeated, max_count, version = None, set(), 0, 0, None, None
        while True:
            if time.monotonic() > deadline:
                raise Stop(f"{entity}: time budget ended after {pages} pages")
            body = self.page(RESOURCES[entity], PAGE_SIZE, token)
            pages += 1
            pagination = body.get("pagination") or {}
            max_count, version = pagination.get("maxCount"), body.get("version")
            for raw in body["results"]:
                record = reduce(entity, raw)
                key = record[KEEP[entity][0]]
                if key in records:
                    repeated += 1
                    if str(record.get("lastRefresh") or "") < str(records[key].get("lastRefresh") or ""):
                        continue
                records[key] = record
            token = pagination.get("token")
            if not body["results"] or not token or token in tokens:
                break
            tokens.add(token)
            if pages >= MAX_PAGES:
                raise Stop(f"{entity}: more than {MAX_PAGES} pages")
        if not isinstance(max_count, int) or len(records) != max_count:
            raise Stop(f"{entity}: {len(records)} distinct records, the API reports maxCount {max_count}")
        return records, {"pages": pages, "max_count": max_count, "repeated": repeated, "api_version": version}


def _land(client: Any, bucket: str, entity: str, records: dict, facts: dict, held: list, landing_root: str) -> bool:
    """Write the resource's records as a new version unless the bytes are those of its newest copy."""
    body = encode(records)
    digest = sha256(body).hexdigest()
    if not body or (held and held[-1]["sha256"] == digest):
        return False
    ingested = bulk_file.stamp(datetime.now(UTC))
    key = f"{bulk_file.prefix(SOURCE, entity, landing_root)}{ingested}__{ingested}.jsonl"
    client.put_object(
        Bucket=bucket, Key=key, Body=body, ContentType="application/x-ndjson", Metadata={"sha256": digest}
    )
    head = client.head_object(Bucket=bucket, Key=key)
    if head["ContentLength"] != len(body) or (head.get("Metadata") or {}).get("sha256") != digest:
        raise RuntimeError(f"Landing write could not be verified: {key}")
    url = API.format(resource=RESOURCES[entity])
    receipt = {
        "schema_version": 3,
        "kind": f"{SOURCE}_landing",
        "source_id": SOURCE,
        "entity": entity,
        "content": "snapshot",
        "area": "data",
        "version_token": ingested,
        "version_basis": "download time",
        "ingested_at": ingested,
        "source_url": f"{url}?resultNumbers={PAGE_SIZE}&token=<cursor>",
        "final_url": url,
        "sha256": digest,
        "size_bytes": len(body),
        "object": {"bucket": bucket, "key": key},
        "replaces_sha256": held[-1]["sha256"] if held else None,
        "code_sha": os.environ.get("GITHUB_SHA", ""),
        "detail": {
            "pattern": SOURCE,
            "format": "jsonl",
            "reduced": "allowlist of non-person fields (ingestion/to_landing/radon/extract.py)",
            "records": len(records),
            **facts,
        },
    }
    client.put_object(
        Bucket=bucket,
        Key=key.rsplit(".", 1)[0] + ".receipt.json",
        ContentType="application/json",
        Body=json.dumps(receipt, indent=2, sort_keys=True).encode(),
    )
    return True


def extract(budget_minutes: float, landing_root: str = "01_landing") -> int:
    started = time.monotonic()
    deadline = started + budget_minutes * 60
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    api = Client()
    summary: dict[str, Any] = {"source": SOURCE, "mode": "extract", "landed": [], "unchanged": [], "failed": []}
    for spec in specs():
        entity = spec["entity"]
        held = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, entity, landing_root))
        try:
            records, facts = api.resource(entity, deadline)
        except (Stop, ValueError, requests.JSONDecodeError) as exc:  # the other resource still lands
            summary["failed"].append({"entity": entity, "error": str(exc)[:300]})
            continue
        summary[entity] = {"records": len(records), **facts}
        landed = _land(client, bucket, entity, records, facts, held, landing_root)
        summary["landed" if landed else "unchanged"].append(entity)
    summary.update({"requests": api.requests, "seconds": round(time.monotonic() - started)})
    status = 1 if summary["failed"] else 0
    print(json.dumps(summary, sort_keys=True, default=str))
    annotate("error" if status else "notice", f"{SOURCE} landing extract", summary)
    step_summary(f"## {SOURCE} · landing extract\n\n```json\n{json.dumps(summary, indent=2, default=str)}\n```\n")
    return status


def inspect(landing_root: str = "01_landing") -> int:
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    api = Client()
    files, errors = [], []
    for spec in specs():
        entity = spec["entity"]
        held = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, entity, landing_root))
        try:
            reported = (api.page(RESOURCES[entity], 1, None).get("pagination") or {}).get("maxCount")
        except (Stop, requests.JSONDecodeError) as exc:
            reported = None
            errors.append(f"{entity}: {str(exc)[:200]}")
        newest = held[-1] if held else None
        files.append(
            {
                "entity": entity,
                "landed_versions": len(held),
                "newest": newest["version_token"] if newest else None,
                "newest_records": (newest.get("detail") or {}).get("records") if newest else None,
                "api_max_count": reported,
                "state": "missing" if not held else "unknown",  # the records are compared when extract runs
            }
        )
    summary = {"source": SOURCE, "mode": "inspect", "files": files, "errors": errors}
    print(json.dumps(summary, sort_keys=True))
    annotate("error" if errors else "notice", f"{SOURCE} landing inspect", summary)
    return 1 if errors else 0
