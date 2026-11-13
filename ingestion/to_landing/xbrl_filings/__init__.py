"""filings.xbrl.org Landing activities: ESEF annual financial reports of Polish issuers (register CD-013).

XBRL International's repository of ESEF filings offers a JSON:API (https://filings.xbrl.org/docs/api, no key).
Two kinds of entity are landed:

``filings_pl``        the list of Polish filings with their issuers (``/api/filings?filter[country]=PL`` with
                      ``include=entity``, one page of up to 1,000 filings, sorted by date added), the exact response,
                      through the shared bulk_file pattern; version = download time, unchanged bytes are skipped.
``facts_<fxo_id>``    per filing, the facts of the selected concepts (CONCEPTS) from the filing's xBRL-JSON document
                      (``json_url``). The document is 0.1 to 9 MB, mostly text blocks, so it is reduced before
                      Landing (storage rule; precedent O33): the selected facts are kept whole, with every dimension,
                      next to the document's identity (URL, SHA-256 and size of the bytes read). Version = the
                      filing's ``processed`` time; a filing whose version Landing holds is not read again, so the
                      first run reads every document once and later runs only new or reprocessed filings.

One request at a time, PAUSE seconds apart, bounded retries with backoff, a time budget; a run that stops at its
budget continues from Landing on the next run. A document that cannot be read (refused, not xBRL-JSON, or still
failing after the retries) is reported and read again on the next run; FAILURES_IN_A_ROW failures stop the run.
The provider states no rate limit and asks routine users for "a user agent that includes a contact email address,
or a link to where we can find your contact details": the repository's identified User-Agent
(bulk_file.USER_AGENT) carries the project link.

landing.inspect  the API's count of Polish filings vs the list Landing holds, and facts held / to read; no writes
landing.extract  the list, then the facts documents not yet held
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
import json
import os
import time
from typing import Any

SOURCE = "xbrl_filings"
BASE = "https://filings.xbrl.org"
LIST_ENTITY = "filings_pl"
PAGE_SIZE = 1000  # the API's maximum; Bronze rejects a list that does not fit one page
LIST_URL = (
    f"{BASE}/api/filings?filter[country]=PL&page[size]={PAGE_SIZE}&page[number]=1&sort=date_added,fxo_id&include=entity"
)
CONCEPTS = ("ifrs-full:ProfitLoss", "ifrs-full:Revenue")
PAUSE = 2.0
RETRIES = 3
FAILURES_IN_A_ROW = 5  # documents that could not be read one after another: the host is down, the run stops


def list_spec() -> dict[str, Any]:
    """The filing list as a bulk_file file (parsed before landing; the API sends no Last-Modified)."""
    return {"entity": LIST_ENTITY, "url": LIST_URL, "format": "json", "version": "ingested", "retries": RETRIES}


def facts_entity(fxo_id: str) -> str:
    return f"facts_{fxo_id}"


def version_token(processed: str) -> str:
    """The filing's ``processed`` time (as published, no zone: read as UTC) as a layout timestamp."""
    return datetime.fromisoformat(processed).replace(tzinfo=UTC).strftime("%Y%m%dT%H%M%SZ")


def filings(body: bytes) -> tuple[list[dict[str, Any]], int]:
    """(the filings of a landed list with the fields the facts step needs, the API's total count)."""
    document = json.loads(body)
    rows = [
        {
            "fxo_id": item["attributes"]["fxo_id"],
            "processed": item["attributes"]["processed"],
            "package_sha256": item["attributes"]["sha256"],
            "json_url": item["attributes"]["json_url"],
        }
        for item in document["data"]
    ]
    return rows, int(document["meta"]["count"])


def reduce_document(body: bytes, filing: dict[str, Any]) -> bytes:
    """The xBRL-JSON document reduced to the facts of CONCEPTS (each fact whole), as deterministic JSON.
    The filing's id and processed time are part of the bytes, so no two filings or versions share a SHA-256."""
    document = json.loads(body)
    facts = document.get("facts")
    if not isinstance(facts, dict):
        raise ValueError("no facts object in the xBRL-JSON document")
    kept = {
        fact_id: fact
        for fact_id, fact in sorted(facts.items())
        if isinstance(fact, dict) and (fact.get("dimensions") or {}).get("concept") in CONCEPTS
    }
    reduced = {
        "fxo_id": filing["fxo_id"],
        "processed": filing["processed"],
        "package_sha256": filing["package_sha256"],
        "json_url": filing["json_url"],
        "document_sha256": sha256(body).hexdigest(),
        "document_bytes": len(body),
        "document_type": (document.get("documentInfo") or {}).get("documentType"),
        "facts_in_document": len(facts),
        "concepts": list(CONCEPTS),
        "facts": kept,
    }
    return json.dumps(reduced, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def held_versions(client: Any, bucket: str, landing_root: str = "01_landing") -> dict[str, set[str]]:
    """{entity: version tokens landed}, from the receipt names (receipts stay when a file moves to the Archive)."""
    folder = f"{landing_root}/{SOURCE}/snapshot/data/"
    out: dict[str, set[str]] = {}
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=folder):
        for item in page.get("Contents", []):
            if item["Key"].endswith(".receipt.json"):
                entity, name = item["Key"][len(folder) :].split("/", 1)
                out.setdefault(entity, set()).add(name.split("__", 1)[0])
    return out


def newest_list(client: Any, bucket: str, landing_root: str = "01_landing") -> bytes | None:
    """The bytes of the newest landed filing list (Landing, else Archive), or None when nothing has landed."""
    from ingestion.common import bulk_bronze
    from ingestion.to_landing.patterns import bulk_file

    held = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, LIST_ENTITY, landing_root))
    return bulk_bronze.read_raw(client, bucket, SOURCE, held[-1]) if held else None


def plan(rows: list[dict[str, Any]], held: dict[str, set[str]]) -> list[dict[str, Any]]:
    """Filings with an xBRL-JSON document whose current version Landing does not hold, in list order."""
    return [
        row
        for row in rows
        if row["json_url"] and version_token(row["processed"]) not in held.get(facts_entity(row["fxo_id"]), set())
    ]


def fetch(url: str) -> bytes:
    """GET one document with bounded retries; a 4xx other than 408/429 fails at once (PermanentError)."""
    import requests

    from ingestion.to_landing.patterns import bulk_file

    for attempt in range(RETRIES):
        try:
            response = requests.get(url, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=(30, 180))
            if 400 <= response.status_code < 500 and response.status_code not in bulk_file.RETRY_STATUSES:
                raise bulk_file.PermanentError(f"download {response.status_code}: {url}")
            if response.status_code != 200:
                raise RuntimeError(f"download {response.status_code}")
            return response.content
        except bulk_file.PermanentError:
            raise
        except (requests.RequestException, RuntimeError) as exc:
            if attempt == RETRIES - 1:
                raise
            print(json.dumps({"status": "retry", "url": url, "error": str(exc)[:200]}), flush=True)
            time.sleep(30 * (attempt + 1))
    raise RuntimeError("unreachable")


def land_facts(client: Any, bucket: str, filing: dict[str, Any], body: bytes, landing_root: str) -> dict[str, Any]:
    """Write one reduced facts document and, after it, its receipt (bulk_file receipt shape)."""
    from ingestion.to_landing.patterns import bulk_file

    entity = facts_entity(filing["fxo_id"])
    reduced = reduce_document(body, filing)
    digest, ingested = sha256(reduced).hexdigest(), bulk_file.stamp(datetime.now(UTC))
    version = version_token(filing["processed"])
    key = f"{bulk_file.prefix(SOURCE, entity, landing_root)}{version}__{ingested}.json"
    client.put_object(Bucket=bucket, Key=key, Body=reduced, ContentType="application/json", Metadata={"sha256": digest})
    head = client.head_object(Bucket=bucket, Key=key)
    if head["ContentLength"] != len(reduced) or (head.get("Metadata") or {}).get("sha256") != digest:
        raise RuntimeError(f"Landing write could not be verified: {key}")
    receipt = {
        "schema_version": 3,
        "kind": f"{SOURCE}_landing",
        "source_id": SOURCE,
        "entity": entity,
        "content": "snapshot",
        "area": "data",
        "version_token": version,
        "version_basis": "filing processed time (API)",
        "ingested_at": ingested,
        "source_url": BASE + filing["json_url"],
        "final_url": BASE + filing["json_url"],
        "sha256": digest,
        "size_bytes": len(reduced),
        "object": {"bucket": bucket, "key": key},
        "replaces_sha256": None,
        "code_sha": os.environ.get("GITHUB_SHA", ""),
        "detail": {
            "pattern": "xbrl_filings facts",
            "format": "json",
            "reduced": "selected concepts",
            "concepts": list(CONCEPTS),
            "document_sha256": sha256(body).hexdigest(),
            "document_bytes": len(body),
        },
    }
    client.put_object(
        Bucket=bucket,
        Key=key.rsplit(".", 1)[0] + ".receipt.json",
        ContentType="application/json",
        Body=json.dumps(receipt, indent=2, sort_keys=True).encode(),
    )
    return {"entity": entity, "bytes": len(reduced), "document_bytes": len(body)}


def extract(budget_minutes: float, landing_root: str = "01_landing", max_items: int = 0) -> int:
    """Land the list (when its bytes changed), then every facts document not yet held, within the budget."""
    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    deadline = time.monotonic() + budget_minutes * 60
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    listed = bulk_file.load_file(client, bucket, SOURCE, list_spec(), landing_root)
    rows, count = filings(newest_list(client, bucket, landing_root))
    summary: dict[str, Any] = {"list": listed["outcome"], "api_count": count, "filings_in_list": len(rows)}
    if count != len(rows):
        summary["error"] = f"the list does not fit one page of {PAGE_SIZE}: add a page before reading facts"
        annotate("error", f"{SOURCE} landing extract", summary)
        return 1
    todo = plan(rows, held_versions(client, bucket, landing_root))
    if max_items:
        todo = todo[:max_items]
    import requests

    landed, refused, invalid, failed, deferred, read_bytes, kept_bytes = 0, [], [], [], 0, 0, 0
    in_a_row = 0
    for n, filing in enumerate(todo):
        if time.monotonic() > deadline or in_a_row >= FAILURES_IN_A_ROW:
            deferred = len(todo) - n
            break
        time.sleep(PAUSE)
        try:
            body = fetch(BASE + filing["json_url"])
        except bulk_file.PermanentError as exc:
            refused.append({"fxo_id": filing["fxo_id"], "error": str(exc)[:120]})
            continue
        except (requests.RequestException, RuntimeError) as exc:  # retries used up: read again next run
            failed.append({"fxo_id": filing["fxo_id"], "error": str(exc)[:120]})
            in_a_row += 1
            continue
        in_a_row = 0
        try:
            result = land_facts(client, bucket, filing, body, landing_root)
        except ValueError as exc:  # not JSON, or not an xBRL-JSON document: nothing is landed, read again next run
            invalid.append({"fxo_id": filing["fxo_id"], "error": str(exc)[:120]})
            continue
        landed += 1
        read_bytes += result["document_bytes"]
        kept_bytes += result["bytes"]
    summary.update(
        {
            "without_json_url": sum(1 for row in rows if not row["json_url"]),
            "facts_planned": len(todo),
            "facts_landed": landed,
            "document_bytes_read": read_bytes,
            "bytes_landed": kept_bytes,
            "deferred_to_next_run": deferred,
            "refused": len(refused),
            "refused_sample": refused[:5],
            "invalid": len(invalid),
            "invalid_sample": invalid[:5],
            "failed": len(failed),
            "failed_sample": failed[:5],
        }
    )
    stopped = in_a_row >= FAILURES_IN_A_ROW
    if stopped:
        summary["error"] = f"{FAILURES_IN_A_ROW} documents in a row could not be read: stopped, the next run continues"
    level = "error" if stopped else "warning" if refused or invalid or failed else "notice"
    annotate(level, f"{SOURCE} landing extract", summary)
    return 1 if stopped else 0


def _inspect(p) -> int:
    import requests

    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    summary: dict[str, Any] = {"source": SOURCE, "mode": "inspect"}
    try:
        answer = requests.get(
            LIST_URL.replace(f"page[size]={PAGE_SIZE}", "page[size]=1").replace("&include=entity", ""),
            headers={"User-Agent": bulk_file.USER_AGENT},
            timeout=60,
        )
        summary["api_status"] = answer.status_code
        summary["api_count"] = answer.json()["meta"]["count"] if answer.status_code == 200 else None
    except (requests.RequestException, ValueError, KeyError) as exc:
        summary["api_status"] = type(exc).__name__
    body = newest_list(client, bucket)
    if body is None:
        summary["state"] = "missing"
    else:
        rows, count = filings(body)
        todo = plan(rows, held_versions(client, bucket))
        summary.update(
            {
                "filings_in_landed_list": len(rows),
                "landed_list_count": count,
                "with_json_url": sum(1 for row in rows if row["json_url"]),
                "facts_to_read": len(todo),
                "state": "current" if summary.get("api_count") == len(rows) and not todo else "stale",
            }
        )
    ok = summary.get("api_status") == 200
    annotate("notice" if ok else "error", f"{SOURCE} landing inspect", summary)
    return 0 if ok else 1


def _extract(p) -> int:
    return extract(p.budget_minutes, max_items=p.max_items)


def _verify(p) -> int:
    from ingestion.common.r2 import req, s3
    from ingestion.to_landing.patterns import bulk_file

    entities = sorted(held_versions(s3(), req("STORAGE_LANDING_BUCKET_NAME")))
    return bulk_file.main(SOURCE, "verify", specs=[{"entity": entity} for entity in entities])


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract, "landing.verify": _verify}
