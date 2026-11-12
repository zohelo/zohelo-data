"""GIOŚ air quality Landing activities: API v1 of the portal Jakość Powietrza (register PL-ENV-010).

Route (docs/sources/gios_air_quality.md; Dev probes 43320759991, 43322192937): https://api.gios.gov.pl/pjp-api/v1/rest/
answers JSON-LD (``Accept: application/ld+json``; ``application/json`` alone is refused with 406) and pages every list
(``page``, ``size``; ``totalPages`` in the answer). One pass, one request at a time:

- ``station/findAll?size=500``: every operating station in one page (288 on 12 Nov 2026); the station and sensor
  lists are limited to 2 requests a minute for some callers ("limit 2 i 1500 żądań na minutę"), so a further page
  waits 31 s;
- ``station/sensors/{stationId}``: the sensors of each station (about 290 requests, 1 s apart);
- ``data/getData/{sensorId}?size=500``: hourly values from the last full hour back to the third day, local time,
  µg/m³ (about 1,100 requests, 1 s apart; the stated limit is 1,500 a minute).

Entity (layout ``<root>/gios_air_quality/snapshot/data/<entity>/<version>__<ingested>.zip``): ``pass_<YYYYMMDD>``,
one daily pass (UTC day of the run), landed once per day, so a second run on the same day writes nothing. The file is
a ZIP of the exact answers, one member per page: ``station/page-0.json``, ``sensors/<station>/page-0.json``,
``data/<station>/<sensor>/page-0.json``. Each pass keeps its own station and sensor lists, so a station that leaves
the list keeps its last known attributes in Silver. The values are unverified (GIOŚ: "Udostępniane dane nie są na
bieżąco weryfikowane"): every pass is kept and Silver takes the newest pass per sensor and hour. A sensor that
answers 4xx is recorded in the receipt and left out of the pass; more than 5% of sensors failing, or the time budget
running out, lands nothing (the next run starts again).

Modes: inspect (today's pass missing or current; one station-list request), load, verify (every receipt has its
file in Landing or the Archive).
"""

from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
import io
import json
import os
import time
from typing import Any
import zipfile

import requests

SOURCE = "gios_air_quality"
API = "https://api.gios.gov.pl/pjp-api/v1/rest"
USER_AGENT = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
ACCEPT = "application/ld+json, application/json;q=0.9"
PAGE_SIZE = 500
PAUSE_SECONDS = 1.0  # sensors and data: 60 a minute at most, against a stated 1,500
LIST_PAUSE_SECONDS = 31.0  # further pages of the station list: below 2 a minute
RETRIES = 4
MAX_FAILED_SHARE = 0.05
LISTS = {"stations": "Lista stacji pomiarowych", "sensors": "Lista stanowisk pomiarowych dla podanej stacji"}
DATA_LIST = "Lista danych pomiarowych"


class Refused(RuntimeError):
    """The API refused one request with a 4xx other than 429 (an unknown or retired id)."""


def fetch(session: requests.Session, url: str, params: dict[str, Any]) -> bytes:
    """One GET with bounded retries: network errors, 429 and 5xx back off (Retry-After when given)."""
    for attempt in range(RETRIES):
        try:
            response = session.get(url, params=params, timeout=(30, 120))
        except (requests.ConnectionError, requests.Timeout):
            response = None
        if response is not None and response.status_code == 200:
            return response.content
        if response is not None and 400 <= response.status_code < 500 and response.status_code != 429:
            raise Refused(f"{response.status_code} for {response.url}")
        wait = 30 * 2**attempt
        if response is not None and (response.headers.get("Retry-After") or "").isdigit():
            wait = max(wait, int(response.headers["Retry-After"]))
        time.sleep(min(wait, 300))
    raise RuntimeError(f"GIOŚ API kept failing: {url}")


def pages(session: requests.Session, url: str, list_key: str, pause: float) -> list[tuple[int, bytes, list]]:
    """Every page of one list: (page number, exact bytes, items)."""
    out, page, total = [], 0, 1
    while page < total:
        if page:
            time.sleep(pause)
        body = fetch(session, url, {"page": page, "size": PAGE_SIZE})
        parsed = json.loads(body)
        if list_key not in parsed:
            raise ValueError(f"{url}: no '{list_key}' in the answer (keys {list(parsed)[:6]})")
        total = int(parsed.get("totalPages") or 1)
        out.append((page, body, parsed[list_key]))
        page += 1
    return out


def zipped(members: dict[str, bytes]) -> bytes:
    """A ZIP of the exact answers with fixed member times: equal answers give equal bytes."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name in sorted(members):
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, members[name])
    return buffer.getvalue()


def receipts(client, bucket: str, root: str, wanted=None) -> dict[str, list[dict[str, Any]]]:
    """Landing receipts by entity, oldest landed first; ``wanted(entity)`` limits which are read (R2 reads)."""
    from ingestion.common.r2 import get_with_backoff

    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=f"{root}/{SOURCE}/snapshot/data/"
    ):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".receipt.json")]
    out: dict[str, list[dict[str, Any]]] = {}
    for key in keys:
        entity = key.rsplit("/", 2)[-2]
        if wanted is None or wanted(entity):
            out.setdefault(entity, []).append({**json.loads(get_with_backoff(client, bucket, key)), "receipt_key": key})
    for rows in out.values():
        rows.sort(key=lambda r: r["ingested_at"])
    return out


def today() -> str:
    return f"pass_{datetime.now(UTC):%Y%m%d}"


def land(client, bucket: str, root: str, entity: str, body: bytes, held: list, detail: dict) -> str:
    """Write one file and its receipt (after the file), unless its bytes equal the newest landed copy."""
    from ingestion.to_landing.patterns import bulk_file
    from ingestion.to_landing.world_bank_icp.extract import put_new

    digest = sha256(body).hexdigest()
    if held and held[-1]["sha256"] == digest:
        return "unchanged"
    ingested = bulk_file.stamp(datetime.now(UTC))
    key = f"{bulk_file.prefix(SOURCE, entity, root)}{ingested}__{ingested}.zip"
    put_new(client, bucket, key, body, "application/zip")
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
        "source_url": API,
        "final_url": API,
        "sha256": digest,
        "size_bytes": len(body),
        "object": {"bucket": bucket, "key": key},
        "replaces_sha256": held[-1]["sha256"] if held else None,
        "code_sha": os.environ.get("GITHUB_SHA", ""),
        "detail": {"pattern": "gios_air_quality", "format": "zip", **detail},
    }
    put_new(
        client,
        bucket,
        key[: -len(".zip")] + ".receipt.json",
        json.dumps(receipt, indent=2, sort_keys=True).encode(),
        "application/json",
    )
    return "landed_changed" if held else "landed_new"


def run_load(client, bucket: str, root: str, held: dict, budget_minutes: float) -> tuple[dict[str, Any], int]:
    entity = today()
    if held.get(entity):
        return {"entity": entity, "pass": "current (one pass per UTC day)"}, 0
    deadline = time.monotonic() + budget_minutes * 60
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": ACCEPT})
    station_pages = pages(session, f"{API}/station/findAll", LISTS["stations"], LIST_PAUSE_SECONDS)
    stations = [s["Identyfikator stacji"] for _, _, items in station_pages for s in items]
    if len(stations) != len(set(stations)) or not stations:
        raise ValueError(f"station list: {len(stations)} ids, {len(set(stations))} distinct")
    members = {f"station/page-{n}.json": body for n, body, _ in station_pages}
    sensors: list[tuple[int, int]] = []
    for station in stations:
        time.sleep(PAUSE_SECONDS)
        for n, body, items in pages(session, f"{API}/station/sensors/{station}", LISTS["sensors"], LIST_PAUSE_SECONDS):
            members[f"sensors/{station}/page-{n}.json"] = body
            sensors += [(station, s["Identyfikator stanowiska"]) for s in items]
    failed, values = [], 0
    for station, sensor in sensors:
        if time.monotonic() > deadline:
            return {"entity": entity, "pass": "not landed: time budget", "sensors_done": len(members)}, 2
        time.sleep(PAUSE_SECONDS)
        try:
            for n, body, items in pages(session, f"{API}/data/getData/{sensor}", DATA_LIST, PAUSE_SECONDS):
                members[f"data/{station}/{sensor}/page-{n}.json"] = body
                values += len(items)
        except Refused as exc:
            failed.append({"sensor": sensor, "error": str(exc)[:160]})
    summary = {"entity": entity, "stations": len(stations), "sensors": len(sensors), "requests": len(members)}
    summary.update({"values": values, "sensors_failed": len(failed), "failed_sample": failed[:5]})
    if len(failed) > MAX_FAILED_SHARE * max(len(sensors), 1):
        summary["pass"] = "not landed: too many sensors failed"
        return summary, 1
    body = zipped(members)
    detail = {"stations": len(stations), "sensors": len(sensors), "sensors_failed": failed, "values": values}
    summary["pass"] = land(client, bucket, root, entity, body, [], detail)
    summary["bytes"] = len(body)
    return summary, 0


def run_verify(client, bucket: str, root: str, held: dict) -> tuple[dict[str, Any], int]:
    from ingestion.common.r2 import archive_key, archived

    stored = archived(client, bucket, SOURCE)
    present = set()
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{root}/{SOURCE}/"):
        present |= {o["Key"] for o in page.get("Contents", []) if not o["Key"].endswith(".receipt.json")}
    rows = [r for copies in held.values() for r in copies]
    missing = [
        r["object"]["key"]
        for r in rows
        if r["object"]["key"] not in present
        and stored.get(r["sha256"], {}).get("key") != archive_key(SOURCE, r["sha256"], r["object"]["key"])
    ]
    return {"receipts": len(rows), "missing_data": len(missing), "missing_sample": missing[:10]}, int(bool(missing))


def main(mode: str, budget_minutes: float = 120, landing_root: str = "01_landing") -> int:
    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate, step_summary

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    held = receipts(client, bucket, landing_root, None if mode == "verify" else {today()}.__contains__)
    base = {"mode": mode, "entities_read": len(held)}
    if mode == "load":
        summary, status = run_load(client, bucket, landing_root, held, budget_minutes)
    elif mode == "verify":
        summary, status = run_verify(client, bucket, landing_root, held)
    else:  # inspect: is today's pass missing, and does the station list answer
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT, "Accept": ACCEPT})
        listed = pages(session, f"{API}/station/findAll", LISTS["stations"], LIST_PAUSE_SECONDS)
        summary = {"stations_listed": sum(len(i) for _, _, i in listed), "today": today()}
        summary["state"] = "current" if held.get(today()) else "missing"
        status = 0
    summary = {**base, **summary}
    print(json.dumps(summary, sort_keys=True, default=str))
    annotate("error" if status else "notice", f"{SOURCE} landing {mode}", summary)
    step_summary(f"## {SOURCE} · landing {mode}\n\n```json\n{json.dumps(summary, indent=2, default=str)}\n```\n")
    return status


ACTIVITIES = {
    "landing.inspect": lambda p: main("inspect", p.budget_minutes),
    "landing.extract": lambda p: main("load", p.budget_minutes),
    "landing.verify": lambda p: main("verify", p.budget_minutes),
}
