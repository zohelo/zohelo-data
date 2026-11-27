"""GUGiK KI EGiB aggregate WFS Landing: parcel and building counts per powiat (resultType=hits). No feature is read.

https://mapy.geoportal.gov.pl/wss/service/PZGIK/EGIB/WFS/UslugaZbiorcza (WFS 2.0.0) gathers the county EGiB WFS
services every day. Its feature types have no county attribute, so a powiat's count is a ``resultType=hits`` request
with a ``fes:PropertyIsLike`` filter ``<powiat TERYT>%`` on the identifier, which starts with the cadastral unit code:
``ID_DZIALKI`` (ms:dzialki, e.g. "146502_8.0615.1") and ``ID_BUDYNKU`` (ms:budynki). ``NUMER_JEDNOSTKI`` is not used:
it is empty for Warsaw (0 against 287,930 by ID_DZIALKI, Dev probes in docs/sources/gugik_egib.md).

A snapshot ("round") of a feature type = the unfiltered national count (part ``PL``) and, per voivodeship, its
``<WW>%`` count and its POWIATS (part ``<WW>``): 397 requests, one at a time, PAUSE apart. A parcel request takes
about 40 s at the server (the unfiltered one too), so a parcel round takes about 4.5 hours and may span two runs: each
part is landed as soon as its answers are read, as its own entity ``<feature type>_<round date>_<part>``, one JSON
Lines file ``<version>__<ingested>.jsonl`` (one line per request: parameters, URL, time, the answer's exact text and
the SHA-256 of its bytes). Restart unit = one part. A run first finishes the open round of each feature type, then
starts a new round (round date = today, UTC) when the newest complete one is FRESH_DAYS old or older; buildings
before parcels. Silver uses complete rounds only. The answers carry a timeStamp, so two reads never have the same
bytes; a complete fresh round is not read again (a second run writes nothing).

POWIATS: the 380 powiat codes of TERYT TERC (state of 1 January 2026, as listed by GUGiK's own BDOT10k package
index, Dev probe 45897200062). A code outside the list (a new powiat, a malformed identifier) is not lost: the
national and voivodeship counts keep it, and Gold reports the difference.

landing.inspect  state of the rounds per feature type and the national counts; no writes
landing.extract  continue or start rounds within the budget, landing each part as one file
landing.verify   every receipt has its file (Landing or Archive); no requests to GUGiK
"""

from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import tempfile
import time
from typing import Any

import requests

SOURCE = "gugik_egib"
SERVICE = "https://mapy.geoportal.gov.pl/wss/service/PZGIK/EGIB/WFS/UslugaZbiorcza"
LANDING_ROOT = "01_landing"  # a Dev trial points it at a zz_dev_ scratch prefix
USER_AGENT = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
TYPES = {"budynki": ("ms:budynki", "ID_BUDYNKU"), "dzialki": ("ms:dzialki", "ID_DZIALKI")}  # run order
PAUSE, RETRIES, FRESH_DAYS = 1.0, 4, 6
# Voivodeship -> its powiat codes (two digits; ranges inclusive): land powiats, then cities with powiat rights.
POWIATS = {
    "02": "01-26,61-62,64-65",
    "04": "01-19,61-64",
    "06": "01-20,61-64",
    "08": "01-12,61-62",
    "10": "01-21,61-63",
    "12": "01-19,61-63",
    "14": "01-30,32-38,61-65",
    "16": "01-11,61",
    "18": "01-21,61-64",
    "20": "01-14,61-63",
    "22": "01-16,61-64",
    "24": "01-17,61-79",
    "26": "01-13,61",
    "28": "01-19,61-62",
    "30": "01-31,61-64",
    "32": "01-18,61-63",
}
MATCHED = re.compile(rb'numberMatched="(\d+)"')


def powiats() -> list[str]:
    out = []
    for woj, spans in POWIATS.items():
        for span in spans.split(","):
            low, _, high = span.partition("-")
            out += [f"{woj}{n:02d}" for n in range(int(low), int(high or low) + 1)]
    return out


def parts() -> dict[str, list[tuple[str, str]]]:
    """Part -> (scope, code) of its requests: PL, then each voivodeship with its powiats."""
    out = {"PL": [("PL", "")]}
    for woj in POWIATS:
        out[woj] = [("voivodeship", woj), *[("powiat", p) for p in powiats() if p.startswith(woj)]]
    return out


def params(feature_type: str, code: str) -> dict[str, str]:
    type_name, prop = TYPES[feature_type]
    out = {"SERVICE": "WFS", "VERSION": "2.0.0", "REQUEST": "GetFeature", "TYPENAMES": type_name, "RESULTTYPE": "hits"}
    if code:
        out["FILTER"] = (
            '<fes:Filter xmlns:fes="http://www.opengis.net/fes/2.0">'
            '<fes:PropertyIsLike wildCard="%" singleChar="#" escapeChar="!">'
            f"<fes:ValueReference>{prop}</fes:ValueReference><fes:Literal>{code}%</fes:Literal>"
            "</fes:PropertyIsLike></fes:Filter>"
        )
    return out


def session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    return s


def hits(s: requests.Session, entity: str, code: str) -> tuple[bytes, str]:
    """One hits answer (exact bytes) and its URL; bounded retries with backoff on network errors, 429 and 5xx
    (Retry-After honoured); other statuses, an OGC exception or an answer without numberMatched fail."""
    for attempt in range(RETRIES):
        time.sleep(PAUSE)
        try:
            response = s.get(SERVICE, params=params(entity, code), timeout=(30, 300))
            if response.status_code == 200:
                body = response.content
                if not MATCHED.search(body[:2000]) or b"FeatureCollection" not in body[:2000]:
                    raise RuntimeError(f"{entity} {code or 'PL'}: no numberMatched in the answer: {body[:300]!r}")
                return body, response.url
            if response.status_code not in (429, 500, 502, 503, 504):
                raise RuntimeError(f"{entity} {code or 'PL'}: HTTP {response.status_code}")
            wait = int(response.headers.get("Retry-After") or 0)
        except (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError):
            wait = 0
        time.sleep(max(wait, min(300, 15 * 2**attempt)))
    raise RuntimeError(f"{entity} {code or 'PL'}: kept failing")


def held(client, bucket: str) -> list[dict[str, Any]]:
    """Every receipt of the source, oldest landed first."""
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.landed(client, bucket, f"{LANDING_ROOT}/{SOURCE}/snapshot/data/")


def entities(receipts: list[dict[str, Any]]) -> list[dict[str, str]]:
    """The landed entities as bulk_file specs (Bronze load and verify read them)."""
    return [{"entity": e} for e in sorted({r["entity"] for r in receipts})]


def rounds(receipts: list[dict[str, Any]], feature_type: str) -> dict[str, set[str]]:
    """Round date -> parts landed, of one feature type."""
    out: dict[str, set[str]] = {}
    for r in receipts:
        kind, day, part = r["entity"].split("_")
        if kind == feature_type:
            out.setdefault(day, set()).add(part)
    return out


def plan(receipts: list[dict[str, Any]], feature_type: str, today: str) -> tuple[str, str, list[str]]:
    """(state, round date, parts to read): an open round is continued, a new one started when the newest complete
    round is FRESH_DAYS old or older, else current."""
    held = rounds(receipts, feature_type)
    for day in sorted(held, reverse=True)[:1]:
        todo = [p for p in parts() if p not in held[day]]
        if todo:
            return "open", day, todo
        age = datetime.strptime(today, "%Y%m%d") - datetime.strptime(day, "%Y%m%d")
        if age.days < FRESH_DAYS:
            return "current", day, []
    return ("stale" if held else "missing"), today, list(parts())


def land(client, bucket: str, s: requests.Session, feature_type: str, day: str, part: str, deadline: float):
    """Read one part of a round; upload it with its receipt only when every answer was read (None at the budget)."""
    version = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    entity = f"{feature_type}_{day}_{part}"
    lines, matched = [], {}
    for scope, code in parts()[part]:
        if time.monotonic() > deadline:
            return None
        asked = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        body, url = hits(s, feature_type, code)
        matched[code or "PL"] = int(MATCHED.search(body).group(1))
        line = {"scope": scope, "code": code, "type_name": TYPES[feature_type][0], "property": TYPES[feature_type][1]}
        line.update({"url": url, "requested_at": asked, "http_status": 200})
        line.update({"body_sha256": sha256(body).hexdigest(), "body": body.decode("utf-8")})
        lines.append(json.dumps(line, ensure_ascii=False, sort_keys=True))
    data = ("\n".join(lines) + "\n").encode("utf-8")
    digest, ingested = sha256(data).hexdigest(), datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    key = f"{LANDING_ROOT}/{SOURCE}/snapshot/data/{entity}/{version}__{ingested}.jsonl"
    with tempfile.TemporaryDirectory(prefix="zohelo-gugik-egib-") as temporary:
        path = Path(temporary) / "snapshot.jsonl"
        path.write_bytes(data)
        client.upload_file(
            str(path), bucket, key, ExtraArgs={"ContentType": "application/x-ndjson", "Metadata": {"sha256": digest}}
        )
    head = client.head_object(Bucket=bucket, Key=key)
    if head["ContentLength"] != len(data) or (head.get("Metadata") or {}).get("sha256") != digest:
        raise RuntimeError(f"Landing write could not be verified: {key}")
    powiat_sum = sum(v for k, v in matched.items() if len(k) == 4)
    filtered = matched.get(part, matched.get("PL"))
    receipt = {
        "schema_version": 3,
        "kind": f"{SOURCE}_landing",
        "source_id": SOURCE,
        "entity": entity,
        "content": "snapshot",
        "area": "data",
        "version_token": version,
        "version_basis": "download time",
        "ingested_at": ingested,
        "source_url": SERVICE,
        "final_url": SERVICE,
        "sha256": digest,
        "size_bytes": len(data),
        "object": {"bucket": bucket, "key": key},
        "replaces_sha256": None,
        "code_sha": os.environ.get("GITHUB_SHA", ""),
        "detail": {
            "pattern": "wfs_hits",
            "round": day,
            "part": part,
            "requests": len(lines),
            "matched": filtered,
            "powiat_sum": powiat_sum,
        },
    }
    client.put_object(
        Bucket=bucket,
        Key=key.removesuffix(".jsonl") + ".receipt.json",
        ContentType="application/json",
        Body=json.dumps(receipt, indent=2, sort_keys=True).encode(),
    )
    return {"part": part, "matched": filtered, "powiat_sum": powiat_sum, "bytes": len(data)}


def extract(client, bucket: str, deadline: float) -> tuple[int, dict[str, Any]]:
    """Continue or start the round of each feature type until the deadline; (exit status, results)."""
    s, status, results = session(), 0, {}
    today = datetime.now(UTC).strftime("%Y%m%d")
    for feature_type in TYPES:
        state, day, todo = plan(held(client, bucket), feature_type, today)
        result: dict[str, Any] = {"state": state, "round": day, "landed": [], "left": len(todo)}
        for part in todo:
            try:
                landed = land(client, bucket, s, feature_type, day, part, deadline)
            except (RuntimeError, UnicodeDecodeError, requests.RequestException) as exc:
                result["failed"], status = {"part": part, "error": str(exc)[:200]}, 1
                break
            if landed is None:
                result["stopped_at_budget"] = part
                break
            result["landed"].append(landed)
            result["left"] -= 1
        results[feature_type] = result
    return status, results


def _run(mode: str, p) -> int:
    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate, step_summary
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    have = held(client, bucket)
    if mode == "verify":
        return bulk_file.main(SOURCE, "verify", landing_root=LANDING_ROOT, specs=entities(have))
    today = datetime.now(UTC).strftime("%Y%m%d")
    summary: dict[str, Any] = {"source": SOURCE, "mode": mode, "files": len(have), "powiats": len(powiats())}
    status = 0
    if mode == "inspect":
        for feature_type in TYPES:
            state, day, todo = plan(have, feature_type, today)
            summary[feature_type] = {"state": state, "round": day, "parts_to_read": len(todo)}
        try:
            summary["national"] = {e: int(MATCHED.search(hits(session(), e, "")[0]).group(1)) for e in TYPES}
        except (RuntimeError, requests.RequestException) as exc:
            summary["unreachable"], status = str(exc)[:200], 1
    else:
        status, summary["results"] = extract(client, bucket, time.monotonic() + max(p.budget_minutes - 20, 5) * 60)
    print(json.dumps(summary, sort_keys=True), flush=True)
    annotate("notice" if status == 0 else "error", f"{SOURCE} landing {mode}", summary)
    step_summary(f"## {SOURCE} · landing {mode}\n\n```json\n{json.dumps(summary, indent=2)}\n```\n")
    return status


ACTIVITIES = {
    "landing.inspect": lambda p: _run("inspect", p),
    "landing.extract": lambda p: _run("load", p),
    "landing.verify": lambda p: _run("verify", p),
}
