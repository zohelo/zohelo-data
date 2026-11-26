"""PIG-PIB CBDG boreholes (register PL-ENV-014) Landing activities: the public WFS of the Central Geological Database,
subsystem boreholes, paged. dm.pgi.gov.pl, baza.pgi.gov.pl and www.pgi.gov.pl answer runners with a bot-protection
page and are never requested.

https://cbdgmapa.pgi.gov.pl/arcgis/services/cbdg_otwory/MapServer/WFSServer (ArcGIS, WFS 2.0.0, listed as a resource of
dane.gov.pl dataset 677) publishes one feature type, ``cbdg_otwory:Otwory_wiertnicze_CBDG``: one point per borehole with
its CBDG id, name, depth, stratigraphy, drilling purpose and year, elevation, coordinates, location basis, locality,
gmina, powiat, voivodeship, archive, inventory number and four flags (core, geophysics, analyses, studies). One entity,
``boreholes``; version = the download time (the service gives none). The layer is landed whole, as pages of PAGE
features ``<version>__<ingested>__part-<nnn>.json`` (GEOJSON, count/startIndex, one request at a time, PAUSE apart),
each page the exact bytes of the answer with a receipt after it. The server refuses SORTBY ("parameter 'SORTBY' not
supported") but answers pages in OBJECTID order, and the same page read twice was byte-identical (Dev probe
45898682643); so the parts are uploaded only after every page was read and checked (a FeatureCollection, OBJECTIDs
strictly ascending across all pages, so none twice, and their number = the resultType=hits count read first), and not
at all when every page equals the newest landed version's (unchanged: same SHA-256 of the pages).

landing.inspect  hits vs the newest complete landed version: missing / stale (count changed) / unknown (same count, the
                 content is compared at extract); no writes
landing.extract  read every page; land a new version unless unchanged
landing.verify   every receipt has its file (Landing or Archive); no requests to PIG-PIB
"""

from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
from itertools import pairwise
import json
import os
from pathlib import Path
import re
import tempfile
import time
from typing import Any

import requests

SOURCE = "pig_boreholes"
ENTITY = "boreholes"
SERVICE = "https://cbdgmapa.pgi.gov.pl/arcgis/services/cbdg_otwory/MapServer/WFSServer"
TYPE = "cbdg_otwory:Otwory_wiertnicze_CBDG"
LANDING_ROOT = "01_landing"  # a Dev trial points it at a zz_dev_ scratch prefix
USER_AGENT = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
# 25,000 features are about 24 MB and took 27 s, as long as a page of 5,000 (Dev probe 45898682643): 14 pages in all.
PAGE, PAUSE, RETRIES = 25000, 5.0, 4
RECEIPT = re.compile(r"([0-9TZ]+)__([0-9TZ]+)__part-(\d{3})\.receipt\.json")


def _get(s: requests.Session, params: dict[str, Any], path: Path | None = None) -> bytes:
    """One WFS request (streamed to ``path`` when given); bounded retries with backoff on network errors, 429 and
    5xx (Retry-After honoured); other statuses and OGC exception reports fail."""
    for attempt in range(RETRIES):
        time.sleep(PAUSE)
        wait = 0
        try:
            with s.get(
                SERVICE, params={"SERVICE": "WFS", "VERSION": "2.0.0", **params}, timeout=(30, 600), stream=True
            ) as response:
                if response.status_code == 200:
                    body = b"".join(response.iter_content(8 << 20))
                    if b"ExceptionReport" in body[:2000]:
                        raise RuntimeError(f"WFS exception: {body[:300]!r}")
                    if path is not None:
                        path.write_bytes(body)
                    return body
                if response.status_code not in (429, 500, 502, 503, 504):
                    raise RuntimeError(f"WFS {params.get('REQUEST')}: HTTP {response.status_code}")
                wait = int(response.headers.get("Retry-After") or 0)
        except (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError):
            pass
        time.sleep(max(wait, min(300, 15 * 2**attempt)))
    raise RuntimeError(f"WFS {params.get('REQUEST')}: kept failing")


def session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    return s


def hits(s: requests.Session) -> int:
    body = _get(s, {"REQUEST": "GetFeature", "TYPENAMES": TYPE, "RESULTTYPE": "hits"}).decode()
    match = re.search(r'numberMatched="(\d+)"', body)
    if not match:
        raise RuntimeError(f"no numberMatched in the hits answer: {body[:200]}")
    return int(match.group(1))


def folder() -> str:
    return f"{LANDING_ROOT}/{SOURCE}/snapshot/data/{ENTITY}/"


def newest_complete(client, bucket: str) -> dict[str, Any] | None:
    """{version, keys, receipt of part 000} of the newest landed version whose parts are all there, else None."""
    stems: dict[str, list[str]] = {}
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=folder()):
        for item in page.get("Contents", []):
            match = RECEIPT.fullmatch(item["Key"].removeprefix(folder()))
            if match:
                stems.setdefault(match.group(2), []).append(item["Key"])
    for ingested in sorted(stems, reverse=True):
        keys = sorted(stems[ingested])
        first = json.loads(client.get_object(Bucket=bucket, Key=keys[0])["Body"].read())
        if len(keys) == first["part"]["total"]:
            return {"version": ingested, "keys": keys, "receipt": first}
    return None


def check_page(path: Path) -> list[int]:
    """OBJECTIDs of one GEOJSON page; raises when the page is not a FeatureCollection of features with an OBJECTID."""
    doc = json.loads(path.read_bytes())
    if doc.get("type") != "FeatureCollection" or not isinstance(doc.get("features"), list):
        raise RuntimeError(f"page {path.name}: not a GEOJSON FeatureCollection")
    ids = [(f.get("properties") or {}).get("OBJECTID") for f in doc["features"]]
    if any(not isinstance(i, int) for i in ids):
        raise RuntimeError(f"page {path.name}: a feature without an integer OBJECTID")
    return ids


def land(client, bucket: str, s: requests.Session, matched: int, held: dict | None, deadline: float) -> dict:
    """Read every page, check them, then upload the parts with their receipts unless unchanged."""
    total = max(1, -(-matched // PAGE))
    with tempfile.TemporaryDirectory(prefix="zohelo-pig-boreholes-") as temporary:
        pages, ids = [], []
        for index in range(total):
            if time.monotonic() > deadline:
                raise RuntimeError(f"time budget spent after {index} of {total} pages; nothing landed")
            path = Path(temporary) / f"part-{index:03d}.json"
            params = {
                "REQUEST": "GetFeature",
                "TYPENAMES": TYPE,
                "OUTPUTFORMAT": "GEOJSON",
                "COUNT": PAGE,
                "STARTINDEX": index * PAGE,
            }
            body = _get(s, params, path)
            ids += check_page(path)
            pages.append((path, params, sha256(body).hexdigest(), len(body)))
        ascending = all(a < b for a, b in pairwise(ids))
        if len(ids) != matched or not ascending:
            raise RuntimeError(f"{len(ids)} features read (OBJECTIDs ascending: {ascending}), hits said {matched}")
        combined = sha256("".join(p[2] for p in pages).encode()).hexdigest()
        if held and held["receipt"]["detail"].get("version_sha256") == combined:
            return {"outcome": "unchanged", "features": matched, "parts": total}
        ingested = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        for index, (path, params, digest, size) in enumerate(pages):
            key = f"{folder()}{ingested}__{ingested}__part-{index:03d}.json"
            client.upload_file(
                str(path),
                bucket,
                key,
                ExtraArgs={"ContentType": "application/geo+json", "Metadata": {"sha256": digest}},
            )
            head = client.head_object(Bucket=bucket, Key=key)
            if head["ContentLength"] != size or (head.get("Metadata") or {}).get("sha256") != digest:
                raise RuntimeError(f"Landing write could not be verified: {key}")
            source_url = requests.Request("GET", SERVICE, params={"SERVICE": "WFS", "VERSION": "2.0.0", **params})
            source_url = source_url.prepare().url
            receipt = {
                "schema_version": 3,
                "kind": f"{SOURCE}_landing",
                "source_id": SOURCE,
                "entity": ENTITY,
                "content": "snapshot",
                "area": "data",
                "version_token": ingested,
                "version_basis": "download time",
                "ingested_at": ingested,
                "source_url": source_url,
                "final_url": source_url,
                "sha256": digest,
                "size_bytes": size,
                "object": {"bucket": bucket, "key": key},
                "part": {"index": index, "total": total},
                "replaces_sha256": None,
                "code_sha": os.environ.get("GITHUB_SHA", ""),
                "detail": {"pattern": "wfs_paged", "matched": matched, "page_size": PAGE, "version_sha256": combined},
            }
            client.put_object(
                Bucket=bucket,
                Key=key.removesuffix(".json") + ".receipt.json",
                ContentType="application/json",
                Body=json.dumps(receipt, indent=2, sort_keys=True).encode(),
            )
    return {
        "outcome": "landed_changed" if held else "landed_new",
        "features": matched,
        "parts": total,
        "bytes": sum(p[3] for p in pages),
    }


def _run(mode: str, p) -> int:
    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate, step_summary
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    if mode == "verify":
        return bulk_file.main(SOURCE, "verify", landing_root=LANDING_ROOT, specs=[{"entity": ENTITY}])
    held = newest_complete(client, bucket)
    s = session()
    matched = hits(s)
    state = "missing" if held is None else "stale" if held["receipt"]["detail"]["matched"] != matched else "unknown"
    summary: dict[str, Any] = {
        "source": SOURCE,
        "mode": mode,
        "entity": ENTITY,
        "hits": matched,
        "state": state,
        "held_version": held and held["version"],
        "held_features": held and held["receipt"]["detail"]["matched"],
    }
    status = 0
    if mode == "load":
        deadline = time.monotonic() + max(p.budget_minutes - 15, 5) * 60
        try:
            summary.update(land(client, bucket, s, matched, held, deadline))
        except (RuntimeError, ValueError, requests.RequestException) as exc:
            summary.update({"outcome": "failed", "error": str(exc)[:300]})
            status = 1
    print(json.dumps(summary, sort_keys=True), flush=True)
    annotate("notice" if status == 0 else "error", f"{SOURCE} landing {mode}", summary)
    step_summary(f"## {SOURCE} · landing {mode}\n\n```json\n{json.dumps(summary, indent=2)}\n```\n")
    return status


ACTIVITIES = {
    "landing.inspect": lambda p: _run("inspect", p),
    "landing.extract": lambda p: _run("load", p),
    "landing.verify": lambda p: _run("verify", p),
}
