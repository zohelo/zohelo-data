"""GUGiK elevation sheet indexes (WFS "Skorowidze" of NMT, NMPT and the LIDAR measurement data) Landing activities.
No raster, grid or point cloud is ever requested.

Six WFS 2.0.0 services under https://mapy.geoportal.gov.pl/wss/service/PZGIK/ (GML 3.2, paging), one per product and
vertical datum (SERVICES): NumerycznyModelTerenu (NMT, terrain model), NumerycznyModelPokryciaTerenu (NMPT, surface
model) and DanePomiaroweLidar (LIDAR point clouds), each EVRF2007 and KRON86. Each publishes one feature type per year
of the data (``gugik:SkorowidzNMT<YYYY>``, ``SkorowidzNMPT<YYYY>``, ``SkorowidzDanychPomiarowychLIDAR<YYYY>``): one
feature per sheet edition with its sheet id (godło), year, date, assortment, format, spatial characteristic (grid size
or point density), height and position errors, CRS, vertical datum, submission number, download link, PZGiK date and
the sheet footprint (polygon, EPSG:2180). Entity = one service's year (``<product>_<datum>_<YYYY>``, e.g.
``nmt_evrf2007_2024``); version = the download time (the services give no version). A year is landed whole, as pages
of PAGE features ``<version>__<ingested>__part-<nnn>.gml`` (WFS count/startIndex sorted by the download link, one
request at a time, PAUSE apart), each page the exact bytes of the answer with a receipt after it; the parts are
uploaded only after every page of the year was read and checked (a FeatureCollection, numberReturned = members, the
year's total = its resultType=hits count, no feature id twice), and not at all when their content equals the newest
landed version's (unchanged: the SHA-256 of the pages without the answer's timeStamp attribute, which changes on every
read).

A year is ``missing`` (no landed version), ``stale`` (its hits count differs from the newest version's, or it is the
current or the previous year and that version is older than REFRESH_DAYS) or ``current``. Older years are read again
only when their count changes (an edit that keeps the count of an old year is not seen until then). The run stops at
its time budget between years; the next run continues (missing first, newest years first).

landing.inspect  feature types and hits vs Landing: missing / stale / current; no writes
landing.extract  download missing, then stale years, within the budget
landing.verify   every receipt has its file (Landing or Archive); no requests to GUGiK
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import tempfile
import time
from typing import Any
import xml.etree.ElementTree as ET

import requests

SOURCE = "gugik_nmt"
BASE = "https://mapy.geoportal.gov.pl/wss/service/PZGIK/"
# entity prefix -> (service, feature type prefix): the six index services linked from the producer's pages
# (Dev probe 45813550746)
SERVICES = {
    "nmt_evrf2007": ("NumerycznyModelTerenuEVRF2007", "SkorowidzNMT"),
    "nmt_kron86": ("NumerycznyModelTerenuKRON86", "SkorowidzNMT"),
    "nmpt_evrf2007": ("NumerycznyModelPokryciaTerenuEVRF2007", "SkorowidzNMPT"),
    "nmpt_kron86": ("NumerycznyModelPokryciaTerenuKRON86", "SkorowidzNMPT"),
    "lidar_evrf2007": ("DanePomiaroweLidarEVRF2007", "SkorowidzDanychPomiarowychLIDAR"),
    "lidar_kron86": ("DanePomiaroweLidarKRON86", "SkorowidzDanychPomiarowychLIDAR"),
}
LANDING_ROOT = "01_landing"  # a Dev trial points it at a zz_dev_ scratch prefix
USER_AGENT = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
PAGE, PAUSE, RETRIES, REFRESH_DAYS = 5000, 1.0, 4, 28
# Pages without a sort order are not stable (the orthophoto index returned other features for the same page twice, Dev
# probe 45813550746); sorted by the download link, unique per feature, the same page came back identical (Dev probe
# 45814802349); every elevation service accepts the same SORTBY (same probe).
SORT = "gugik:url_do_pobrania ASC"
WFS = "{http://www.opengis.net/wfs/2.0}"
GML_ID = "{http://www.opengis.net/gml/3.2}id"
STAMP = re.compile(rb'\stimeStamp="[^"]*"')  # the answer's creation time, the only part that changes on every read
RECEIPT = re.compile(r"([a-z0-9_]+)/([0-9TZ]+)__([0-9TZ]+)__part-(\d{3})\.receipt\.json")


def _get(session: requests.Session, url: str, params: dict[str, Any], path: Path | None = None) -> bytes:
    """One WFS request (streamed to ``path`` when given); bounded retries with backoff on network errors, 429 and
    5xx (Retry-After honoured); other statuses and OGC exception reports fail."""
    for attempt in range(RETRIES):
        time.sleep(PAUSE)
        try:
            with session.get(url, params=params, timeout=(30, 600), stream=True) as response:
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
            wait = 0
        time.sleep(max(wait, min(300, 15 * 2**attempt)))
    raise RuntimeError(f"WFS {params.get('REQUEST')} {params.get('TYPENAMES', '')}: kept failing")


def session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    return s


def service(entity: str) -> str:
    return f"{BASE}{SERVICES[entity.rsplit('_', 1)[0]][0]}/WFS/Skorowidze"


def catalogue(s: requests.Session) -> dict[str, int]:
    """Entity -> hits count of every year's feature type of every service (a service listing none fails: a changed
    service must not look like an empty index)."""
    out = {}
    for key, (name, prefix) in SERVICES.items():
        url = f"{BASE}{name}/WFS/Skorowidze"
        capabilities = _get(s, url, {"SERVICE": "WFS", "REQUEST": "GetCapabilities"}).decode()
        found = re.findall(rf"<(?:wfs:)?Name>gugik:{prefix}(\d{{4}})</(?:wfs:)?Name>", capabilities)
        if not found:
            raise RuntimeError(f"{name} capabilities list no {prefix}<YYYY> feature type (service changed?)")
        for year in sorted(set(found)):
            entity = f"{key}_{year}"
            body = _get(s, url, {**_query(entity), "RESULTTYPE": "hits"}).decode()
            match = re.search(r'numberMatched="(\d+)"', body)
            if not match:
                raise RuntimeError(f"{entity}: no numberMatched in the hits answer: {body[:200]}")
            out[entity] = int(match.group(1))
    return out


def _query(entity: str) -> dict[str, Any]:
    key, year = entity.rsplit("_", 1)
    return {
        "SERVICE": "WFS",
        "VERSION": "2.0.0",
        "REQUEST": "GetFeature",
        "TYPENAMES": f"gugik:{SERVICES[key][1]}{year}",
    }


def landing_state(client, bucket: str) -> dict[str, dict[str, Any]]:
    """Entity -> {version, parts, receipt of part 000} of its newest complete landed version (all parts present)."""
    prefix = f"{LANDING_ROOT}/{SOURCE}/snapshot/data/"
    stems: dict[tuple[str, str], list[str]] = {}
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            match = RECEIPT.fullmatch(item["Key"].removeprefix(prefix))
            if match:
                stems.setdefault((match.group(1), match.group(2)), []).append(item["Key"])
    out: dict[str, dict[str, Any]] = {}
    for (entity, version), keys in sorted(stems.items(), key=lambda kv: kv[0][1], reverse=True):
        if entity in out:
            continue
        first = json.loads(client.get_object(Bucket=bucket, Key=sorted(keys)[0])["Body"].read())
        if len(keys) == first["part"]["total"]:
            out[entity] = {"version": version, "keys": sorted(keys), "receipt": first}
    return out


def classify(hits: dict[str, int], held: dict[str, dict[str, Any]], now: datetime) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {"missing": [], "stale": [], "current": []}
    recent = {str(now.year), str(now.year - 1)}
    for entity in sorted(hits, reverse=True):
        h = held.get(entity)
        if h is None:
            out["missing"].append(entity)
            continue
        age = now - datetime.strptime(h["version"], "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
        changed = h["receipt"]["detail"]["matched"] != hits[entity]
        out[
            "stale"
            if changed or (entity.rsplit("_", 1)[1] in recent and age > timedelta(days=REFRESH_DAYS))
            else "current"
        ].append(entity)
    return out


def check_page(path: Path) -> list[str]:
    """Feature ids of one page; raises when the page is not a complete FeatureCollection."""
    ids, returned = [], None
    for event, element in ET.iterparse(path, events=("start", "end")):
        if event == "start" and element.tag == WFS + "FeatureCollection":
            returned = element.get("numberReturned")
        elif event == "end" and element.tag == WFS + "member":
            ids += [child.get(GML_ID) for child in element]
            element.clear()
    if returned is None or int(returned) != len(ids):
        raise RuntimeError(f"page {path.name}: numberReturned {returned}, members {len(ids)}")
    return ids


def land_year(client, bucket: str, s: requests.Session, entity: str, matched: int, held: dict | None) -> dict:
    """Read every page of one year, check them, then upload the parts with their receipts unless unchanged."""
    total, url = max(1, -(-matched // PAGE)), service(entity)
    with tempfile.TemporaryDirectory(prefix="zohelo-gugik-nmt-") as temporary:
        pages, ids = [], []
        for index in range(total):
            path = Path(temporary) / f"part-{index:03d}.gml"
            params = {**_query(entity), "COUNT": PAGE, "STARTINDEX": index * PAGE, "SORTBY": SORT}
            body = _get(s, url, params, path)
            ids += check_page(path)
            content = sha256(STAMP.sub(b"", body[:4096], count=1) + body[4096:]).hexdigest()
            pages.append((path, params, sha256(body).hexdigest(), len(body), content))
        if len(ids) != matched or len(set(ids)) != len(ids):
            raise RuntimeError(f"{entity}: {len(ids)} features ({len(set(ids))} distinct) read, hits said {matched}")
        combined = sha256("".join(p[4] for p in pages).encode()).hexdigest()
        if held and held["receipt"]["detail"].get("version_sha256") == combined:
            return {"entity": entity, "outcome": "unchanged", "features": matched}
        ingested = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        folder = f"{LANDING_ROOT}/{SOURCE}/snapshot/data/{entity}/"
        for index, (path, params, digest, size, _content) in enumerate(pages):
            key = f"{folder}{ingested}__{ingested}__part-{index:03d}.gml"
            client.upload_file(
                str(path), bucket, key, ExtraArgs={"ContentType": "application/gml+xml", "Metadata": {"sha256": digest}}
            )
            head = client.head_object(Bucket=bucket, Key=key)
            if head["ContentLength"] != size or (head.get("Metadata") or {}).get("sha256") != digest:
                raise RuntimeError(f"Landing write could not be verified: {key}")
            source_url = requests.Request("GET", url, params=params).prepare().url
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
                "source_url": source_url,
                "final_url": source_url,
                "sha256": digest,
                "size_bytes": size,
                "object": {"bucket": bucket, "key": key},
                "part": {"index": index, "total": total},
                "replaces_sha256": None,
                "code_sha": os.environ.get("GITHUB_SHA", ""),
                "detail": {"pattern": "wfs_index", "matched": matched, "page_size": PAGE, "version_sha256": combined},
            }
            client.put_object(
                Bucket=bucket,
                Key=key.removesuffix(".gml") + ".receipt.json",
                ContentType="application/json",
                Body=json.dumps(receipt, indent=2, sort_keys=True).encode(),
            )
    return {
        "entity": entity,
        "outcome": "landed_changed" if held else "landed_new",
        "features": matched,
        "parts": total,
    }


def _run(mode: str, p) -> int:
    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate, step_summary
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    held = landing_state(client, bucket)
    if mode == "verify":
        return bulk_file.main(SOURCE, "verify", landing_root=LANDING_ROOT, specs=[{"entity": e} for e in sorted(held)])
    s = session()
    hits = catalogue(s)
    state = classify(hits, held, datetime.now(UTC))
    summary: dict[str, Any] = {
        "source": SOURCE,
        "mode": mode,
        "entities": len(hits),
        "features": sum(hits.values()),
        **{k: len(v) for k, v in state.items()},
        "missing_sample": state["missing"][:5],
        "stale_sample": state["stale"][:5],
    }
    status = 0
    if mode == "load":
        deadline = time.monotonic() + max(p.budget_minutes - 20, 5) * 60
        outcomes: dict[str, int] = {}
        failed = []
        for n, entity in enumerate(state["missing"] + state["stale"]):
            if time.monotonic() > deadline:
                summary["stopped_at_budget"] = n
                break
            try:
                result = land_year(client, bucket, s, entity, hits[entity], held.get(entity))
            except (RuntimeError, ET.ParseError, requests.RequestException) as exc:
                failed.append({"entity": entity, "error": str(exc)[:200]})
                continue
            outcomes[result["outcome"]] = outcomes.get(result["outcome"], 0) + 1
        summary.update({"outcomes": outcomes, "failed": len(failed), "failed_sample": failed[:5]})
        status = 1 if failed else 0
    print(json.dumps(summary, sort_keys=True), flush=True)
    annotate("notice" if status == 0 else "error", f"{SOURCE} landing {mode}", summary)
    step_summary(f"## {SOURCE} · landing {mode}\n\n```json\n{json.dumps(summary, indent=2)}\n```\n")
    return status


ACTIVITIES = {
    "landing.inspect": lambda p: _run("inspect", p),
    "landing.extract": lambda p: _run("load", p),
    "landing.verify": lambda p: _run("verify", p),
}
