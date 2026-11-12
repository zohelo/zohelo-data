"""Senate of the Republic of Poland (register PLG-025): Landing activities, route authorized by decision D36.

No API, export or feed exists: the route is a polite read of server-rendered public pages of www.senat.gov.pl that
robots.txt allows, term XI only. One connection, one request at a time, at least 1.1 seconds after the previous
request ended, a clear user agent; robots.txt is read first in every run and each URL is checked against it (never
/gfx/ or /pdf/). HTTP 401/403/429 or a bot-protection page stops the run and is not retried; 5xx and network errors
are retried twice (30 and 90 s), then the run stops. Every run is bounded (MAX_REQUESTS, MAX_MINUTES) and resumable.

Entities (one HTML page each, ``01_landing/senat/snapshot/data/<entity>/<version>__<ingested>.html``; version =
download time, the server's Last-Modified is the response time; a page is written only when its SHA-256 changed):
  sittings_page_<nn>   the sittings list (/prace/posiedzenia/, page,<n>.html), read again in every run
  senators             the senators list (/sklad/senatorowie/), read again in every run
  day_<id>_<day>       votes page of one sitting day (przebieg,<id>,<day>,glosowania.html): read once; read again
                       while its sitting is not finished, and for the REREAD_FINISHED newest finished sittings
  vote_<id>            print page of one vote (glosowanie-drukuj,<id>.html) with the totals: read once, newest first
Not read: the stenogram and speeches, the per-club pages, senator profiles, photos, PDFs, bip.senat.gov.pl.

landing.inspect  robots.txt and the sittings list against Landing: sittings, days and votes known, held, missing
landing.extract  as above, missing first (days before votes); remaining work is left for the next run
landing.verify   every receipt has its file in Landing or the Archive
"""

from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
import json
import os
import re
import time
from typing import Any
from urllib.robotparser import RobotFileParser

import requests

from ingestion.to_landing.senat import pages

SOURCE = "senat"
BASE = "https://www.senat.gov.pl"
USER_AGENT = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
PAUSE = 1.1  # seconds between the end of one request and the start of the next
MAX_REQUESTS = 2000  # per run; the first load (about 3,500 pages) takes two or three runs
MAX_MINUTES = 75.0
REREAD_FINISHED = 2  # newest finished sittings whose day pages are read again in every run
FINISHED = "Zakończone"
REFUSED = {401, 403, 429}
BOT = re.compile(r"request rejected|captcha|cf-chl|access denied|attention required", re.I)
MARKERS = {  # a page without its marker is not landed (maintenance or error page)
    "sittings": "container-posiedzenia",
    "senators": "senator-kontener",
    "day": "meeting-title",
    "vote": "ajax-szczegoly-glosowania",
}


class Stop(RuntimeError):
    """The run must end now. ``fatal``: a refusal, bot protection, robots.txt, an unexpected page or a server that
    keeps failing (the run fails); not fatal: the run's budget is used (the next run continues)."""

    def __init__(self, message: str, fatal: bool = True) -> None:
        super().__init__(message)
        self.fatal = fatal


class Client:
    """One polite HTTP connection to www.senat.gov.pl, with a request and time budget."""

    def __init__(self, max_requests: int, minutes: float) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "pl", "Accept-Encoding": "identity"})
        self.left, self.deadline, self.last, self.made = max_requests, time.monotonic() + minutes * 60, 0.0, 0
        self.robots: RobotFileParser | None = None

    def budget_left(self) -> bool:
        return self.left > 0 and time.monotonic() < self.deadline

    def get(self, path: str, kind: str) -> requests.Response | None:
        """GET one page; None for a 404 (reported by the caller). Raises Stop on refusal or exhausted budget."""
        url = BASE + path
        if path.startswith(("/gfx/", "/pdf/")) or (self.robots and not self.robots.can_fetch(USER_AGENT, url)):
            raise Stop(f"robots.txt does not allow {path}")
        for attempt in range(3):
            if not self.budget_left():
                raise Stop("request or time budget of the run used", fatal=False)
            wait = PAUSE - (time.monotonic() - self.last)
            if wait > 0:
                time.sleep(wait)
            self.left -= 1
            self.made += 1
            try:
                response = self.session.get(url, timeout=(15, 60), allow_redirects=True)
            except requests.RequestException as exc:
                response, error = None, type(exc).__name__
            finally:
                self.last = time.monotonic()
            if response is not None:
                head = response.content[:6000].decode("utf-8", "replace")
                if response.status_code in REFUSED or BOT.search(head):
                    raise Stop(f"refused: HTTP {response.status_code} on {path} (not retried)")
                if response.status_code == 404:
                    return None
                if response.status_code == 200:
                    if not response.url.startswith(BASE + "/"):
                        raise Stop(f"redirected off the site: {path} -> {response.url}")
                    if kind in MARKERS and MARKERS[kind] not in response.text:
                        raise Stop(f"{path}: HTTP 200 without the {kind} page layout (not landed)")
                    return response
                error = f"HTTP {response.status_code}"
            if attempt < 2:
                print(json.dumps({"status": "retry", "path": path, "error": error}), flush=True)
                time.sleep((30, 90)[attempt])
        raise Stop(f"{path}: {error} after 3 attempts")

    def read_robots(self) -> None:
        response = self.get("/robots.txt", "robots")
        if response is None:
            raise Stop("robots.txt not found: the site is not read without it")
        parser = RobotFileParser(BASE + "/robots.txt")
        parser.parse(response.text.splitlines())
        self.robots = parser


# --- Landing state ------------------------------------------------------------------------------------------------


def folder(landing_root: str) -> str:
    return f"{landing_root}/{SOURCE}/snapshot/data/"


def held_receipts(client, bucket: str, landing_root: str) -> dict[str, str]:
    """{entity: key of its newest receipt}, from one listing (receipts are named <version>__<ingested>)."""
    prefix, newest = folder(landing_root), {}
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            if not key.endswith(".receipt.json"):
                continue
            entity, name = key[len(prefix) :].split("/", 1)
            stamp = name.split("__", 1)[1]
            if entity not in newest or stamp > newest[entity].rsplit("__", 1)[1]:
                newest[entity] = key
    return newest


def receipt(client, bucket: str, key: str) -> dict[str, Any]:
    from ingestion.common.r2 import get_with_backoff

    return json.loads(get_with_backoff(client, bucket, key))


def stamp(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


def land(client, bucket: str, landing_root: str, entity: str, response, held_key: str | None) -> str:
    """Write the page and its receipt unless the newest copy has the same SHA-256; returns the outcome."""
    body = response.content
    digest = sha256(body).hexdigest()
    previous = receipt(client, bucket, held_key) if held_key else None
    if previous and previous["sha256"] == digest:
        return "unchanged"
    ingested = stamp(datetime.now(UTC))
    key = f"{folder(landing_root)}{entity}/{ingested}__{ingested}.html"
    client.put_object(
        Bucket=bucket, Key=key, Body=body, ContentType="text/html; charset=utf-8", Metadata={"sha256": digest}
    )
    head = client.head_object(Bucket=bucket, Key=key)
    if head["ContentLength"] != len(body) or (head.get("Metadata") or {}).get("sha256") != digest:
        raise RuntimeError(f"Landing write could not be verified: {key}")
    record = {
        "schema_version": 3,
        "kind": f"{SOURCE}_landing",
        "source_id": SOURCE,
        "entity": entity,
        "content": "snapshot",
        "area": "data",
        "version_token": ingested,
        "version_basis": "download time",
        "ingested_at": ingested,
        "source_url": response.request.url,
        "final_url": response.url,
        "sha256": digest,
        "size_bytes": len(body),
        "object": {"bucket": bucket, "key": key},
        "replaces_sha256": previous["sha256"] if previous else None,
        "code_sha": os.environ.get("GITHUB_SHA", ""),
        "detail": {
            "pattern": "senat_pages",
            "format": "html",
            "http": {"content_type": response.headers.get("Content-Type"), "etag": response.headers.get("ETag")},
        },
    }
    client.put_object(
        Bucket=bucket,
        Key=key.rsplit(".", 1)[0] + ".receipt.json",
        ContentType="application/json",
        Body=json.dumps(record, indent=2, sort_keys=True).encode(),
    )
    return "landed_changed" if previous else "landed_new"


def read_held(client, bucket: str, key: str) -> bytes:
    """The bytes of a held copy (Landing or Archive), checked against its receipt."""
    from ingestion.common.bulk_bronze import read_raw

    return read_raw(client, bucket, SOURCE, receipt(client, bucket, key))


# --- the work of a run --------------------------------------------------------------------------------------------


def list_sittings(web: Client) -> tuple[list[tuple[str, Any]], list[dict]]:
    """Every sittings list page (responses, in page order) and the sittings they show, newest first."""
    first = web.get("/prace/posiedzenia/", "sittings")
    if first is None:
        raise Stop("the sittings list answered 404")
    parsed = pages.sittings(first.content)
    responses, found = [("sittings_page_01", first)], list(parsed["sittings"])
    for n in range(2, parsed["last_page"] + 1):
        response = web.get(f"/prace/posiedzenia/page,{n}.html", "sittings")
        if response is None:
            raise Stop(f"sittings list page {n} answered 404")
        responses.append((f"sittings_page_{n:02d}", response))
        found += pages.sittings(response.content)["sittings"]
    return responses, found


def work(sittings: list[dict], held: dict[str, str]) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """(days to read: missing first, then the ones read again), as (entity, path)."""
    seen, missing, again, finished = set(), [], [], 0
    for s in sittings:  # newest first
        if s["sitting_id"] in seen:
            continue
        seen.add(s["sitting_id"])
        days = [d for d in s["day_numbers"].split("|") if d]
        if not days:
            continue  # planned: no votes page yet
        is_finished = s["status_text"] == FINISHED
        reread = not is_finished or finished < REREAD_FINISHED
        finished += is_finished
        for d in days:
            entity = f"day_{s['sitting_id']}_{d}"
            path = f"/prace/posiedzenia/przebieg,{s['sitting_id']},{d},glosowania.html"
            if entity not in held:
                missing.append((entity, path))
            elif reread:
                again.append((entity, path))
    return missing, again


def run(
    mode: str,
    landing_root: str = "01_landing",
    max_requests: int = MAX_REQUESTS,
    minutes: float = MAX_MINUTES,
    max_new_votes: int | None = None,
) -> dict[str, Any]:
    """One inspect or extract run; returns the summary (also annotated by the activity)."""
    from ingestion.common.r2 import req, s3

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    web = Client(max_requests, minutes)
    held = held_receipts(client, bucket, landing_root)
    outcomes: dict[str, dict[str, int]] = {}
    summary: dict[str, Any] = {"mode": mode, "landing_root": landing_root, "held_entities_before": len(held)}

    def put(kind: str, entity: str, response) -> None:
        if response is None:
            outcomes.setdefault(kind, {}).setdefault("not_found_404", 0)
            outcomes[kind]["not_found_404"] += 1
            summary.setdefault("not_found_sample", []).append(entity)
            return
        if mode == "extract":
            result = land(client, bucket, landing_root, entity, response, held.get(entity))
            outcomes.setdefault(kind, {}).setdefault(result, 0)
            outcomes[kind][result] += 1
            if result != "unchanged":
                held[entity] = "this run"

    status = 0
    try:
        web.read_robots()
        listed, sittings = list_sittings(web)
        for entity, response in listed:
            put("sittings", entity, response)
        summary["sittings"] = len({s["sitting_id"] for s in sittings})
        summary["sittings_finished"] = len({s["sitting_id"] for s in sittings if s["status_text"] == FINISHED})
        summary["sitting_numbers"] = [
            min(int(s["sitting_number"]) for s in sittings),
            max(int(s["sitting_number"]) for s in sittings),
        ]
        put("senators", "senators", web.get("/sklad/senatorowie/", "senators"))
        missing_days, again = work(sittings, held)
        summary["days_listed"] = sum(
            len([d for d in s["day_numbers"].split("|") if d]) for s in {s["sitting_id"]: s for s in sittings}.values()
        )
        summary["days_missing"] = len(missing_days)
        fresh: dict[str, bytes] = {}
        if mode == "extract":
            for entity, path in missing_days + again:
                response = web.get(path, "day")
                put("days", entity, response)
                if response is not None:
                    fresh[entity] = response.content
        # every vote named on a held or freshly read day page
        votes: dict[str, str] = {}
        for entity in sorted(e for e in held if e.startswith("day_")):
            body = fresh.get(entity) or read_held(client, bucket, held[entity])
            _, sitting_id, day = entity.split("_")
            for v in pages.day_votes(body, sitting_id, day)["votes"]:
                votes[v["vote_id"]] = entity
        todo = sorted((v for v in votes if f"vote_{v}" not in held), key=int, reverse=True)
        summary.update({"votes_known": len(votes), "votes_missing_before": len(todo)})
        if mode == "extract":
            limit = len(todo) if max_new_votes is None else max_new_votes
            for vote_id in todo[:limit]:
                if not web.budget_left():
                    break
                put("votes", f"vote_{vote_id}", web.get(f"/prace/posiedzenia/glosowanie-drukuj,{vote_id}.html", "vote"))
            summary["votes_missing_after"] = sum(1 for v in votes if f"vote_{v}" not in held)
    except Stop as exc:
        summary["stopped"] = str(exc)[:300]
        status = 1 if exc.fatal else 0
    summary.update({"requests": web.made, "outcomes": outcomes, "exit": status})
    return summary


def _report(mode: str, summary: dict[str, Any]) -> int:
    from ingestion.common.report import annotate, step_summary

    annotate("error" if summary["exit"] else "notice", f"{SOURCE} landing {mode}", summary)
    step_summary(f"## {SOURCE} · landing {mode}\n\n```json\n{json.dumps(summary, indent=2, default=str)}\n```\n")
    return summary["exit"]


def inspect(landing_root: str = "01_landing") -> int:
    return _report("inspect", run("inspect", landing_root, max_requests=40))


def extract(budget_minutes: float = MAX_MINUTES, landing_root: str = "01_landing", **limits) -> int:
    return _report("extract", run("extract", landing_root, minutes=min(budget_minutes, MAX_MINUTES), **limits))


def verify(landing_root: str = "01_landing") -> int:
    """Every receipt's file is in Landing, or its SHA-256 is in the Archive."""
    from ingestion.common.r2 import archive_key, archived, req, s3

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    prefix, receipts, data = folder(landing_root), [], set()
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            if item["Key"].endswith(".receipt.json"):
                receipts.append(item["Key"])
            else:
                data.add(item["Key"])
    stored = archived(client, bucket, SOURCE)
    missing = []
    for key in receipts:
        data_key = key.removesuffix(".receipt.json") + ".html"
        if data_key in data:
            continue
        r = receipt(client, bucket, key)
        if stored.get(r["sha256"], {}).get("key") != archive_key(SOURCE, r["sha256"], data_key):
            missing.append(data_key)
    summary = {
        "mode": "verify",
        "receipts": len(receipts),
        "in_landing": len(data),
        "entities": len({k[len(prefix) :].split("/", 1)[0] for k in receipts}),
        "missing_data": len(missing),
        "missing_sample": missing[:5],
        "exit": 1 if missing else 0,
    }
    return _report("verify", summary)


ACTIVITIES = {
    "landing.inspect": lambda p: inspect(),
    "landing.extract": lambda p: extract(p.budget_minutes),
    "landing.verify": lambda p: verify(),
}
