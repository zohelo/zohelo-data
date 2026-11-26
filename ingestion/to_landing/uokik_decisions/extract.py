#!/usr/bin/env python3
"""UOKiK decisions Landing: the index of the database of decisions of the President of UOKiK, one category at a time.

Route (docs/sources/uokik_decisions.md, register PLG-020 and PLG-021). The decisions database
https://decyzje.uokik.gov.pl/bp/dec_prez.nsf is a Lotus Domino database; its view ``WWW-wszystkie`` answers
``?ReadViewEntries&OutputFormat=JSON`` with ``count`` and ``start`` (a view position such as ``3.12.40``). The view
is categorised: the subject area (``@position`` ``n``), then the year (``n.m``), then one entry per decision
(``n.m.k``) whose single cell is an HTML fragment starting "Numer decyzji:". Only this host is read: it has no
robots.txt (HTTP 404); the main site uokik.gov.pl, whose robots.txt names AI crawlers, is never requested. No
decision PDF is downloaded. Requests go one at a time, PAUSE_SECONDS apart, with long timeouts (the host timed out
in a 2026 probe) and at most RETRIES tries; a timeout or refusal is reported and the run stops there (no hammering).

Reading: the collapsed view gives the categories with their ``@descendants`` (years plus decisions). Each category
is read from its own position in pages of PAGE_ENTRIES (the next page starts at the last entry read, which is
dropped from it) until an entry of the next category; the entries read must equal the category's
``@descendants``, else the category is read once more and then fails (a decision added between two pages).

Reduced before Landing: a decision's cell names the parties, and a party may be a natural person (a sole trader),
so the cell is never stored. Of each decision only the index fields are kept: the view's ``@unid`` and
``@noteid``, the category, the year category, and the labelled values of KEPT_LABELS (decision number, date,
department signature). A cell whose first label is not "Numer decyzji:" or that holds no number fails the
category instead of landing unchecked; other labels are dropped by name (their values are never read into the
document), and the labels seen are counted in the run summary.

Entity = one category, ``category_<slug>`` (slug of the category name, ASCII); version = download time. A
category's document is written only when its SHA-256 differs from its newest landed copy. Every run reads every
category (the index is small; corrections need not change a category's count), categories without a copy first,
then the oldest copies, within the time budget; a category left over is read first by the next run.

Layout (docs/landing.md): ``<root>/uokik_decisions/snapshot/data/<entity>/<ingested>__<ingested>.json`` + receipt.
Modes: inspect (categories of the view vs Landing, one request), load, verify.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, datetime
from hashlib import sha256
import html
import json
import os
import re
import time
from typing import Any
import unicodedata

import requests

from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.patterns import bulk_file

SOURCE = "uokik_decisions"
BASE = "https://decyzje.uokik.gov.pl/bp/dec_prez.nsf/"
VIEW = BASE + "WWW-wszystkie?ReadViewEntries&OutputFormat=JSON"
DOCUMENT = BASE + "WWW-wszystkie/{unid}?OpenDocument"  # the decision's page; never requested here
PAGE_ENTRIES = 500
PAUSE_SECONDS = 1.0
TIMEOUT = (30, 180)  # connect, read: the host answered slowly or not at all in one probe
RETRIES = 3
MAX_PAGES = 200  # pages of one category: beyond this is an error, never an endless loop
FIELDS = ["unid", "noteid", "category", "year_category", "decision_number", "decision_date", "department"]
FIRST_LABEL = "Numer decyzji:"
KEPT_LABELS = {  # label in the cell -> field; the values of every other label are never kept
    "Numer decyzji:": "decision_number",
    "Data decyzji:": "decision_date",
    "Data wydania:": "decision_date",
    "Sygnatura:": "department",
    "Departament:": "department",
    "Komórka:": "department",
}


class Refused(RuntimeError):
    """A 4xx answer (other than 408/429): the run stops, nothing is tried another way."""


class Layout(ValueError):
    """An answer that does not have the shape the parser knows: nothing is guessed from it."""


class Reader:
    """One polite HTTP session: sequential requests, PAUSE_SECONDS between them, bounded retries."""

    def __init__(self, pause: float = PAUSE_SECONDS) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": bulk_file.USER_AGENT, "Accept": "application/json"})
        self.pause, self.requests, self.last, self.slowest, self.retries = pause, 0, 0.0, 0.0, 0

    def json(self, url: str) -> dict[str, Any]:
        for attempt in range(RETRIES):
            wait = self.pause - (time.monotonic() - self.last)
            if self.requests and wait > 0:
                time.sleep(wait)
            self.requests += 1
            started = time.monotonic()
            try:
                response = self.session.get(url, timeout=TIMEOUT)
                self.last = time.monotonic()
                self.slowest = max(self.slowest, self.last - started)
                if 400 <= response.status_code < 500 and response.status_code not in bulk_file.RETRY_STATUSES:
                    raise Refused(f"HTTP {response.status_code}: {url}")
                if response.status_code != 200:
                    raise RuntimeError(f"HTTP {response.status_code}")
                # Domino writes control characters unescaped inside strings: strict=False reads them
                return json.loads(response.content.decode("utf-8", "replace"), strict=False)
            except Refused:
                raise
            except (requests.RequestException, RuntimeError, ValueError) as exc:
                self.last = time.monotonic()
                self.retries += 1
                if attempt == RETRIES - 1:
                    raise RuntimeError(f"{type(exc).__name__}: {str(exc)[:200]}") from exc
                print(json.dumps({"status": "retry", "url": url, "error": str(exc)[:200]}), flush=True)
                time.sleep(30 * (attempt + 1))
        raise RuntimeError("unreachable")


def cells(entry: dict[str, Any]) -> list[str]:
    """The text values of a view entry's cells (text, number, date, or each item of a text list)."""
    out = []
    for column in entry.get("entrydata", []):
        for kind in ("text", "number", "datetime"):
            if kind in column:
                out.append(str(column[kind].get("0", "")))
        if "textlist" in column:
            out.extend(str(item.get("0", "")) for item in column["textlist"].get("text", []))
    return out


def nodes(fragment: str) -> list[str]:
    """The text nodes of an HTML fragment (between tags), entities resolved, white space collapsed, empty ones out."""
    out = []
    for node in re.split(r"(?s)<[^>]*>", fragment):
        node = " ".join(html.unescape(node).split())
        if node:
            out.append(node)
    return out


VALID = {  # a kept value must have this shape, else the category fails: no other text can ride along into Landing
    "decision_number": re.compile(r"^[A-ZĄĆĘŁŃÓŚŹŻ][A-Za-z0-9ĄĆĘŁŃÓŚŹŻ.\-/ ]{1,39}$"),
    "decision_date": re.compile(r"^\d{1,4}[-./]\d{1,2}[-./]\d{1,4}$"),
    "department": re.compile(r"^[A-Za-z0-9ĄĆĘŁŃÓŚŹŻąćęłńóśźż .,\-/()]{1,120}$"),
}


def reduce_cell(fragment: str, seen: Counter) -> dict[str, str]:
    """The kept labelled values of a decision's cell. A label is a text node that starts with one of KEPT_LABELS,
    or any text node of at most 40 characters ending with a colon; its value is the text that follows up to the
    next label. Only the values of KEPT_LABELS are kept, each checked against VALID; every label is counted in
    ``seen`` (labels only, never values)."""
    parts: list[list[str]] = []  # [label, value text, ...]
    for node in nodes(fragment):
        known = next((label for label in KEPT_LABELS if node.startswith(label)), None)
        if known:
            parts.append([known, node[len(known) :].strip()])
        elif node.endswith(":") and len(node) <= 40:
            parts.append([node, ""])
        elif parts:
            parts[-1].append(node)
        else:
            raise Layout(f"decision cell does not start with '{FIRST_LABEL}'")
    if not parts or parts[0][0] != FIRST_LABEL:
        raise Layout(f"decision cell does not start with '{FIRST_LABEL}'")
    out: dict[str, str] = {}
    for label, *values in parts:
        seen[label] += 1
        field = KEPT_LABELS.get(label)
        if field is None:
            continue
        value = " ".join(" ".join(values).split())
        if not VALID[field].match(value):
            raise Layout(f"decision cell: the value of {label!r} has an unknown shape ({len(value)} characters)")
        if field in out and out[field] != value:
            raise Layout(f"decision cell holds two values for {field}")
        out[field] = value
    if not out.get("decision_number"):
        raise Layout("decision cell without a decision number")
    return out


def slug(name: str) -> str:
    ascii_name = unicodedata.normalize("NFKD", name.replace("ł", "l").replace("Ł", "L")).encode("ascii", "ignore")
    return re.sub(r"[^a-z0-9]+", "_", ascii_name.decode().lower()).strip("_")[:60] or "unnamed"


def categories(reader: Reader) -> list[dict[str, Any]]:
    """The top-level categories of the collapsed view: position, name, entity, descendants."""
    top = reader.json(VIEW + "&CollapseView&count=1000")
    out = []
    for entry in top.get("viewentry", []):
        position = str(entry.get("@position", ""))
        names = [c for c in cells(entry) if c.strip()]
        if "." in position or len(names) != 1 or "@descendants" not in entry:
            raise Layout(f"collapsed view entry of unknown shape at position {position!r}")
        name = " ".join(names[0].split())
        out.append(
            {
                "position": position,
                "name": name,
                "entity": f"category_{slug(name)}",
                "descendants": int(entry["@descendants"]),
            }
        )
    if not out:
        raise Layout("the collapsed view lists no categories")
    if len({c["entity"] for c in out}) != len(out):
        raise Layout("two categories share one slug")
    stated = top.get("@toplevelentries")
    if stated is not None and int(stated) != len(out):
        raise Layout(f"collapsed view states {stated} categories, lists {len(out)}")
    return out


def read_category(reader: Reader, category: dict[str, Any], seen: Counter) -> list[dict[str, str]]:
    """Every decision of one category, reduced; the entries read must add up to the category's descendants."""
    top = category["position"]
    for attempt in (1, 2):
        rows: dict[str, dict[str, str]] = {}
        positions: set[str] = set()
        year, start, done = None, top, False
        for _ in range(MAX_PAGES):
            page = reader.json(VIEW + f"&start={start}&count={PAGE_ENTRIES + 1}")
            entries = page.get("viewentry", [])
            fresh = 0
            for entry in entries:
                position = str(entry.get("@position", ""))
                parts = position.split(".")
                if parts[0] != top:
                    done = True
                    break
                if position in positions:
                    continue
                positions.add(position)
                fresh += 1
                if len(parts) == 1:
                    continue
                if len(parts) == 2:
                    names = [c for c in cells(entry) if c.strip()]
                    if len(names) != 1 or entry.get("@unid"):
                        raise Layout(f"year entry of unknown shape at {position}")
                    year = " ".join(names[0].split())
                    continue
                if len(parts) != 3 or not entry.get("@unid") or year is None:
                    raise Layout(f"decision entry of unknown shape at {position}")
                cell = [c for c in cells(entry) if FIRST_LABEL in c]
                if len(cell) != 1:
                    raise Layout(f"decision entry at {position}: {len(cell)} cells hold '{FIRST_LABEL}'")
                kept = reduce_cell(cell[0], seen)
                unid = str(entry["@unid"]).upper()
                if unid in rows:
                    raise Layout(f"decision {unid} listed twice in category {category['name']!r}")
                rows[unid] = {
                    "unid": unid,
                    "noteid": str(entry.get("@noteid", "")),
                    "category": category["name"],
                    "year_category": year,
                    "decision_number": kept.get("decision_number", ""),
                    "decision_date": kept.get("decision_date", ""),
                    "department": kept.get("department", ""),
                }
            if done or not fresh or len(entries) < PAGE_ENTRIES + 1:
                break
            start = str(entries[-1]["@position"])
        else:
            raise Layout(f"category {category['name']!r}: more than {MAX_PAGES} pages")
        read = len(positions) - 1  # the category's own entry is not a descendant
        if read == category["descendants"]:
            return list(rows.values())
        if attempt == 2:
            raise RuntimeError(f"category {category['name']!r}: {read} entries read, {category['descendants']} stated")
    raise RuntimeError("unreachable")


def document(category: dict[str, Any], rows: list[dict[str, str]]) -> bytes:
    """The category's reduced decisions as one JSON document with a stable byte form (rows by @unid, keys sorted)."""
    kept = sorted(({f: row[f] for f in FIELDS} for row in rows), key=lambda row: row["unid"])
    body = {
        "source": SOURCE,
        "category": category["name"],
        "descendants": category["descendants"],
        "fields": FIELDS,
        "rows": kept,
    }
    return json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def landed_entities(client: Any, bucket: str, landing_root: str = "01_landing") -> list[str]:
    """Category entities with a landed copy, from the entity folders (one listing, no receipt read)."""
    found = set()
    base = f"{landing_root}/{SOURCE}/snapshot/data/"
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=base, Delimiter="/"):
        for item in page.get("CommonPrefixes", []):
            name = item["Prefix"].rstrip("/").rsplit("/", 1)[-1]
            if name.startswith("category_"):
                found.add(name)
    return sorted(found)


def landed_specs(landing_root: str = "01_landing") -> list[dict[str, str]]:
    from ingestion.common.r2 import req, s3

    return [{"entity": e} for e in landed_entities(s3(), req("STORAGE_LANDING_BUCKET_NAME"), landing_root)]


def land(client: Any, bucket: str, category: dict[str, Any], body: bytes, landing_root: str, detail: dict) -> str:
    """Write the category's document and its receipt unless the newest landed copy has the same SHA-256."""
    folder = bulk_file.prefix(SOURCE, category["entity"], landing_root)
    held = bulk_file.landed(client, bucket, folder)
    digest = sha256(body).hexdigest()
    if held and held[-1]["sha256"] == digest:
        return "unchanged"
    ingested = bulk_file.stamp(datetime.now(UTC))
    key = f"{folder}{ingested}__{ingested}.json"
    client.put_object(
        Bucket=bucket, Key=key, Body=body, ContentType=bulk_file.CONTENT_TYPES["json"], Metadata={"sha256": digest}
    )
    head = client.head_object(Bucket=bucket, Key=key)
    if head["ContentLength"] != len(body) or (head.get("Metadata") or {}).get("sha256") != digest:
        raise RuntimeError(f"Landing write could not be verified: {key}")
    url = VIEW + f"&start={category['position']}&count={PAGE_ENTRIES + 1}"
    receipt = {
        "schema_version": 3,
        "kind": f"{SOURCE}_landing",
        "source_id": SOURCE,
        "entity": category["entity"],
        "content": "snapshot",
        "area": "data",
        "version_token": ingested,
        "version_basis": "download time",
        "ingested_at": ingested,
        "source_url": url,
        "final_url": url,
        "sha256": digest,
        "size_bytes": len(body),
        "object": {"bucket": bucket, "key": key},
        "replaces_sha256": held[-1]["sha256"] if held else None,
        "code_sha": os.environ.get("GITHUB_SHA", ""),
        "detail": {"pattern": "domino_view", "format": "json", "reduced": "index fields only", **detail},
    }
    client.put_object(
        Bucket=bucket,
        Key=key.rsplit(".", 1)[0] + ".receipt.json",
        ContentType="application/json",
        Body=json.dumps(receipt, indent=2, sort_keys=True).encode(),
    )
    return "landed_new" if not held else "landed_changed"


def newest_copies(client: Any, bucket: str, cats: list[dict[str, Any]], landing_root: str) -> dict[str, dict]:
    out = {}
    for category in cats:
        held = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, category["entity"], landing_root))
        if held:
            out[category["entity"]] = held[-1]
    return out


def load(budget_minutes: float = 30.0, landing_root: str = "01_landing") -> int:
    """landing.extract: every category of the view, those without a copy first, then the oldest copies."""
    from ingestion.common.r2 import req, s3

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    reader, started, seen = Reader(), time.monotonic(), Counter()
    done: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    summary: dict[str, Any] = {"source": SOURCE}
    try:
        cats = categories(reader)
    except (Refused, Layout, RuntimeError) as exc:
        cats, failed = [], [{"category": None, "error": f"{type(exc).__name__}: {str(exc)[:200]}"}]
    held = newest_copies(client, bucket, cats, landing_root)
    order = sorted(cats, key=lambda c: (c["entity"] in held, (held.get(c["entity"]) or {}).get("ingested_at", "")))
    left = 0
    for n, category in enumerate(order):
        if time.monotonic() - started > budget_minutes * 60:
            left = len(order) - n
            break
        try:
            rows = read_category(reader, category, seen)
            body = document(category, rows)
            detail = {"category": category["name"], "rows": len(rows), "descendants": category["descendants"]}
            outcome = land(client, bucket, category, body, landing_root, detail)
            done.append({"entity": category["entity"], "outcome": outcome, "rows": len(rows)})
        except (Refused, Layout, RuntimeError) as exc:
            failed.append({"category": category["name"], "error": f"{type(exc).__name__}: {str(exc)[:200]}"})
            left = len(order) - n - 1
            break  # a timeout, refusal or unknown layout stops the run: no hammering, nothing guessed
    outcomes = Counter(item["outcome"] for item in done)
    summary.update(
        {
            "categories": len(cats),
            "read": len(done),
            "left_for_next_run": left,
            "outcomes": dict(outcomes),
            "decisions_read": sum(item["rows"] for item in done),
            "labels_seen": dict(seen.most_common(12)),
            "requests": reader.requests,
            "retries": reader.retries,
            "slowest_seconds": round(reader.slowest, 1),
            "seconds": round(time.monotonic() - started),
            "failed": failed[:5],
        }
    )
    print(json.dumps({**summary, "entities": done}, ensure_ascii=False, sort_keys=True), flush=True)
    annotate("error" if failed else "notice", f"{SOURCE} landing load", summary)
    step_summary(
        f"## {SOURCE} · landing extract\n\n```json\n{json.dumps(summary, indent=2, ensure_ascii=False)}\n```\n"
    )
    return 1 if failed else 0


def inspect(landing_root: str = "01_landing") -> int:
    """landing.inspect: the categories of the collapsed view (one request) against Landing, without writing. A
    category is ``missing`` without a copy, ``stale`` when its stated descendants differ from the copy's, else
    ``current`` (a correction that keeps the count is found by the next load, not here)."""
    from ingestion.common.r2 import req, s3

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    status, states = 0, Counter()
    summary: dict[str, Any] = {"source": SOURCE}
    try:
        cats = categories(Reader())
        held = newest_copies(client, bucket, cats, landing_root)
        listed = {c["entity"] for c in cats}
        for category in cats:
            copy = held.get(category["entity"])
            if copy is None:
                states["missing"] += 1
            elif (copy.get("detail") or {}).get("descendants") != category["descendants"]:
                states["stale"] += 1
            else:
                states["current"] += 1
        summary.update(
            {
                "categories": len(cats),
                "states": dict(states),
                "entries_stated": sum(c["descendants"] for c in cats),
                "landed_not_listed": sorted(set(landed_entities(client, bucket, landing_root)) - listed),
            }
        )
    except (Refused, Layout, RuntimeError) as exc:
        status = 1
        summary["error"] = f"unreachable: {type(exc).__name__}: {str(exc)[:200]}"
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    annotate("notice" if status == 0 else "error", f"{SOURCE} landing inspect", summary)
    return status


def verify(landing_root: str = "01_landing") -> int:
    """landing.verify: every receipt has its file in Landing or the Archive (bulk_file pattern)."""
    return bulk_file.main(SOURCE, "verify", landing_root=landing_root, specs=landed_specs(landing_root))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--mode", choices=["inspect", "load", "verify"], required=True)
    parser.add_argument("--budget-minutes", type=float, default=30.0)
    parser.add_argument("--landing-root", default="01_landing")
    args = parser.parse_args(argv)
    if args.mode == "inspect":
        return inspect(args.landing_root)
    if args.mode == "verify":
        return verify(args.landing_root)
    return load(min(args.budget_minutes, 60.0), args.landing_root)


if __name__ == "__main__":
    raise SystemExit(main())
