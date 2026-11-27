#!/usr/bin/env python3
"""UOKiK decisions Landing: the index of the database of decisions of the President of UOKiK, one category at a time.

Route (docs/sources/uokik_decisions.md, register PLG-020 and PLG-021): the Lotus Domino view ``WWW-wszystkie`` of
https://decyzje.uokik.gov.pl/bp/dec_prez.nsf as JSON (``?ReadViewEntries&OutputFormat=JSON&start=<pos>&count=<n>``).
The view is categorised: subject area (``@position`` ``n``), year (``n.m``), one entry per decision (``n.m.k``) whose
cell is an HTML fragment starting "Numer decyzji:". Only this host is read (no robots.txt); uokik.gov.pl, whose
robots.txt names AI crawlers, is never requested, and no decision PDF is downloaded. One request at a time,
PAUSE_SECONDS apart, long timeouts, at most RETRIES tries; a refusal, a timeout or an unknown layout stops the run.

A category is read from its own position in pages of PAGE_ENTRIES until an entry of the next category; the entries
read must equal the category's ``@descendants`` (read once more, then fail). Reduced before Landing: the cell names the
parties, and a party can be a natural person, so the cell is never stored; of each decision only the view's ``@unid``
and ``@noteid``, the category, the year category and the decision number and date (KEPT_LABELS) are kept, each
checked against the closed shapes of VALID.
Entity = ``category_<slug>``; version = download time; written only when the SHA-256 changed. Every run reads every
category, those without a copy first, then the oldest copies, within the time budget (restart unit: the category).
Modes: inspect (one request, no writes), load, verify.
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
VIEW = "https://decyzje.uokik.gov.pl/bp/dec_prez.nsf/WWW-wszystkie?ReadViewEntries&OutputFormat=JSON"
PAGE_ENTRIES = 500
PAUSE_SECONDS = 1.0
TIMEOUT = (45, 180)  # connect, read: the host timed out in one probe and refused the runner in others
RETRIES = 3
MAX_PAGES = 200  # pages of one category: beyond this is an error, never an endless loop
FIELDS = ["unid", "noteid", "category", "year_category", "decision_number", "decision_date"]
FIRST_LABEL = "Numer decyzji:"
KEPT_LABELS = {  # label in the cell -> field; the values of every other label are never kept (unverified names)
    "Numer decyzji:": "decision_number",
    "Data decyzji:": "decision_date",
    "Data wydania:": "decision_date",
}
# A kept value must have this closed shape, else the category fails: no free words can ride along into Landing (#3330
# review). Number = department or branch signature (capitals, at most one lower-case "i" as in DOZiK, an optional digit),
# a dash or space, then digit groups joined by / - or . and optional capital initials (RKR-1/2020, RWA 12/2015,
# DKK2-421-12/19/AI). No lower-case word, no second space-separated word: a party's name cannot match.
VALID = {
    "decision_number": re.compile(
        r"^[A-ZĄĆĘŁŃÓŚŹŻ]{1,5}(?:i[A-ZĄĆĘŁŃÓŚŹŻ]{1,3})?\d?(?: ?[-–] ?| )\d{1,5}(?:[-/.]\d{1,5}){0,4}(?:/[A-Z]{1,4})?$"
    ),
    "decision_date": re.compile(r"^\d{1,4}[-./]\d{1,2}[-./]\d{1,4}$"),
}


class Refused(RuntimeError):
    """A 4xx answer other than 408/429: the run stops, nothing is tried another way."""


class Layout(ValueError):
    """An answer of a shape the parser does not know: nothing is guessed from it."""


class Reader:
    """One polite HTTP session: sequential requests, PAUSE_SECONDS between them, bounded retries with backoff."""

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": bulk_file.USER_AGENT, "Accept": "application/json"})
        self.requests, self.retries, self.last, self.slowest = 0, 0, 0.0, 0.0

    def json(self, url: str) -> dict[str, Any]:
        for attempt in range(RETRIES):
            wait = PAUSE_SECONDS - (time.monotonic() - self.last)
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


def reduce_cell(fragment: str, seen: Counter) -> dict[str, str]:
    """The kept labelled values of a decision's cell. A label is a text node starting with one of KEPT_LABELS, or any
    text node of at most 40 characters ending with a colon. A kept label's value is its inline text or the one text node
    after it (a second unlabelled node is a Layout error); another label's text is skipped. Labels are
    counted in ``seen`` (never values)."""
    parts: list[list[str]] = []  # [label, value text, ...]
    for node in re.split(r"(?s)<[^>]*>", fragment):
        node = " ".join(html.unescape(node).split())
        if not node:
            continue
        known = next((label for label in KEPT_LABELS if node.startswith(label)), None)
        if known:
            parts.append([known, node[len(known) :].strip()])
        elif node.endswith(":") and len(node) <= 40:
            parts.append([node, ""])
        elif parts and parts[-1][0] in KEPT_LABELS:
            # a kept label takes one value (inline or the next text node); a further unlabelled node is a layout the
            # parser does not know, never appended to the kept value (it could be a party's name)
            if any(parts[-1][1:]):
                raise Layout(f"decision cell: unlabelled text after the value of {parts[-1][0]!r}")
            parts[-1] = [parts[-1][0], node]
        elif parts:
            parts[-1].append(node)
        else:
            break
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
        if out.get(field, value) != value:
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
        position, names = str(entry.get("@position", "")), [c for c in cells(entry) if c.strip()]
        if "." in position or len(names) != 1 or "@descendants" not in entry:
            raise Layout(f"collapsed view entry of unknown shape at position {position!r}")
        name = " ".join(names[0].split())
        out.append({"position": position, "name": name, "entity": f"category_{slug(name)}",
                    "descendants": int(entry["@descendants"])})  # fmt: skip
    if not out or len({c["entity"] for c in out}) != len(out):
        raise Layout("the collapsed view lists no categories, or two categories share one slug")
    if top.get("@toplevelentries") is not None and int(top["@toplevelentries"]) != len(out):
        raise Layout(f"collapsed view states {top['@toplevelentries']} categories, lists {len(out)}")
    return out


def read_category(reader: Reader, category: dict[str, Any], seen: Counter) -> list[dict[str, str]]:
    """Every decision of one category, reduced; the entries read must add up to the category's descendants."""
    top = category["position"]
    for attempt in (1, 2):
        rows: dict[str, dict[str, str]] = {}
        positions: set[str] = set()
        year, start, done = None, top, False
        for _ in range(MAX_PAGES):
            entries = reader.json(VIEW + f"&start={start}&count={PAGE_ENTRIES + 1}").get("viewentry", [])
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
                if len(parts) == 2:
                    names = [c for c in cells(entry) if c.strip()]
                    if len(names) != 1 or entry.get("@unid"):
                        raise Layout(f"year entry of unknown shape at {position}")
                    year = " ".join(names[0].split())
                elif len(parts) > 2:
                    cell = [c for c in cells(entry) if FIRST_LABEL in c]
                    if len(parts) != 3 or not entry.get("@unid") or year is None or len(cell) != 1:
                        raise Layout(f"decision entry of unknown shape at {position}")
                    unid = str(entry["@unid"]).upper()
                    if unid in rows:
                        raise Layout(f"decision {unid} listed twice in category {category['name']!r}")
                    kept = reduce_cell(cell[0], seen)
                    rows[unid] = {"unid": unid, "noteid": str(entry.get("@noteid", "")), "category": category["name"],
                                  "year_category": year, **{f: kept.get(f, "") for f in FIELDS[4:]}}  # fmt: skip
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
    body = {"source": SOURCE, "category": category["name"], "descendants": category["descendants"], "fields": FIELDS,
            "rows": sorted(({f: r[f] for f in FIELDS} for r in rows), key=lambda r: r["unid"])}  # fmt: skip
    return json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def landed_specs(landing_root: str = "01_landing") -> list[dict[str, str]]:
    """Category entities with a landed copy, from the entity folders (one listing, no receipt read)."""
    from ingestion.common.r2 import req, s3

    found, base = set(), f"{landing_root}/{SOURCE}/snapshot/data/"
    pages = (
        s3()
        .get_paginator("list_objects_v2")
        .paginate(Bucket=req("STORAGE_LANDING_BUCKET_NAME"), Prefix=base, Delimiter="/")
    )
    for page in pages:
        for item in page.get("CommonPrefixes", []):
            name = item["Prefix"].rstrip("/").rsplit("/", 1)[-1]
            if name.startswith("category_"):
                found.add(name)
    return [{"entity": e} for e in sorted(found)]


def land(client: Any, bucket: str, category: dict[str, Any], body: bytes, held: list, landing_root: str) -> str:
    """Write the category's document and its receipt unless the newest landed copy has the same SHA-256."""
    digest = sha256(body).hexdigest()
    if held and held[-1]["sha256"] == digest:
        return "unchanged"
    ingested = bulk_file.stamp(datetime.now(UTC))
    key = f"{bulk_file.prefix(SOURCE, category['entity'], landing_root)}{ingested}__{ingested}.json"
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json", Metadata={"sha256": digest})
    head = client.head_object(Bucket=bucket, Key=key)
    if head["ContentLength"] != len(body) or (head.get("Metadata") or {}).get("sha256") != digest:
        raise RuntimeError(f"Landing write could not be verified: {key}")
    url = VIEW + f"&start={category['position']}&count={PAGE_ENTRIES + 1}"
    receipt = {
        "schema_version": 3, "kind": f"{SOURCE}_landing", "source_id": SOURCE, "entity": category["entity"],
        "content": "snapshot", "area": "data", "version_token": ingested, "version_basis": "download time",
        "ingested_at": ingested, "source_url": url, "final_url": url, "sha256": digest, "size_bytes": len(body),
        "object": {"bucket": bucket, "key": key}, "replaces_sha256": held[-1]["sha256"] if held else None,
        "code_sha": os.environ.get("GITHUB_SHA", ""),
        "detail": {"pattern": "domino_view", "format": "json", "reduced": "index fields only",
                   "category": category["name"], "descendants": category["descendants"]},
    }  # fmt: skip
    client.put_object(Bucket=bucket, Key=key[: -len(".json")] + ".receipt.json", ContentType="application/json",
                      Body=json.dumps(receipt, indent=2, sort_keys=True).encode())  # fmt: skip
    return "landed_changed" if held else "landed_new"


def run(mode: str, budget_minutes: float, landing_root: str) -> int:
    """inspect: categories of the view vs Landing (missing / stale by stated descendants / current), no writes.
    load: every category, those without a copy first, then the oldest copies, within the budget."""
    from ingestion.common.r2 import req, s3

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    reader, started, seen = Reader(), time.monotonic(), Counter()
    summary: dict[str, Any] = {"source": SOURCE, "mode": mode}
    outcomes: Counter = Counter()
    failed, decisions, left = [], 0, 0
    try:
        cats = categories(reader)
    except (Refused, Layout, RuntimeError) as exc:
        cats, failed = [], [f"categories: {type(exc).__name__}: {str(exc)[:200]}"]
    held = {c["entity"]: bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, c["entity"], landing_root))
            for c in cats}  # fmt: skip
    if mode == "inspect":
        for c in cats:
            copy = held[c["entity"]][-1] if held[c["entity"]] else None
            same = copy and (copy.get("detail") or {}).get("descendants") == c["descendants"]
            outcomes["missing" if copy is None else "current" if same else "stale"] += 1
    order = sorted(
        cats, key=lambda c: (bool(held[c["entity"]]), (held[c["entity"]] or [{}])[-1].get("ingested_at", ""))
    )
    for n, category in enumerate(order if mode == "load" else []):
        if time.monotonic() - started > budget_minutes * 60:
            left = len(order) - n
            break
        try:
            rows = read_category(reader, category, seen)
            body = document(category, rows)
            outcomes[land(client, bucket, category, body, held[category["entity"]], landing_root)] += 1
            decisions += len(rows)
        except (Refused, Layout, RuntimeError) as exc:
            failed.append(f"{category['name']}: {type(exc).__name__}: {str(exc)[:200]}")
            left = len(order) - n - 1
            break  # a refusal, timeout or unknown layout stops the run: no hammering, nothing guessed
    summary.update({"categories": len(cats), "outcomes": dict(outcomes), "left_for_next_run": left,
                    "entries_stated": sum(c["descendants"] for c in cats), "decisions_read": decisions,
                    "labels_seen": dict(seen.most_common(12)), "requests": reader.requests, "retries": reader.retries,
                    "slowest_seconds": round(reader.slowest, 1), "seconds": round(time.monotonic() - started),
                    "failed": failed[:5]})  # fmt: skip
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True), flush=True)
    annotate("error" if failed else "notice", f"{SOURCE} landing {mode}", summary)
    step_summary(f"## {SOURCE} · landing {mode}\n\n```json\n{json.dumps(summary, indent=2, ensure_ascii=False)}\n```\n")
    return 1 if failed else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--mode", choices=["inspect", "load", "verify"], required=True)
    parser.add_argument("--budget-minutes", type=float, default=30.0)
    parser.add_argument("--landing-root", default="01_landing")
    args = parser.parse_args(argv)
    if args.mode == "verify":  # every receipt has its file in Landing or the Archive (bulk_file pattern)
        return bulk_file.main(SOURCE, "verify", landing_root=args.landing_root, specs=landed_specs(args.landing_root))
    return run(args.mode, min(args.budget_minutes, 60.0), args.landing_root)


if __name__ == "__main__":
    raise SystemExit(main())
