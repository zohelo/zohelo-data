"""Issuer report lists (KNF OAM: ESPI reports, CD-006; GPW report list: EBI reports, CD-007), shared code.

Both sources are public HTML lists of issuer reports, read politely (decision D36): list pages only, one request
at a time, PAUSE_SECONDS apart, the platform's identified User-Agent, bounded retries, never a report document or
an attachment. A refusal (4xx, robots.txt, bot protection) stops the day; it is never tried another way.

What is kept (O32, O33): per report the issuer name, the title (the public headline), the report type or category,
the report number, the date or time as listed, the source's own report id and the link to the report's page, and
the ISIN where the list gives it. A page is reduced to these fields in memory; the page HTML is never stored (the
GPW page also carries prices). Persons are not parsed out of titles.

Landing: one JSON document per source and day of the list (entity ``day_<YYYYMMDD>``), the day's reports sorted by
report id, written only when its SHA-256 differs from the newest landed copy of that day; a day without reports is
not landed. Layout, receipts and verification are those of the bulk_file pattern, so the shared Bronze loader, the
lineage check and the Archive read them unchanged.

Two ways to read, by what the list offers (Dev probes 43321077523 and 43326025983, 12 Nov 2026):
- KNF has a view by day of receipt with its own pager (20 reports a page, about 90 reports a day): ``extract_by_day``
  reads chosen days (``days_to_read``), one day at a time.
- GPW lists reports newest first behind the page's own "Show more" request (15 a page) and holds few EBI reports
  (15 rows covered 11 weeks): ``extract_newest_first`` reads from the top down to the first day it needs.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
import html
import json
import os
import re
import time
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow as pa
import requests

from ingestion.common import bulk_bronze
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.patterns import bulk_file

WARSAW = ZoneInfo("Europe/Warsaw")
PAUSE_SECONDS = 5  # between any two requests to one host
RETRIES = 3
REREAD_DAYS = 3  # the days before today that every run reads again (late reports, a missed run)
BACKFILL_DAYS = {"knf_espi": 45, "gpw_ebi": 365}  # how far back the first runs read (KNF: one whole month held)
MAX_PAGES = 60  # pages of one KNF day or of one GPW pass: beyond this is an error or a stop, never an endless loop
FIELDS = {  # the reduced row of each source: every value is text, as listed
    "knf_espi": ["report_id", "issuer_name", "received_date", "title", "link"],
    "gpw_ebi": ["report_id", "published_text", "category", "report_number", "issuer_name", "isin", "title", "link"],
}


class Refused(RuntimeError):
    """The host answered a request with a 4xx: the day is not read, and nothing else is tried."""


class Layout(ValueError):
    """A page that does not have the layout the parser knows: nothing is guessed from it."""


class Reader:
    """One polite HTTP session: sequential requests, a pause before each one after the first, bounded retries."""

    def __init__(self, pause: float = PAUSE_SECONDS) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": bulk_file.USER_AGENT, "Accept-Language": "pl,en"})
        self.pause, self.requests, self.last = pause, 0, 0.0

    def fetch(self, method: str, url: str, **kwargs: Any) -> str:
        """The response body as text (UTF-8, broken bytes replaced). 4xx except 408/429 raises Refused at once."""
        for attempt in range(RETRIES):
            wait = self.pause - (time.monotonic() - self.last)
            if self.requests and wait > 0:
                time.sleep(wait)
            self.requests += 1
            try:
                response = self.session.request(method, url, timeout=(30, 60), **kwargs)
                self.last = time.monotonic()
                if 400 <= response.status_code < 500 and response.status_code not in bulk_file.RETRY_STATUSES:
                    raise Refused(f"{response.status_code}: {url}")
                if response.status_code != 200:
                    raise RuntimeError(f"HTTP {response.status_code}")
                return response.content.decode("utf-8", "replace")
            except Refused:
                raise
            except (requests.RequestException, RuntimeError) as exc:
                self.last = time.monotonic()
                if attempt == RETRIES - 1:
                    raise RuntimeError(f"{url}: {type(exc).__name__}: {str(exc)[:200]}") from exc
                print(json.dumps({"status": "retry", "url": url, "error": str(exc)[:200]}), flush=True)
                time.sleep(30 * (attempt + 1))
        raise RuntimeError("unreachable")


def text(fragment: str) -> str:
    """The text of an HTML fragment: tags dropped, entities resolved, white space collapsed."""
    return " ".join(html.unescape(re.sub(r"(?s)<[^>]+>", " ", fragment)).split())


# --- KNF OAM list (https://moam.knf.gov.pl/moam.nsf) -------------------------------------------------------------

KNF_BASE = "https://moam.knf.gov.pl/moam.nsf/"
KNF_PAGE = KNF_BASE + "mOAM?readForm&RestrictToCategory={day}&start={start}&count={count}"  # the list's own pager
KNF_PAGE_ROWS = 20
KNF_MESSAGE = re.compile(r"Liczba raport\S+ za dzie\S+ (\d{4}-\d{2}-\d{2}) wynosi (\d+)")
KNF_TR = re.compile(r'(?is)<tr class="(?:even|odd)">(.*?)</tr>')
KNF_CELLS = re.compile(
    r"(?is)^\s*<td[^>]*>(.*?)</td>\s*<td[^>]*>\s*(\d{4}-\d{2}-\d{2})\s*</td>\s*<td[^>]*>\s*"
    r'<a href="?0/([0-9A-Fa-f]{32})"?\s*>(.*?)</a>\s*</td>\s*<td[^>]*>.*?</td>\s*$'
)


def parse_knf(page: str) -> tuple[str | None, int | None, list[dict[str, str]]]:
    """(day, number of reports the page states for the day, the page's rows). Each list row has four cells:
    issuer, date received, title linking the report's page (``0/<document id>``), file link (not kept). A row
    of another shape raises Layout."""
    stated = KNF_MESSAGE.search(page)
    rows = []
    for cells in KNF_TR.findall(page):
        found = KNF_CELLS.match(cells)
        if not found:
            raise Layout(f"KNF list row of unknown shape: {text(cells)[:120]!r}")
        issuer, received, document, title = found.groups()
        rows.append(
            {
                "report_id": document.upper(),
                "issuer_name": text(issuer),
                "received_date": received,
                "title": text(title),
                "link": f"{KNF_BASE}0/{document.upper()}",
            }
        )
    if not stated:
        return None, None, rows
    return stated.group(1), int(stated.group(2)), rows


def read_knf_day(reader: Reader, day: date) -> tuple[int | None, list[dict[str, str]], str]:
    """Every row the list holds for one day of receipt, page by page (the pager's own ``start`` steps of 20),
    with the number the list states for the day. A day whose rows do not add up to that number is read once more
    (a report arrived between two pages) and then fails."""
    first = KNF_PAGE.format(day=day.isoformat(), start=1, count=KNF_PAGE_ROWS)
    for attempt in (1, 2):
        rows: dict[str, dict[str, str]] = {}
        listed, start = None, 1
        for _ in range(MAX_PAGES):
            url = KNF_PAGE.format(day=day.isoformat(), start=start, count=KNF_PAGE_ROWS)
            stated_day, stated, found = parse_knf(reader.fetch("GET", url))
            if stated_day is not None and stated_day != day.isoformat():
                raise Layout(f"KNF list answered for {stated_day}, asked for {day}")
            if any(row["received_date"] != day.isoformat() for row in found):
                raise Layout(f"KNF list of {day} holds a row of another day")
            listed = stated if stated is not None else listed
            rows.update({row["report_id"]: row for row in found})
            start += KNF_PAGE_ROWS
            if len(found) < KNF_PAGE_ROWS or (listed is not None and start > listed):
                break
        else:
            raise Layout(f"KNF list of {day}: more than {MAX_PAGES} pages")
        if listed is None and rows:
            raise Layout(f"KNF list of {day}: rows without the day's stated number of reports")
        if listed is None or len(rows) == listed:
            return listed, list(rows.values()), first
        if attempt == 2:
            raise RuntimeError(f"KNF list of {day}: {len(rows)} rows read, {listed} stated")
    raise RuntimeError("unreachable")


# --- GPW report list (https://www.gpw.pl/espi-ebi-reports) -------------------------------------------------------

GPW_PAGE = "https://www.gpw.pl/espi-ebi-reports"
GPW_REPORT = "https://www.gpw.pl/espi-ebi-report?geru_id={id}"
GPW_ITEM = re.compile(r"(?is)<li\b[^>]*>(.*?)</li>")
GPW_PARTS = re.compile(
    r'(?is)<span class="date">(.*?)</span>\s*<strong class="name">\s*'
    r'<a href="espi-ebi-report\?geru_id=(\d+)[^"]*">(.*?)</a>\s*</strong>.*?<p>(.*?)</p>'
)
GPW_HEAD = re.compile(r"^(\d{2})-(\d{2})-(\d{4}) (\d{2}:\d{2}:\d{2})$")
ISIN = re.compile(r"^(.*\S)\s*\(([A-Z]{2}[A-Z0-9]{9}[0-9])\)$")


def parse_gpw(page: str) -> list[dict[str, str]]:
    """The report rows of a list page or of a "Show more" fragment, ESPI and EBI alike, with ``system`` to tell
    them apart. Each item is ``<date time> | <category> | <system> | <number>``, the issuer with its ISIN in
    brackets linking the report's page, and the title. An item that names a report in another shape raises Layout.
    The price and change shown beside the issuer are not read."""
    rows = []
    for item in GPW_ITEM.findall(page):
        if "geru_id=" not in item:
            continue
        found = GPW_PARTS.search(item)
        if not found:
            raise Layout(f"GPW list item of unknown shape: {text(item)[:120]!r}")
        head = [part.strip() for part in text(found.group(1)).split("|")]
        if len(head) != 4 or not GPW_HEAD.match(head[0]) or head[2] not in {"ESPI", "EBI"}:
            raise Layout(f"GPW list item head of unknown shape: {head!r}")
        issuer = text(found.group(3))
        with_isin = ISIN.match(issuer)
        rows.append(
            {
                "report_id": found.group(2),
                "published_text": head[0],
                "category": head[1],
                "system": head[2],
                "report_number": head[3],
                "issuer_name": with_isin.group(1) if with_isin else issuer,
                "isin": with_isin.group(2) if with_isin else "",
                "title": text(found.group(4)),
                "link": GPW_REPORT.format(id=found.group(2)),
            }
        )
    return rows


GPW_AJAX = "https://www.gpw.pl/ajaxindex.php"  # where the list page's own form posts ("Show more")
GPW_PAGE_ROWS = 15
GPW_FORM = [  # the fields of the page's form ``espi-union-reports`` as the browser sends them, EBI ticked only
    ("action", "GPWEspiReportUnion"),
    ("start", "ajaxSearch"),
    ("page", "espi-ebi-reports"),
    ("format", "html"),
    ("lang", "EN"),
    ("letter", ""),
    ("categoryRaports[]", "EBI"),
    *[("typeRaports[]", kind) for kind in ("RB", "P", "Q", "O", "R")],
    ("searchText", ""),
    ("date", ""),
]


def gpw_day(row: dict[str, str]) -> date:
    found = GPW_HEAD.match(row["published_text"])
    return date(int(found.group(3)), int(found.group(2)), int(found.group(1)))


def gpw_moment(row: dict[str, str]) -> tuple[date, str]:
    return gpw_day(row), GPW_HEAD.match(row["published_text"]).group(4)


def read_gpw_since(reader: Reader, floor: date) -> tuple[dict[date, list[dict[str, str]]], date, int]:
    """The EBI rows of the list published on ``floor`` or later, by day, read newest first, 15 rows a request,
    until a row older than ``floor``, a short page (the end of the list) or MAX_PAGES. Returns (rows by day, the
    first day read in full, rows of other systems dropped). The list must be in publication order, newest first:
    only then is every day after the oldest row read complete; another order raises Layout."""
    rows: dict[str, dict[str, str]] = {}
    previous, covered, other = None, floor, 0
    for page in range(MAX_PAGES):
        form = [*GPW_FORM, ("offset", str(page * GPW_PAGE_ROWS)), ("limit", str(GPW_PAGE_ROWS))]
        found = parse_gpw(reader.fetch("POST", GPW_AJAX, data=form, headers={"X-Requested-With": "XMLHttpRequest"}))
        for row in found:
            moment = gpw_moment(row)
            if previous is not None and moment > previous:
                raise Layout(f"GPW list is not newest first at report {row['report_id']} ({row['published_text']})")
            previous = moment
            if row["system"] != "EBI":
                other += 1
            elif moment[0] >= floor:
                rows[row["report_id"]] = row
        if len(found) < GPW_PAGE_ROWS or (previous is not None and previous[0] < floor):
            break
    else:  # the page limit came first: the oldest day read may be cut, so it is left for a later run
        covered = previous[0] + timedelta(days=1)
    by_day: dict[date, list[dict[str, str]]] = {}
    for row in rows.values():
        if gpw_day(row) >= covered:
            by_day.setdefault(gpw_day(row), []).append(row)
    return by_day, covered, other


# --- days, Landing, Bronze ---------------------------------------------------------------------------------------


def entity(day: date) -> str:
    return f"day_{day:%Y%m%d}"


def document(source: str, day: date, listed: int | None, rows: list[dict[str, str]]) -> bytes:
    """The day's reduced rows as one JSON document with a stable byte form (rows by report id, keys sorted)."""
    fields = FIELDS[source]
    kept = sorted(({f: row[f] for f in fields} for row in rows), key=lambda row: row["report_id"])
    body = {"source": source, "day": day.isoformat(), "listed": listed, "fields": fields, "rows": kept}
    return json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def landed_days(client: Any, bucket: str, source: str, landing_root: str = "01_landing") -> list[date]:
    """Days with a landed copy, from the entity folders of the source (one listing, no receipt is read)."""
    found = set()
    base = f"{landing_root}/{source}/snapshot/data/"
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=base, Delimiter="/"):
        for item in page.get("CommonPrefixes", []):
            name = item["Prefix"].rstrip("/").rsplit("/", 1)[-1]
            if re.fullmatch(r"day_\d{8}", name):
                found.add(datetime.strptime(name[4:], "%Y%m%d").date())
    return sorted(found)


def landed_specs(source: str, landing_root: str = "01_landing") -> list[dict[str, str]]:
    """Every day entity with a landed copy, for verify and the Bronze load."""
    from ingestion.common.r2 import req, s3

    days = landed_days(s3(), req("STORAGE_LANDING_BUCKET_NAME"), source, landing_root)
    return [{"entity": entity(day)} for day in days]


def days_to_read(today: date, held: list[date], reread: int, backfill: int) -> list[tuple[date, str]]:
    """The days of one run, in reading order, each with why it is read:
    ``window``    today and the ``reread`` days before it, oldest first: read on every run;
    ``gap``       working days (Monday to Friday) between the oldest landed day and the window without a copy,
                  oldest first: a day a past run failed on (a public holiday without reports is asked again,
                  one request);
    ``backfill``  every day from ``backfill`` days before today up to the oldest landed day (or the window),
                  newest first, so an interrupted backfill continues where it stopped."""
    window_start, floor = today - timedelta(days=reread), today - timedelta(days=backfill)
    have = set(held)
    oldest = min([d for d in held if d >= floor], default=window_start)
    oldest = min(oldest, window_start)
    plan = [(today - timedelta(days=n), "window") for n in range(reread, -1, -1)]
    day = oldest + timedelta(days=1)
    while day < window_start:
        if day not in have and day.weekday() < 5:
            plan.append((day, "gap"))
        day += timedelta(days=1)
    day = oldest - timedelta(days=1) if oldest in have else oldest
    while day >= floor:
        if day < window_start and day not in have:
            plan.append((day, "backfill"))
        day -= timedelta(days=1)
    return plan


def land_day(
    client: Any, bucket: str, source: str, day: date, body: bytes, url: str, landing_root: str, detail: dict[str, Any]
) -> dict[str, Any]:
    """Write the day's document and its receipt unless the newest landed copy of the day has the same SHA-256."""
    folder = bulk_file.prefix(source, entity(day), landing_root)
    held = bulk_file.landed(client, bucket, folder)
    digest = sha256(body).hexdigest()
    if held and held[-1]["sha256"] == digest:
        return {"day": day.isoformat(), "outcome": "unchanged", "rows": detail["rows"]}
    ingested = bulk_file.stamp(datetime.now(UTC))
    key = f"{folder}{ingested}__{ingested}.json"
    client.put_object(
        Bucket=bucket, Key=key, Body=body, ContentType=bulk_file.CONTENT_TYPES["json"], Metadata={"sha256": digest}
    )
    head = client.head_object(Bucket=bucket, Key=key)
    if head["ContentLength"] != len(body) or (head.get("Metadata") or {}).get("sha256") != digest:
        raise RuntimeError(f"Landing write could not be verified: {key}")
    receipt = {
        "schema_version": 3,
        "kind": f"{source}_landing",
        "source_id": source,
        "entity": entity(day),
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
        "detail": {"pattern": "report_list", "format": "json", "reduced": "report_list", **detail},
    }
    client.put_object(
        Bucket=bucket,
        Key=key.rsplit(".", 1)[0] + ".receipt.json",
        ContentType="application/json",
        Body=json.dumps(receipt, indent=2, sort_keys=True).encode(),
    )
    return {"day": day.isoformat(), "outcome": "landed_new" if not held else "landed_changed", "rows": detail["rows"]}


def finish(source: str, started: float, reader: Reader, done: list[dict], failed: list[dict], extra: dict) -> int:
    """The run's summary (annotation, job summary, log line with every day) and exit code."""
    outcomes: dict[str, int] = {}
    for item in done:
        outcomes[item["outcome"]] = outcomes.get(item["outcome"], 0) + 1
    read = [item["day"] for item in done]
    summary = {
        "source": source,
        **extra,
        "days_read": len(done),
        "first_day": min(read, default=None),
        "last_day": max(read, default=None),
        "outcomes": outcomes,
        "rows_landed": sum(item["rows"] for item in done if item["outcome"].startswith("landed")),
        "requests": reader.requests,
        "seconds": round(time.monotonic() - started),
        "failed": failed[:10],
    }
    print(json.dumps({**summary, "days": done}, sort_keys=True), flush=True)
    annotate("error" if failed else "notice", f"{source} landing load", summary)
    step_summary(f"## {source} · landing extract\n\n```json\n{json.dumps(summary, indent=2)}\n```\n")
    return 1 if failed else 0


def extract_by_day(
    source: str,
    read_day: Any,
    budget_minutes: float = 30.0,
    landing_root: str = "01_landing",
    today: date | None = None,
    reread: int = REREAD_DAYS,
    backfill: int | None = None,
) -> int:
    """landing.extract of a list with a view by day: read the days of ``days_to_read`` with
    ``read_day(reader, day)`` → (listed, rows, url) and land each day that has reports. A window day that fails
    is reported and the run ends with 1 after the other window days were read; a refusal, or a gap or backfill
    day that fails, stops the run there (the next run continues from it). The time budget stops the backfill
    early, never the window."""
    from ingestion.common.r2 import req, s3

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    today = today or datetime.now(WARSAW).date()
    backfill = BACKFILL_DAYS[source] if backfill is None else backfill
    reader, started = Reader(), time.monotonic()
    plan = days_to_read(today, landed_days(client, bucket, source, landing_root), reread, backfill)
    done, failed, left = [], [], 0
    for n, (day, why) in enumerate(plan):
        if why != "window" and time.monotonic() - started > budget_minutes * 60:
            left = len(plan) - n
            break
        try:
            listed, rows, url = read_day(reader, day)
            if not rows:
                done.append({"day": day.isoformat(), "outcome": "no reports", "rows": 0, "why": why})
                continue
            body = document(source, day, listed, rows)
            detail = {"rows": len(rows), "listed": listed}
            done.append({**land_day(client, bucket, source, day, body, url, landing_root, detail), "why": why})
        except (Refused, Layout, RuntimeError) as exc:
            failed.append({"day": day.isoformat(), "why": why, "error": f"{type(exc).__name__}: {str(exc)[:200]}"})
            if why != "window" or isinstance(exc, Refused):
                left = len(plan) - n - 1
                break
    extra = {"today": today.isoformat(), "days_planned": len(plan), "days_left_for_next_run": left}
    return finish(source, started, reader, done, failed, extra)


def extract_newest_first(
    source: str,
    read_since: Any,
    url: str,
    landing_root: str = "01_landing",
    today: date | None = None,
    reread: int = REREAD_DAYS,
    backfill: int | None = None,
) -> int:
    """landing.extract of a list that is only newest first: one pass from the top with
    ``read_since(reader, floor)`` → (rows by day, first day read in full, rows dropped), then each day with
    reports is landed. The pass goes back to the newest landed day or ``reread`` days, whichever is older (so a
    missed run leaves no hole), and ``backfill`` days when nothing is landed yet. Nothing is landed when the
    pass fails."""
    from ingestion.common.r2 import req, s3

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    today = today or datetime.now(WARSAW).date()
    backfill = BACKFILL_DAYS[source] if backfill is None else backfill
    held = landed_days(client, bucket, source, landing_root)
    floor = min(today - timedelta(days=reread), max(held)) if held else today - timedelta(days=backfill)
    reader, started = Reader(), time.monotonic()
    done, failed, extra = [], [], {"today": today.isoformat(), "read_from": floor.isoformat()}
    try:
        by_day, covered, other = read_since(reader, floor)
        extra.update({"complete_from": covered.isoformat(), "rows_of_other_systems_dropped": other})
        for day in sorted(by_day):
            detail = {"rows": len(by_day[day]), "listed": None}
            body = document(source, day, None, by_day[day])
            done.append(land_day(client, bucket, source, day, body, url, landing_root, detail))
    except (Refused, Layout, RuntimeError) as exc:
        failed.append({"day": None, "error": f"{type(exc).__name__}: {str(exc)[:200]}"})
    return finish(source, started, reader, done, failed, extra)


def inspect(source: str, read_today: Any, landing_root: str = "01_landing", today: date | None = None) -> int:
    """landing.inspect: today's list against Landing, without writing. ``read_today(reader, today)`` → (listed,
    rows) is the only reading (KNF: today's pages; GPW: the first page); the other days are counted from the
    Landing folders."""
    from ingestion.common.r2 import req, s3

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    today = today or datetime.now(WARSAW).date()
    held = landed_days(client, bucket, source, landing_root)
    status = 0
    try:
        listed, rows = read_today(Reader(), today)
        copies = bulk_file.landed(client, bucket, bulk_file.prefix(source, entity(today), landing_root))
        if not rows:
            state = "no reports listed today"
        elif not copies:
            state = "missing"
        else:
            same = copies[-1]["sha256"] == sha256(document(source, today, listed, rows)).hexdigest()
            state = "current" if same else "stale"
    except (Refused, Layout, RuntimeError) as exc:
        status, state = 1, f"unreachable: {type(exc).__name__}: {str(exc)[:200]}"
    window = [today - timedelta(days=n) for n in range(REREAD_DAYS, -1, -1)]
    summary = {
        "source": source,
        "today": today.isoformat(),
        "today_state": state,
        "landed_days": len(held),
        "oldest_landed": min(held).isoformat() if held else None,
        "newest_landed": max(held).isoformat() if held else None,
        "window_days_without_copy": [d.isoformat() for d in window if d not in held],
    }
    print(json.dumps(summary, sort_keys=True))
    annotate("notice" if status == 0 else "error", f"{source} landing inspect", summary)
    return status


def verify(source: str, landing_root: str = "01_landing") -> int:
    """landing.verify: every receipt has its file in Landing or the Archive (bulk_file pattern)."""
    return bulk_file.main(source, "verify", landing_root=landing_root, specs=landed_specs(source, landing_root))


def columns(source: str) -> list[tuple[str, str]]:
    """Bronze columns: the entity (day), the listed fields as text, the number the list stated, provenance."""
    return [
        ("entity", "VARCHAR"),
        *[(field, "VARCHAR") for field in FIELDS[source]],
        ("listed", "BIGINT"),
        *bulk_bronze.PROVENANCE,
        ("source_row_number", "BIGINT"),
    ]


def decode(source: str, body: bytes, receipt: dict[str, Any]) -> pa.Table:
    """One Bronze row per report of a landed day document; a document of another shape is rejected."""
    doc = json.loads(body)
    fields = FIELDS[source]
    day = entity(date.fromisoformat(doc["day"]))
    if doc.get("source") != source or doc.get("fields") != fields or day != receipt["entity"]:
        raise ValueError(f"not a {source} day document of {receipt['entity']}")
    if not doc.get("rows"):
        raise ValueError("day document without reports")
    base = bulk_bronze.provenance(receipt)
    rows = []
    for n, row in enumerate(doc["rows"], start=1):
        if sorted(row) != sorted(fields) or not row["report_id"]:
            raise ValueError(f"row {n}: fields {sorted(row)} or an empty report id")
        rows.append({**base, **row, "listed": doc.get("listed"), "source_row_number": n})
    return pa.Table.from_pylist(rows, schema=bulk_bronze.arrow_schema(columns(source)))


def bronze_load(source: str, table: str, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """bronze.load: the newest landed copy of every day → ``bronze.<table>`` (shared loader, one file per copy)."""
    return bulk_bronze.load(
        source,
        (namespace, table),
        (namespace, f"{source}_rejected_files"),
        columns(source),
        lambda body, receipt: decode(source, body, receipt),
        "reports",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=landed_specs(source, landing_root),
    )
