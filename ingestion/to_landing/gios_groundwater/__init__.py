"""GIOŚ groundwater quality monitoring Landing activities (register PL-ENV-012).

The groundwater portal of GIOŚ (mjwp.gios.gov.pl, "Wyniki badań") has one result page per year (2008-2025) and two
for the earlier years (1991-2003, 2004-2007), all linked from every result page's menu. Each page links its XLSX
workbooks under hash names (``/g2/oryginal/<yyyy_mm>/<md5>.xlsx``, a new name when a file is re-issued); the link
text names the year and the workbook, e.g. "2025 - Wyniki badań wskaźników fizykochemicznych NIEORGANICZNYCH -
monitoring jakości wód podziemnych - monitoring operacyjny". Entity = workbook kind x year (``inorganic_2025``),
read from the link text:

  inorganic          "Wyniki badań wskaźników fizykochemicznych NIEORGANICZNYCH" (1991-)
  organic            "Wyniki badań wskaźników fizykochemicznych ORGANICZNYCH" (2009-)
  classes            "Klasy jakości wód podziemnych" (final quality class per point, 2004-)
  nitrate_inorganic  "... NIEORGANICZNYCH - uzupełniające badania stężeń azotanów" (2025-)
  nitrate_classes    "Klasy jakości wód podziemnych wg stężeń POSZCZEGÓLNYCH WSKAŹNIKÓW – ... azotanów" (2025-)

Version = the file's Last-Modified; a copy is written only when its SHA-256 changed. The link text goes into the
receipt (``detail.link_text``): Bronze keeps it, Silver reads the monitoring network from it. A workbook link whose
text names no known kind, or a second link of one entity with another URL, is reported and not landed.

landing.inspect  workbooks listed on the result pages vs Landing (missing / held / URL changed); no writes
landing.extract  missing entities first, then held entities whose URL changed (a re-issued file) and the
                 entities of the newest RECHECK years; one download at a time, PAUSE seconds apart
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

import re
import time
from typing import Any

SOURCE = "gios_groundwater"
START = "https://mjwp.gios.gov.pl/wyniki-badan/wyniki-badan-2025.html"  # any result page; its menu lists them all
PAGE = re.compile(r"^https?://mjwp\.gios\.gov\.pl/wyniki-badan/wyniki-badan-[0-9a-z-]+\.html$")
PAGE_TEXT = re.compile(r"^\d{4}(-\d{4})?$")  # menu entries: "2025", "1991-2003"
WORKBOOK = re.compile(r"^https?://mjwp\.gios\.gov\.pl/g2/oryginal/[0-9_]+/[0-9a-f]+\.xlsx$", re.IGNORECASE)
LINK_YEAR = re.compile(r"^(\d{4})\s*-\s*")
RECHECK = 2  # newest years whose workbooks are downloaded again on every run (corrections land as new versions)
PAUSE = 10.0  # seconds between requests: the server answered a burst with a short page without links (#2870)
FIRST_YEAR = 1991


def kind_of(text: str) -> str | None:
    """Workbook kind from the link text (see the module docstring); None for a text that names no known kind."""
    low = " ".join(text.split()).lower()
    nitrate = "azotanów" in low
    if "klasy jakości" in low:
        if nitrate:
            return "nitrate_classes" if "poszczególnych" in low else None
        return "classes"
    if "nieorganicznych" in low:
        return "nitrate_inorganic" if nitrate else "inorganic"
    if "organicznych" in low and not nitrate:
        return "organic"
    return None


class PageWithoutLinks(RuntimeError):
    """A result page answered without its links (a short page instead of the portal page): reported with its status,
    headers and start, never retried around (docs/landing.md: polite access, never around bot protection)."""


def _links(url: str) -> list[tuple[str, str]]:
    """(absolute href, link text) of every <a> on a page; PageWithoutLinks when the page has none."""
    from html.parser import HTMLParser
    from urllib.parse import urljoin

    import requests

    from ingestion.to_landing.patterns import bulk_file

    class Anchors(HTMLParser):
        def __init__(self) -> None:
            super().__init__(convert_charrefs=True)
            self.out: list[list[str]] = []
            self.open = False

        def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
            if tag == "a":
                self.out.append([urljoin(url, (dict(attrs).get("href") or "").strip()), ""])
                self.open = True

        def handle_data(self, data: str) -> None:
            if self.open:
                self.out[-1][1] += data

        def handle_endtag(self, tag: str) -> None:
            if tag == "a":
                self.open = False

    for attempt in range(bulk_file.RETRIES):
        try:
            response = requests.get(url, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=60)
            if 400 <= response.status_code < 500 and response.status_code not in bulk_file.RETRY_STATUSES:
                raise bulk_file.PermanentError(f"page {response.status_code}: {url}")
            response.raise_for_status()
            parser = Anchors()
            parser.feed(response.text)
            if not parser.out:
                headers = {k: v for k, v in response.headers.items() if k.lower() not in {"set-cookie"}}
                raise PageWithoutLinks(
                    f"{url}: status {response.status_code}, {len(response.content)} bytes, no links; headers "
                    f"{headers}; start {' '.join(response.text.split())[:400]!r}"
                )
            return [(href, " ".join(text.split())) for href, text in parser.out]
        except bulk_file.PermanentError:
            raise
        except requests.RequestException:
            if attempt == bulk_file.RETRIES - 1:
                raise
            time.sleep(30 * (attempt + 1))
    raise RuntimeError("unreachable")


def pages() -> dict[str, str]:
    """{menu text: page URL} of every result page, from the menu of START (which is itself in the menu)."""
    found: dict[str, str] = {}
    for href, text in _links(START):
        if PAGE.match(href) and PAGE_TEXT.match(text):
            found.setdefault(text, href)
    if not found:
        raise PageWithoutLinks(f"{START}: no result-page menu")
    return dict(sorted(found.items()))


def discover() -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """({entity: {url, link_text, page}}, report) over every result page, one page at a time."""
    found: dict[str, dict[str, Any]] = {}
    report: dict[str, Any] = {"pages": 0, "pages_without_workbooks": [], "unclassified": [], "conflicts": []}
    for label, page in pages().items():
        time.sleep(PAUSE)
        links = _links(page)
        report["pages"] += 1
        workbooks = [(h, t) for h, t in links if WORKBOOK.match(h) and t]  # untitled repeats of a link are skipped
        if not workbooks:
            report["pages_without_workbooks"].append(label)
        for href, text in workbooks:
            year, kind = LINK_YEAR.match(text), kind_of(text)
            if not year or not kind or int(year.group(1)) < FIRST_YEAR:
                report["unclassified"].append({"page": label, "text": text[:120]})
                continue
            entity = f"{kind}_{year.group(1)}"
            held = found.get(entity)
            if held and held["url"] != href:
                report["conflicts"].append({"entity": entity, "urls": [held["url"], href]})
                held["conflict"] = True
                continue
            found.setdefault(entity, {"url": href, "link_text": text, "page": page})
    conflicted = {e for e, v in found.items() if v.get("conflict")}
    return {e: v for e, v in sorted(found.items()) if e not in conflicted}, report


def held_receipts(landing_root: str = "01_landing") -> dict[str, dict[str, Any]]:
    """{entity: newest receipt} of every landed entity."""
    from ingestion.common.r2 import req, s3
    from ingestion.to_landing.patterns import bulk_file

    client, bucket, out = s3(), req("STORAGE_LANDING_BUCKET_NAME"), {}
    for entity in held_entities(landing_root):
        held = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, entity, landing_root))
        if held:
            out[entity] = held[-1]
    return out


def held_entities(landing_root: str = "01_landing") -> set[str]:
    """Entities with a landed copy."""
    from ingestion.common.r2 import req, s3

    client, bucket, out = s3(), req("STORAGE_LANDING_BUCKET_NAME"), set()
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=f"{landing_root}/{SOURCE}/snapshot/data/", Delimiter="/"
    ):
        out |= {p["Prefix"].rstrip("/").rsplit("/", 1)[-1] for p in page.get("CommonPrefixes", [])}
    return out


def year_of(entity: str) -> int:
    return int(entity.rsplit("_", 1)[1])


def plan(found: dict[str, dict[str, Any]], held: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """Missing entities first (oldest first), then held ones whose URL changed or whose year is among the newest
    RECHECK years listed."""
    newest = sorted({year_of(e) for e in found})[-RECHECK:]

    def spec(entity: str) -> dict[str, Any]:
        item = found[entity]
        return {
            "entity": entity,
            "url": item["url"],
            "format": "raw",  # XLSX, kept as exact bytes
            "version": "last_modified",
            "detail": {"link_text": item["link_text"], "page": item["page"]},
        }

    missing = [spec(e) for e in found if e not in held]
    again = [
        spec(e) for e in found if e in held and (held[e].get("source_url") != found[e]["url"] or year_of(e) in newest)
    ]
    return missing + again


def _inspect(p) -> int:
    from ingestion.common.report import annotate

    held = held_receipts()
    try:
        found, report = discover()
    except PageWithoutLinks as exc:
        annotate("error", f"{SOURCE} landing inspect: page without links", {"error": str(exc)[:3000]})
        return 1
    missing = sorted(set(found) - set(held))
    moved = sorted(e for e in found if e in held and held[e].get("source_url") != found[e]["url"])
    summary = {
        "listed": len(found),
        "held": len(set(held) & set(found)),
        "missing": missing[:40],
        "url_changed": moved[:20],
        "held_not_listed": sorted(set(held) - set(found))[:20],
        "years": f"{min(map(year_of, found))}-{max(map(year_of, found))}" if found else None,
        **report,
    }
    bad = not found or report["conflicts"] or report["unclassified"]
    annotate("warning" if bad else "notice", f"{SOURCE} landing inspect", summary)
    return 1 if not found else 0


def extract(budget_minutes: float, landing_root: str = "01_landing") -> int:
    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    deadline = time.monotonic() + budget_minutes * 60
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    try:
        found, report = discover()
    except PageWithoutLinks as exc:
        annotate("error", f"{SOURCE} landing extract: page without links", {"error": str(exc)[:3000]})
        return 1
    todo = plan(found, held_receipts(landing_root))
    results, refused, deferred = [], [], 0
    for n, spec in enumerate(todo):
        if time.monotonic() > deadline:
            deferred = len(todo) - n
            break
        if n:
            time.sleep(PAUSE)
        try:
            results.append(bulk_file.load_file(client, bucket, SOURCE, spec, landing_root))
        except bulk_file.PermanentError as exc:
            refused.append({"entity": spec["entity"], "error": str(exc)[:120]})
    summary = {
        "listed": len(found),
        "planned": len(todo),
        "landed": sum(r["outcome"] != "unchanged" for r in results),
        "unchanged": sum(r["outcome"] == "unchanged" for r in results),
        "bytes_landed": sum(round(r.get("size_mb", 0) * 1e6) for r in results if r["outcome"] != "unchanged"),
        "deferred_to_next_run": deferred,
        "refused": refused,
        **{k: v for k, v in report.items() if v},
    }
    annotate("notice" if not refused else "warning", f"{SOURCE} landing extract", summary)
    return 1 if refused and not results else 0


def _extract(p) -> int:
    return extract(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "verify", specs=[{"entity": e} for e in sorted(held_entities())])


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract, "landing.verify": _verify}
