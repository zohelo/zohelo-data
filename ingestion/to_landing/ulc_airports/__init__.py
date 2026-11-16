"""ULC airport traffic statistics: Landing activities (register PL-ENV-026).

The Civil Aviation Authority (Urząd Lotnictwa Cywilnego) lists on one page every quarterly PDF "statystyki wg
portów lotniczych" (passengers and passenger flight operations per Polish airport, Q1 2010 on). The PDF paths differ
per release, so the listing page is the entry. Entity = the reporting quarter (``<year>_q<n>``) read from the link
text ("Pierwszy kwartał 2026 r.", "Cały 2020 rok" = Q4, "Drugi kwartał 2017-2019" = Q2 2019); version = the file's
Last-Modified, a new copy only when the SHA-256 changed.

landing.inspect  quarters the page lists vs Landing (missing / held); no writes
landing.extract  missing quarters, quarters whose link changed (a corrected file under a new path) and the newest
                 RECHECK quarters again; one request at a time, PAUSE seconds apart
landing.verify   every receipt has its file (bulk_file pattern)

The site carries Imperva Incapsula bot protection: a refusal (HTTP 401/403/429/503) or a challenge page instead of
the listing or a PDF stops the run and is recorded; it is never retried around.
"""

from __future__ import annotations

import re
import time
from typing import Any

SOURCE = "ulc_airports"
LISTING = "https://ulc.gov.pl/statystyki-analizy/statystyki-wg-portow-lotniczych"
PAUSE = 4.0
RECHECK = 2  # newest quarters downloaded again (unchanged bytes are not landed)
ORDINALS = {"pierwszy": 1, "drugi": 2, "trzeci": 3, "czwarty": 4, "cały": 4}  # "cały rok" = the Q4 file
REFUSED = {401, 403, 429, 503}
CHALLENGE = re.compile(rb"_Incapsula_Resource|Incapsula incident|captcha", re.IGNORECASE)


class Refused(RuntimeError):
    """The site refused the request or answered with a bot challenge: recorded, never worked around."""


def entity_of(text: str) -> str | None:
    """``<year>_q<n>`` from a link text; the file size the page appends ("…2022264.61 KB") is cut first."""
    text = re.sub(r"\d{1,3}\.\d{1,2} KB$", "", " ".join(text.split())).lower()
    words = re.findall(r"[a-ząćęłńóśźż]+", text)
    years = [int(y) for y in re.findall(r"(?<!\d)(20\d\d)(?!\d)", text)]
    if not words or words[0] not in ORDINALS or not years:
        return None
    return f"{max(years)}_q{ORDINALS[words[0]]}"


def discover() -> tuple[dict[str, str], list[str]]:
    """({entity: PDF URL} of every quarter the page links, link texts that name no quarter)."""
    import requests

    from ingestion.to_landing.patterns import bulk_file

    response = None
    for attempt in range(bulk_file.RETRIES):
        try:
            response = requests.get(LISTING, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=(30, 60))
            break
        except requests.RequestException:
            if attempt == bulk_file.RETRIES - 1:
                raise
            time.sleep(30 * (attempt + 1))
    if response.status_code in REFUSED or CHALLENGE.search(response.content[:20000]):
        raise Refused(f"listing refused: HTTP {response.status_code}, challenge page: {response.status_code == 200}")
    response.raise_for_status()
    found: dict[str, str] = {}
    unparsed: list[str] = []
    for href, text in _anchors(response.text):
        if not href.lower().split("?")[0].endswith(".pdf"):
            continue
        entity = entity_of(text)
        if entity is None or entity in found:
            unparsed.append(text[:60])
            continue
        found[entity] = href
    return dict(sorted(found.items())), unparsed


def _anchors(html: str) -> list[tuple[str, str]]:
    from html.parser import HTMLParser
    from urllib.parse import urljoin

    class Anchors(HTMLParser):
        def __init__(self) -> None:
            super().__init__(convert_charrefs=True)
            self.out: list[list[str]] = []
            self.open = False

        def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
            if tag == "a":
                self.out.append([urljoin(LISTING, (dict(attrs).get("href") or "").strip()), ""])
                self.open = True

        def handle_data(self, data: str) -> None:
            if self.open:
                self.out[-1][1] += data

        def handle_endtag(self, tag: str) -> None:
            if tag == "a":
                self.open = False

    parser = Anchors()
    parser.feed(html)
    return [(href, " ".join(text.split())) for href, text in parser.out]


def held(landing_root: str = "01_landing") -> dict[str, str]:
    """{entity: source URL of its newest landed copy}."""
    from ingestion.common.r2 import req, s3
    from ingestion.to_landing.patterns import bulk_file

    client, bucket, out = s3(), req("STORAGE_LANDING_BUCKET_NAME"), {}
    for entity in held_entities(landing_root):
        receipts = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, entity, landing_root))
        if receipts:
            out[entity] = receipts[-1].get("source_url", "")
    return out


def held_entities(landing_root: str = "01_landing") -> set[str]:
    """Quarters with a landed copy."""
    from ingestion.common.r2 import req, s3

    client, bucket, out = s3(), req("STORAGE_LANDING_BUCKET_NAME"), set()
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=f"{landing_root}/{SOURCE}/snapshot/data/", Delimiter="/"
    ):
        out |= {p["Prefix"].rstrip("/").rsplit("/", 1)[-1] for p in page.get("CommonPrefixes", [])}
    return out


def plan(found: dict[str, str], landed: dict[str, str]) -> list[dict[str, Any]]:
    """Missing quarters (oldest first), quarters whose link changed, then the newest RECHECK quarters again."""
    spec = {"format": "pdf", "version": "last_modified", "retries": 3, "read_timeout": 120, "wait": 30}
    missing = [e for e in found if e not in landed]
    moved = [e for e in found if e in landed and landed[e] != found[e]]
    recheck = [e for e in sorted(found)[-RECHECK:] if e in landed and e not in moved]
    return [{"entity": e, "url": found[e], **spec} for e in missing + moved + recheck]


def _inspect(p) -> int:
    from ingestion.common.report import annotate

    landed = held_entities()
    try:
        found, unparsed = discover()
    except Refused as exc:
        annotate("error", f"{SOURCE} landing inspect", {"refused": str(exc)})
        return 1
    missing = sorted(set(found) - landed)
    summary = {"quarters_listed": len(found), "held": len(landed & set(found)), "missing": missing[:24]}
    summary.update({"newest_listed": max(found) if found else None, "links_without_quarter": unparsed[:5]})
    annotate("notice" if found else "error", f"{SOURCE} landing inspect", summary)
    return 0 if found else 1


def extract(budget_minutes: float, landing_root: str = "01_landing") -> int:
    import requests

    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    deadline = time.monotonic() + budget_minutes * 60
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    try:
        found, unparsed = discover()
    except Refused as exc:
        annotate("error", f"{SOURCE} landing extract", {"refused": str(exc)})
        return 1
    todo = plan(found, held(landing_root))
    results, failed, deferred, refused = [], [], 0, None
    for n, spec in enumerate(todo):
        if time.monotonic() > deadline:
            deferred = len(todo) - n
            break
        time.sleep(PAUSE)
        try:
            results.append(bulk_file.load_file(client, bucket, SOURCE, spec, landing_root))
        except bulk_file.PermanentError as exc:
            if any(f" {code}:" in str(exc) for code in REFUSED) or "not a PDF" in str(exc):
                refused = {"entity": spec["entity"], "error": str(exc)[:160]}
                break  # a refusal or a challenge page stops the run
            failed.append({"entity": spec["entity"], "error": str(exc)[:160]})
        except (RuntimeError, requests.RequestException) as exc:  # after bulk_file's bounded retries
            failed.append({"entity": spec["entity"], "error": f"{type(exc).__name__}: {exc}"[:160]})
    summary = {
        "listed": len(found),
        "planned": len(todo),
        "landed": sum(r["outcome"] != "unchanged" for r in results),
        "unchanged": sum(r["outcome"] == "unchanged" for r in results),
        "failed": failed,
        "refused": refused,
        "deferred_to_next_run": deferred,
        "links_without_quarter": unparsed[:5],
    }
    annotate("warning" if failed or refused else "notice", f"{SOURCE} landing extract", summary)
    return 1 if refused or (failed and not results) else 0


def _extract(p) -> int:
    return extract(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "verify", specs=[{"entity": e} for e in sorted(held_entities())])


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract, "landing.verify": _verify}
