"""ZUS social insurance yearbook Landing activities (register PLS-010, Rocznik Statystyczny Ubezpieczeń Społecznych).

ZUS publishes the yearbook every three years as one PDF per edition on
https://www.zus.pl/baza-wiedzy/statystyka/rocznik-statystyczny-ubezpieczen-spolecznych (no XLSX, CSV or API; Dev probe
43326914693). The page links each edition under ``/documents/10182/167642/<file name>.pdf/<uuid>``; the file name
carries the edition's years (``Rocznik statystyczny 2021_2023.pdf``). Entity = one Polish edition,
``rocznik_<first year>_<last year>``; version = the file's Last-Modified (download time when absent), written only when
the SHA-256 changed. Editions before FIRST_EDITION, the English copies ("Statistical Yearbook ...") and the 1996-1998
"Informacja statystyczna" are not landed. Restart unit: one edition.

landing.inspect  editions the page links vs Landing (missing / held); no writes
landing.extract  missing editions first (oldest first), then the newest edition again (ZUS replaces a file in place
                 when it corrects it), one download at a time, PAUSE seconds apart
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

import re
import time
from typing import Any
from urllib.parse import unquote_plus

SOURCE = "zus_yearbook"
PAGE = "https://www.zus.pl/baza-wiedzy/statystyka/rocznik-statystyczny-ubezpieczen-spolecznych"
FIRST_EDITION = 2009  # first year of the oldest edition landed (Bronze parses these layouts)
PAUSE = 5.0
MAX_PAGES = 4  # the page lists ten documents per page; older editions sit on the next page
LINK = re.compile(r"/documents/10182/167642/(?P<name>[^/?#]+\.pdf)(?:/|$)", re.IGNORECASE)
YEARS = re.compile(r"\b(?P<first>(?:19|20)\d\d)\s*[-_ ]\s*(?P<last>(?:19|20)\d\d)\b")
NEXT = re.compile(r"_cur=\d+")


def entity_of(file_name: str) -> str | None:
    """The edition a PDF file name stands for (``rocznik_2021_2023``), or None for other documents."""
    name = " ".join(unquote_plus(file_name).split())
    if not re.search(r"rocznik", name, re.IGNORECASE) or re.search(r"statistical|yearbook", name, re.IGNORECASE):
        return None
    m = YEARS.search(name)
    if not m or int(m["first"]) < FIRST_EDITION or not 1 <= int(m["last"]) - int(m["first"]) <= 3:
        return None
    return f"rocznik_{m['first']}_{m['last']}"


def _get(url: str) -> Any:
    """One page, with retries like bulk_file."""
    import requests

    from ingestion.to_landing.patterns import bulk_file

    for attempt in range(bulk_file.RETRIES):
        try:
            response = requests.get(url, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=60)
            if 400 <= response.status_code < 500 and response.status_code not in bulk_file.RETRY_STATUSES:
                raise bulk_file.PermanentError(f"page {response.status_code}: {url}")
            response.raise_for_status()
            return response
        except bulk_file.PermanentError:
            raise
        except requests.RequestException:
            if attempt == bulk_file.RETRIES - 1:
                raise
            time.sleep(30 * (attempt + 1))
    raise RuntimeError("unreachable")


def discover() -> dict[str, str]:
    """{entity: PDF URL} of every edition the yearbook page links (its pages followed, at most MAX_PAGES)."""
    from ingestion.to_landing.patterns import bulk_file

    found: dict[str, str] = {}
    queue, seen = [PAGE], set()
    while queue and len(seen) < MAX_PAGES:
        url = queue.pop(0)
        if url in seen:
            continue
        if seen:
            time.sleep(PAUSE)
        seen.add(url)
        response = _get(url)
        for link in bulk_file.links_matching(response.text, response.url, r"\.pdf|_cur="):
            m = LINK.search(link)
            if m:
                entity = entity_of(m.group("name"))
                if entity:
                    found.setdefault(entity, link)
            elif NEXT.search(link) and link.startswith(PAGE) and link not in seen and link not in queue:
                queue.append(link)
    print({"pages_read": len(seen), "editions_found": len(found)}, flush=True)
    return dict(sorted(found.items()))


def held_entities(landing_root: str = "01_landing") -> set[str]:
    """Editions with a landed copy."""
    from ingestion.common.r2 import req, s3

    client, bucket, out = s3(), req("STORAGE_LANDING_BUCKET_NAME"), set()
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=f"{landing_root}/{SOURCE}/snapshot/data/", Delimiter="/"
    ):
        out |= {p["Prefix"].rstrip("/").rsplit("/", 1)[-1] for p in page.get("CommonPrefixes", [])}
    return out


def plan(found: dict[str, str], held: set[str]) -> list[dict[str, Any]]:
    """Missing editions first (oldest first), then the newest edition again when it is held."""
    spec = {"format": "raw", "version": "last_modified"}
    todo = [{"entity": e, "url": u, **spec} for e, u in found.items() if e not in held]
    newest = max(found, default=None)
    if newest in held:
        todo.append({"entity": newest, "url": found[newest], **spec})
    return todo


def _inspect(p) -> int:
    from ingestion.common.report import annotate

    held = held_entities()
    found = discover()
    summary = {"editions_listed": sorted(found), "missing": sorted(set(found) - held), "held": len(held)}
    annotate("notice" if found else "error", f"{SOURCE} landing inspect", summary)
    return 0 if found else 1


def extract(budget_minutes: float, landing_root: str = "01_landing") -> int:
    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    deadline = time.monotonic() + budget_minutes * 60
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    found = discover()
    todo = plan(found, held_entities(landing_root))
    results, refused, deferred = [], [], 0
    for n, spec in enumerate(todo):
        if time.monotonic() > deadline:
            deferred = len(todo) - n
            break
        time.sleep(PAUSE)
        try:
            results.append(bulk_file.load_file(client, bucket, SOURCE, spec, landing_root))
        except bulk_file.PermanentError as exc:
            refused.append({"entity": spec["entity"], "error": str(exc)[:120]})
    summary = {
        "editions_listed": len(found),
        "planned": len(todo),
        "landed": [r["entity"] for r in results if r["outcome"] != "unchanged"],
        "unchanged": sum(r["outcome"] == "unchanged" for r in results),
        "deferred_to_next_run": deferred,
        "refused": refused,
    }
    annotate("notice" if not refused else "warning", f"{SOURCE} landing extract", summary)
    return 1 if (refused and not results) or not found else 0


def _extract(p) -> int:
    return extract(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "verify", specs=[{"entity": e} for e in sorted(held_entities())])


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract, "landing.verify": _verify}
