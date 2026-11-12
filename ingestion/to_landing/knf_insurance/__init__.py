"""KNF insurance and pension statistics Landing activities (register PLG-018).

The Polish Financial Supervision Authority (KNF) publishes its statistics as articles of one paged listing,
https://www.knf.gov.pl/publikacje_i_opracowania/dane_statystyczne: every release is its own article with one to four
workbooks under ``/knf/pl/komponenty/img/<name>_<id>.xlsx``, so a file's URL changes with each release and the stable
page is the listing. Series built here (the article title names the series and the period):

  ofe       "Dane miesięczne OFE - <month> <year>"                 one workbook a month   entity ofe_YYYY_MM
  bulletin  "Biuletyn Kwartalny. Rynek ubezpieczeń <q>/<year>"     parts A-D a quarter    entity bulletin_YYYY_qN_<part>
  ike/ikze  "Informacje o rynku IKE|IKZE ... 31 grudnia <year>"    one workbook a year    entity ike_YYYY, ikze_YYYY
            an edition "... 30 czerwca <year>", if KNF publishes one                    entity ike_YYYY_h1, ikze_YYYY_h1

Entity = one file of one release; version = the response's Last-Modified (KNF sends none: download time), written
only when the SHA-256 changed. An accumulating source: every release is kept (each file holds its own period).

landing.inspect  releases the listing names vs entities in Landing (missing / held); no writes
landing.extract  missing releases first (oldest first), then the newest RECHECK of each series again; one request
                 at a time, PAUSE seconds apart, within the time budget; releases before FIRST_YEAR are ignored
landing.verify   every receipt has its file (bulk_file pattern)

Discovery reads the listing's first page and follows its numbered pages (``?page=<n>``) while a page names a release
from FIRST_YEAR on that is not held, at most MAX_PAGES pages; then one article page per missing or rechecked release.
Workbooks are .xlsx, except the older OFE months, which KNF links as .xls (Dev trial 43297449852): landed as
published, rejected by Bronze with that reason (no .xls reader without a new dependency).
"""

from __future__ import annotations

from datetime import UTC, datetime
import re
import time
from typing import Any

SOURCE = "knf_insurance"
LISTING = "https://www.knf.gov.pl/publikacje_i_opracowania/dane_statystyczne"
FIRST_YEAR = 2023  # releases from 2023 on (the trial profiled their layouts)
MAX_PAGES = 60  # 421 articles on 12 Nov 2026
RECHECK = 1  # the newest release of each series is downloaded again (KNF replaces files in place)
PAUSE = 2.0
MONTHS = {
    "styczen": 1, "luty": 2, "marzec": 3, "kwiecien": 4, "maj": 5, "czerwiec": 6,
    "lipiec": 7, "sierpien": 8, "wrzesien": 9, "pazdziernik": 10, "listopad": 11, "grudzien": 12,
}  # fmt: skip
PLAIN = str.maketrans("ąćęłńóśźżĄĆĘŁŃÓŚŹŻ", "acelnoszzACELNOSZZ")
OFE_TITLE = re.compile(r"Dane miesi[eę]czne OFE\s*[-–]\s*(\S+)\s+(\d{4})", re.IGNORECASE)
BULLETIN_TITLE = re.compile(r"Biuletyn Kwartalny\.?\s*Rynek ubezpiecze[nń]\s+([1-4])\s*/\s*(\d{4})", re.IGNORECASE)
IKE_TITLE = re.compile(r"Informacj\w* o rynku (IKE|IKZE)\b.*?(31 grudnia|30 czerwca) (\d{4})", re.IGNORECASE)
ARTICLE_LINK = re.compile(r"articleId=\d+")
FILE_LINK = re.compile(r"/komponenty/img/[^?#]+\.xlsx?$", re.IGNORECASE)
PART = re.compile(r"/([A-D])_[^/]*$")
PAGE_LINK = re.compile(r"dane_statystyczne\?.*\b(?:page|strona|p_page|pageNumber|cur)=(\d+)", re.IGNORECASE)


def _anchors(url: str) -> list[tuple[str, str]]:
    """(absolute href, link text) of every <a> on a page, with bulk_file's retries and user agent."""
    from html.parser import HTMLParser
    from urllib.parse import urljoin

    import requests

    from ingestion.to_landing.patterns import bulk_file

    class Anchors(HTMLParser):
        def __init__(self) -> None:
            super().__init__(convert_charrefs=True)
            self.out: list[list[str]] = []
            self.depth = 0

        def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
            if tag == "a":
                self.out.append([urljoin(url, (dict(attrs).get("href") or "").strip()), ""])
                self.depth = 1

        def handle_data(self, data: str) -> None:
            if self.depth:
                self.out[-1][1] += " " + data

        def handle_endtag(self, tag: str) -> None:
            if tag == "a":
                self.depth = 0

    for attempt in range(bulk_file.RETRIES):
        try:
            response = requests.get(url, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=60)
            if 400 <= response.status_code < 500 and response.status_code not in bulk_file.RETRY_STATUSES:
                raise bulk_file.PermanentError(f"page {response.status_code}: {url}")
            response.raise_for_status()
            parser = Anchors()
            parser.feed(response.text)
            return [(href, " ".join(text.split())) for href, text in parser.out]
        except bulk_file.PermanentError:
            raise
        except requests.RequestException:
            if attempt == bulk_file.RETRIES - 1:
                raise
            time.sleep(30 * (attempt + 1))
    raise RuntimeError("unreachable")


def release(title: str) -> tuple[str, str] | None:
    """(series, release key) named by an article title, or None: ('ofe', '2026_09'), ('bulletin', '2026_q2'),
    ('ike', '2025'), ('ikze', '2025'), ('ike', '2025_h1') for an edition at 30 June."""
    m = OFE_TITLE.search(title)
    if m:
        month = MONTHS.get(m.group(1).lower().translate(PLAIN))
        return ("ofe", f"{m.group(2)}_{month:02d}") if month else None
    m = BULLETIN_TITLE.search(title)
    if m:
        return "bulletin", f"{m.group(2)}_q{m.group(1)}"
    m = IKE_TITLE.search(title)
    if m:
        return m.group(1).lower(), m.group(3) + ("" if m.group(2).lower().startswith("31") else "_h1")
    return None


def newest_first(key: str) -> str:
    """Sort key of a release key in time order: '2025_h1' (30 June) before '2025' (31 December)."""
    return key + "_12" if re.fullmatch(r"\d{4}", key) else key.replace("_h1", "_06")


def articles(held: set[str], max_pages: int = MAX_PAGES) -> dict[tuple[str, str], str]:
    """{(series, release key): article URL} for every release from FIRST_YEAR on that the listing names.

    The listing is newest first: the next page is read while the current one names a release from FIRST_YEAR on
    that is not held, so a run with everything held reads one page."""
    held_releases = {release_of(e) for e in held}
    found: dict[tuple[str, str], str] = {}
    seen_pages, queue, pages_read = {LISTING}, [LISTING], 0
    while queue and pages_read < max_pages:
        page = queue.pop(0)
        if pages_read:
            time.sleep(PAUSE)
        links = _anchors(page)
        pages_read += 1
        on_page = set()
        for href, text in links:
            named = release(text) if ARTICLE_LINK.search(href) else None
            if named and int(named[1][:4]) >= FIRST_YEAR:
                found.setdefault(named, href)
                on_page.add(named)
        if not on_page - held_releases:
            break
        numbered = sorted(
            {(int(m.group(1)), h) for h, _ in links if (m := PAGE_LINK.search(h)) and h not in seen_pages}
        )
        for _, href in numbered:
            seen_pages.add(href)
            queue.append(href)
    print({"listing_pages_read": pages_read, "releases_found": len(found)}, flush=True)
    return dict(sorted(found.items()))


def files(series: str, key: str, article: str) -> dict[str, str]:
    """{entity: file URL} of one release's article: bulletin parts by the file name's A_..D_ prefix."""
    urls = list(dict.fromkeys(h for h, _ in _anchors(article) if FILE_LINK.search(h)))
    if series != "bulletin":
        if len(urls) != 1:
            raise ValueError(f"{series} {key}: expected one .xls/.xlsx on {article}, found {urls}")
        return {f"{series}_{key}": urls[0]}
    out: dict[str, str] = {}
    for url in urls:
        m = PART.search(url)
        if m:
            out.setdefault(f"bulletin_{key}_{m.group(1).lower()}", url)
    if not out:
        raise ValueError(f"bulletin {key}: no part A-D workbook on {article}, found {urls}")
    return out


def release_of(entity: str) -> tuple[str, str]:
    """(series, release key) of an entity: bulletin_2026_q2_a → ('bulletin', '2026_q2')."""
    series, rest = entity.split("_", 1)
    return series, (rest.rsplit("_", 1)[0] if series == "bulletin" else rest)


def held_entities(landing_root: str = "01_landing") -> set[str]:
    """Entities with a complete landed copy (a receipt: it is written after the file)."""
    from ingestion.common.r2 import req, s3

    client, bucket, out = s3(), req("STORAGE_LANDING_BUCKET_NAME"), set()
    root = f"{landing_root}/{SOURCE}/snapshot/data/"
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=root):
        out |= {
            o["Key"][len(root) :].split("/", 1)[0]
            for o in page.get("Contents", [])
            if o["Key"].endswith(".receipt.json")
        }
    return out


def plan(found: dict[tuple[str, str], str], held: set[str]) -> list[tuple[str, str, str]]:
    """(series, key, article) to read: releases without a landed file and bulletins with fewer than four parts
    landed (a run stopped between parts; 2023 Q4 has no part D, so its article is read again whenever the listing walk
    reaches it), oldest first; then the newest RECHECK releases of each series again."""
    held_releases = {release_of(e) for e in held}
    parts = {r: sum(1 for e in held if release_of(e) == r) for r in held_releases}
    missing = [
        (s, k, a)
        for (s, k), a in found.items()
        if (s, k) not in held_releases or (s == "bulletin" and parts[(s, k)] < 4)
    ]
    recheck = []
    for series in sorted({s for s, _ in found}):
        newest = sorted((k for s, k in found if s == series), key=newest_first)[-RECHECK:]
        recheck += [(series, k, found[(series, k)]) for k in newest if (series, k, found[(series, k)]) not in missing]
    return missing + recheck


def _inspect(p) -> int:
    from ingestion.common.report import annotate

    held = held_entities()
    found = articles(held)
    held_releases = {release_of(e) for e in held}
    missing = [f"{s}_{k}" for s, k in found if (s, k) not in held_releases]
    per_series = {s: sum(1 for x, _ in found if x == s) for s in sorted({s for s, _ in found})}
    summary = {"releases_listed": per_series, "entities_held": len(held), "missing": missing[:24]}
    annotate("notice" if found else "error", f"{SOURCE} landing inspect", summary)
    return 0 if found else 1


def extract(budget_minutes: float, landing_root: str = "01_landing", max_pages: int = MAX_PAGES) -> int:
    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    deadline = time.monotonic() + budget_minutes * 60
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    held = held_entities(landing_root)
    todo = plan(articles(held, max_pages), held)
    results, refused, deferred = [], [], 0
    for n, (series, key, article) in enumerate(todo):
        if time.monotonic() > deadline:
            deferred = len(todo) - n
            break
        time.sleep(PAUSE)
        try:
            targets = files(series, key, article)
        except (ValueError, bulk_file.PermanentError) as exc:
            refused.append({"release": f"{series}_{key}", "error": str(exc)[:160]})
            continue
        for entity, url in targets.items():
            time.sleep(PAUSE)
            spec = {"entity": entity, "url": url, "format": "raw", "version": "last_modified"}
            try:
                results.append(bulk_file.load_file(client, bucket, SOURCE, spec, landing_root))
            except bulk_file.PermanentError as exc:
                refused.append({"entity": entity, "error": str(exc)[:160]})
    summary = {
        "releases_planned": len(todo),
        "files_landed": sum(r["outcome"] != "unchanged" for r in results),
        "files_unchanged": sum(r["outcome"] == "unchanged" for r in results),
        "deferred_to_next_run": deferred,
        "refused": refused,
        "at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    annotate("notice" if not refused else "warning", f"{SOURCE} landing extract", summary)
    return 1 if refused and not results else 0


def _extract(p) -> int:
    return extract(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    specs: list[dict[str, Any]] = [{"entity": e} for e in sorted(held_entities())]
    return bulk_file.main(SOURCE, "verify", specs=specs)


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract, "landing.verify": _verify}
