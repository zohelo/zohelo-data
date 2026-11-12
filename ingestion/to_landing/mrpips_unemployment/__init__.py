"""MRPiPS registered unemployment Landing activities (register PLS-029).

The Ministry of Family, Labour and Social Policy publishes, each month on the Public Employment Services portal
(psz.praca.gov.pl, "Bezrobocie rejestrowane"), a ZIP "statystyki strukturalne <month> <year>" with the month's
tables: ``Stopa bezrobocia rejestrowanego MM_YYYY.xlsx`` (registered unemployed in thousands and the registered
unemployment rate for Poland, the 16 voivodeships and every powiat, with TERYT codes),
``MRPiPS-01_Tablice_wynikowe_MM_YYYY.xlsx`` (the MRPiPS-01 report's result tables), ``Dane_MM_YYYY.xlsx`` and a
PDF report. Entity = one month (``stat_YYYY_MM``), version = the file's Last-Modified (written only when the SHA-256
changed, so a corrected ZIP lands as a new version).

landing.inspect  the months the portal lists vs the months in Landing (missing / held); no writes
landing.extract  missing months first, then the newest RECHECK months again (corrections), one download at a
                 time, PAUSE seconds apart; months held and older than that are not downloaded again
landing.verify   every receipt has its file (bulk_file pattern)

Discovery: the listing page links the newest ZIPs and, per year, a category page (``categoryId``, text = the year)
that links the year's asset page ("Statystyki strukturalne <year>", ``/-/<id>-statystyki-strukturalne-…``), which
links the year's twelve ZIPs. A year's pages are read only while one of its months is missing or the year is the
current or previous one; months before FIRST_YEAR are ignored (before 2019 MRPiPS used one asset page per month).
"""

from __future__ import annotations

from datetime import UTC, datetime
import re
import time
from typing import Any

SOURCE = "mrpips_unemployment"
LISTING = "https://psz.praca.gov.pl/rynek-pracy/statystyki-i-analizy/bezrobocie-rejestrowane"
FIRST_YEAR = 2019  # first year with one asset page per year (earlier years: one page per month, .xls only)
RECHECK = 2  # newest months downloaded again on every run (MRPiPS corrects the rate in later months)
PAUSE = 2.0
MONTHS = {
    "styczen": 1, "luty": 2, "marzec": 3, "kwiecien": 4, "maj": 5, "czerwiec": 6,
    "lipiec": 7, "sierpien": 8, "wrzesien": 9, "pazdziernik": 10, "listopad": 11, "grudzien": 12,
}  # fmt: skip
ZIP_TEXT = re.compile(r"^statystyki strukturalne\s+(\S+)\s+(\d{4})\.zip", re.IGNORECASE)
ASSET_TEXT = re.compile(r"^statystyki strukturalne\s+(\d{4})$", re.IGNORECASE)
ASSET_LINK = re.compile(r"psz\.praca\.gov\.pl/-/[0-9a-z-]*statystyki-strukturalne-[0-9a-z-]+$")
CATEGORY_LINK = re.compile(r"categoryId=\d+$")
PLAIN = str.maketrans("ąćęłńóśźż", "acelnoszz")


def _links(url: str) -> list[tuple[str, str]]:
    """(href, link text) of every <a> on a page; the text names the year of a category link."""
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
            return [(href, " ".join(text.split())) for href, text in parser.out]
        except bulk_file.PermanentError:
            raise
        except requests.RequestException:
            if attempt == bulk_file.RETRIES - 1:
                raise
            time.sleep(30 * (attempt + 1))
    raise RuntimeError("unreachable")


def entity(year: int, month: int) -> str:
    return f"stat_{year}_{month:02d}"


def discover(held: set[str], today=None) -> dict[str, str]:
    """{entity: ZIP URL} of every month the portal links from FIRST_YEAR on (reading as few pages as possible).

    Months are read from the link text ("statystyki strukturalne wrzesień 2026.zip"), which both link styles share
    (``/documents/d/urzad-pracy/statystyki-strukturalne-<month>-<year>`` since 2023,
    ``/documents/10828/<folder>/statystyki+strukturalne+<month>+<year>.zip/<uuid>`` before)."""
    today = today or datetime.now(UTC).date()
    found: dict[str, str] = {}
    pages_read = 0

    def collect(links: list[tuple[str, str]]) -> list[tuple[str, int]]:
        assets = []
        for href, text in links:
            zip_match = ZIP_TEXT.match(text)
            if zip_match:
                month = MONTHS.get(zip_match.group(1).lower().translate(PLAIN))
                year = int(zip_match.group(2))
                if month and year >= FIRST_YEAR:
                    found.setdefault(entity(year, month), href)
            asset_match = ASSET_TEXT.match(text)
            if asset_match and ASSET_LINK.search(href) and int(asset_match.group(1)) >= FIRST_YEAR:
                assets.append((href, int(asset_match.group(1))))
        return assets

    listing = _links(LISTING)
    pages_read += 1
    assets = collect(listing)
    for href, text in listing:
        if not CATEGORY_LINK.search(href) or not re.fullmatch(r"\d{4}", text):
            continue
        year = int(text)
        complete = all(entity(year, m) in held for m in range(1, 13))
        if year < FIRST_YEAR or (complete and year < today.year - 1):
            continue
        time.sleep(PAUSE)
        assets += collect(_links(href))
        pages_read += 1
    for href, year in dict.fromkeys(assets):
        if year < today.year - 1 and all(entity(year, m) in held for m in range(1, 13)):
            continue
        time.sleep(PAUSE)
        collect(_links(href))
        pages_read += 1
    print({"pages_read": pages_read, "months_found": len(found)}, flush=True)
    return dict(sorted(found.items()))


def held_entities(landing_root: str = "01_landing") -> set[str]:
    """Months with a landed copy."""
    from ingestion.common.r2 import req, s3

    client, bucket, out = s3(), req("STORAGE_LANDING_BUCKET_NAME"), set()
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=f"{landing_root}/{SOURCE}/snapshot/data/", Delimiter="/"
    ):
        out |= {p["Prefix"].rstrip("/").rsplit("/", 1)[-1] for p in page.get("CommonPrefixes", [])}
    return out


def plan(found: dict[str, str], held: set[str]) -> list[dict[str, Any]]:
    """Missing months first (oldest first), then the newest RECHECK months again."""
    spec = {"format": "zip", "version": "last_modified"}
    missing = [{"entity": e, "url": u, **spec} for e, u in found.items() if e not in held]
    recheck = [{"entity": e, "url": found[e], **spec} for e in sorted(found)[-RECHECK:] if e in held]
    return missing + recheck


def _inspect(p) -> int:
    from ingestion.common.report import annotate

    held = held_entities()
    found = discover(held)
    missing = sorted(set(found) - held)
    summary = {"months_listed": len(found), "months_held": len(held & set(found)), "missing": missing[:24]}
    summary["newest_listed"] = max(found) if found else None
    annotate("notice" if found else "error", f"{SOURCE} landing inspect", summary)
    return 0 if found else 1


def extract(budget_minutes: float, landing_root: str = "01_landing") -> int:
    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    deadline = time.monotonic() + budget_minutes * 60
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    held = held_entities(landing_root)
    todo = plan(discover(held), held)
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
        "planned": len(todo),
        "landed": sum(r["outcome"] != "unchanged" for r in results),
        "unchanged": sum(r["outcome"] == "unchanged" for r in results),
        "deferred_to_next_run": deferred,
        "refused": refused,
    }
    annotate("notice" if not refused else "warning", f"{SOURCE} landing extract", summary)
    return 1 if refused and not results else 0


def _extract(p) -> int:
    return extract(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "verify", specs=[{"entity": e} for e in sorted(held_entities())])


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract, "landing.verify": _verify}
