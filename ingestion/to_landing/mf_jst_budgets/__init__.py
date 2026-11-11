"""Ministry of Finance local-government budget reports Landing activities (register PLG-010).

The Ministry of Finance publishes the budget reports of local-government units (sprawozdania budżetowe JST) on
gov.pl: per year a page ("2025 r.", linked from https://www.gov.pl/web/finanse/sprawozdania-budzetowe) with a
sub-page "Zestawienia zbiorcze" of summary workbooks, one XLSX per quarter, unit type and report family:
``<Type>_<DW|ZN>_<year>_k<quarter>_<YYYYMMDD>.xlsx`` (Type: Gminy, Miasta (cities with powiat rights), Powiaty,
Wojew (voivodeships), JST (all units), Związki (unions); DW: revenue and expenditure, ZN: liabilities and
receivables; the date is the file's preparation date). Entity = type × family × year × quarter
(``gminy_dw_2025_k4``); a corrected file gets a new attachment URL and preparation date and lands as a new version
(version = download time, gov.pl sends no Last-Modified); the newest wins. Already landed attachment URLs are not
downloaded again. The per-unit report databases (Rb27S, Rb28S, … ZIPs of 1-27 MB a quarter) are a separate scope.

landing.inspect  the files the year pages list vs Landing (missing / held); no writes
landing.extract  every listed attachment not yet in Landing, one download at a time,
                 PAUSE seconds apart (pages and files of www.gov.pl)
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

import re
import time
from typing import Any

SOURCE = "mf_jst_budgets"
LISTING = "https://www.gov.pl/web/finanse/sprawozdania-budzetowe"
LISTING_PAGES = 3  # the listing is paginated (?page=N&size=10): year pages 2018 onwards and the archive
FIRST_YEAR = 2018
PAUSE = 2.0
YEAR_TEXT = re.compile(r"^(20\d{2}) r\.?$")
SUMMARY_TEXT = re.compile(r"^Zestawienia zbiorcze$", re.IGNORECASE)
FILE = re.compile(r"(Gminy|Miasta|Powiaty|Wojew|JST|Zwi[aą]zki)_(DW|ZN)_(\d{4})_k([1-4])_(\d{8})\.xlsx", re.IGNORECASE)
PLAIN = str.maketrans("ąćęłńóśźżĄĆĘŁŃÓŚŹŻ", "acelnoszzACELNOSZZ")


def _links(url: str) -> list[tuple[str, str]]:
    """(absolute href, link text without zero-width spaces) of every <a> on a page, with bounded retries."""
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
            return [(href, " ".join(text.replace("\u200b", "").split())) for href, text in parser.out]
        except bulk_file.PermanentError:
            raise
        except requests.RequestException:
            if attempt == bulk_file.RETRIES - 1:
                raise
            time.sleep(30 * (attempt + 1))
    raise RuntimeError("unreachable")


def files(links: list[tuple[str, str]]) -> dict[str, dict[str, Any]]:
    """{entity: spec} of the summary workbooks among a page's links (the newest preparation date per entity)."""
    out: dict[str, dict[str, Any]] = {}
    for href, text in links:
        match = FILE.search(text)
        if not match or "/attachment/" not in href:
            continue
        kind, family, year, quarter, prepared = match.groups()
        entity = f"{kind.translate(PLAIN).lower()}_{family.lower()}_{year}_k{quarter}"
        if int(year) < FIRST_YEAR or (entity in out and out[entity]["prepared"] >= prepared):
            continue
        out[entity] = {"entity": entity, "url": href, "prepared": prepared, "format": "raw", "version": "ingested"}
    return out


def discover() -> dict[str, dict[str, Any]]:
    """Every summary workbook the year pages list, from FIRST_YEAR on."""
    found: dict[str, dict[str, Any]] = {}
    years: dict[int, str] = {}
    for page in range(1, LISTING_PAGES + 1):
        url = LISTING if page == 1 else f"{LISTING}?page={page}&size=10"
        for href, text in _links(url):
            match = YEAR_TEXT.match(text.strip())
            if match and int(match.group(1)) >= FIRST_YEAR:
                years.setdefault(int(match.group(1)), href)
        time.sleep(PAUSE)
    pages_read = LISTING_PAGES
    for _year, href in sorted(years.items()):
        summary = [h for h, t in _links(href) if SUMMARY_TEXT.match(t.strip())]
        pages_read += 1
        for target in dict.fromkeys(summary):
            time.sleep(PAUSE)
            found.update(files(_links(target)))
            pages_read += 1
    print({"pages_read": pages_read, "years": sorted(years), "files_found": len(found)}, flush=True)
    return dict(sorted(found.items()))


def held(landing_root: str = "01_landing") -> dict[str, set[str]]:
    """{entity: attachment URLs landed}: gov.pl gives a corrected file a new attachment URL (and file name)."""
    from ingestion.common.r2 import req, s3
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    out: dict[str, set[str]] = {}
    for receipt in bulk_file.landed(client, bucket, f"{landing_root}/{SOURCE}/snapshot/data/"):
        out.setdefault(receipt["entity"], set()).add(receipt["source_url"])
    return out


def plan(found: dict[str, dict[str, Any]], landed: dict[str, set[str]]) -> list[dict[str, Any]]:
    """Entities whose listed attachment has not landed yet (new quarters and corrected files), oldest first."""
    return [spec for entity, spec in found.items() if spec["url"] not in landed.get(entity, set())]


def _inspect(p) -> int:
    from ingestion.common.report import annotate

    found = discover()
    todo = plan(found, held())
    summary = {"files_listed": len(found), "to_land": len(todo), "sample": [s["entity"] for s in todo[:20]]}
    annotate("notice" if found else "error", f"{SOURCE} landing inspect", summary)
    return 0 if found else 1


def extract(budget_minutes: float, landing_root: str = "01_landing") -> int:
    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    deadline = time.monotonic() + budget_minutes * 60
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    todo = plan(discover(), held(landing_root))
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

    return bulk_file.main(SOURCE, "verify", specs=[{"entity": e} for e in sorted(held())])


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract, "landing.verify": _verify}
