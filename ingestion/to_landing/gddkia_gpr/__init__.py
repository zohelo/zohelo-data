"""GDDKiA Generalny Pomiar Ruchu (GPR, road traffic census) Landing activities (register PL-ENV-022).

GDDKiA publishes one gov.pl page per census edition (every five years) whose attachments are the result tables as
spreadsheets with PDF twins, maps, a SHP file and syntheses: https://www.gov.pl/web/gddkia/generalny-pomiar-ruchu-2025
and https://www.gov.pl/web/gddkia/generalny-pomiar-ruchu-20202021 (older editions are on archiwum.gddkia.gov.pl, not
reachable from runners). Only table 2, the average annual daily traffic (SDRR) per measured section and vehicle class,
is landed: national roads (dk) and voivodeship roads (dw) of both editions, plus the 2020/21 national-roads erratum
(21 sections with corrected values). Attachment URLs are https://www.gov.pl/attachment/<uuid> and change when a file
is replaced; the 2020/21 page labels its spreadsheet links only "XLS", so a file is identified by the file name the
server sends (Content-Disposition), read with one header-only request per spreadsheet link. Every FILES pattern must
match exactly one link of its edition page (else the run fails: a renamed or second file is never guessed).
Entity = one file (``gpr<edition>_<road class>_sdrr[_errata]``); version = Last-Modified; a copy lands only when its
SHA-256 changed (bulk_file.load_file). Restart unit: one file. One request at a time, PAUSE seconds apart.

landing.inspect  the files the edition pages link vs Landing; no writes
landing.extract  every declared file (unchanged bytes are not written again)
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

import re
import time
from typing import Any
from urllib.parse import unquote

SOURCE = "gddkia_gpr"
PAUSE = 2.0
EDITIONS = {
    "2025": "https://www.gov.pl/web/gddkia/generalny-pomiar-ruchu-2025",
    "2020": "https://www.gov.pl/web/gddkia/generalny-pomiar-ruchu-20202021",
}
# entity → (edition, file name pattern). Names seen on 13 Nov 2026: 25_dk_KRAJ_20260926__SDRR.xlsx,
# Tab02_25_dw_KRAJ_20260622.xlsx, Tab02_20_dk_KRAJ.xls, Tab02_20_dk_KRAJ_(errata-21_odcinków).xls, Tab02_20_dw_KRAJ.xls
FILES = {
    "gpr2025_dk_sdrr": ("2025", r"^25_dk_KRAJ_\d{8}_+SDRR\.xlsx?$"),
    "gpr2025_dw_sdrr": ("2025", r"^Tab02_25_dw_KRAJ(_\d{8})?\.xlsx?$"),
    "gpr2020_dk_sdrr": ("2020", r"^Tab02_20_dk_KRAJ\.xlsx?$"),
    "gpr2020_dk_sdrr_errata": ("2020", r"^Tab02_20_dk_KRAJ_\(errata[^)]*\)\.xlsx?$"),
    "gpr2020_dw_sdrr": ("2020", r"^Tab02_20_dw_KRAJ(_v\d+)?\.xlsx?$"),
}
SPREADSHEET_TEXT = re.compile(r"\bXLSX?\b|\.xlsx?\b", re.IGNORECASE)


def _links(url: str) -> list[tuple[str, str]]:
    from ingestion.common.gus_publications import _links as links

    return [(href, text.replace("​", "")) for href, text in links(url)]


def file_name(headers: dict[str, str]) -> str | None:
    """The file name of a Content-Disposition header (RFC 5987 filename* first)."""
    value = headers.get("Content-Disposition") or ""
    match = re.search(r"filename\*\s*=\s*[^']*''([^;]+)", value)
    if match:
        return unquote(match.group(1).strip().strip('"'))
    match = re.search(r'filename\s*=\s*"?([^";]+)"?', value)
    return match.group(1).strip() if match else None


def named_links(page: str) -> dict[str, str]:
    """{attachment URL: file name} of every spreadsheet link on an edition page (header-only requests)."""
    from ingestion.to_landing.patterns import bulk_file

    urls = []
    for href, text in _links(page):
        if "/attachment/" in href and SPREADSHEET_TEXT.search(text) and href not in urls:
            urls.append(href)
    out = {}
    for url in urls:
        time.sleep(PAUSE)
        response, http = bulk_file.probe(url, "get")
        if response is None or http != "200":
            raise RuntimeError(f"{SOURCE}: attachment {url} answered {http}")
        out[url] = file_name(dict(response.headers)) or ""
    return out


def match(files: dict[str, str], edition: str) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """The declared files of one edition found among {url: file name}; problems as text (none or several matches)."""
    found, problems = {}, []
    for entity, (ed, pattern) in FILES.items():
        if ed != edition:
            continue
        hits = sorted({url for url, name in files.items() if re.match(pattern, name)})
        if len(hits) != 1:
            problems.append(f"{entity}: {len(hits)} links match {pattern!r}")
            continue
        found[entity] = {
            "entity": entity,
            "url": hits[0],
            "file_name": files[hits[0]],
            "format": "raw",
            "version": "last_modified",
        }
    return found, problems


def discover() -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    found, problems, names = {}, [], {}
    for edition, page in EDITIONS.items():
        time.sleep(PAUSE)
        files = named_links(page)
        names[edition] = sorted(files.values())
        hits, issues = match(files, edition)
        found.update(hits)
        problems += issues
    return found, {"spreadsheets_listed": names, "problems": problems}


def held(landing_root: str = "01_landing") -> dict[str, set[str]]:
    """{entity: attachment URLs landed}."""
    from ingestion.common.r2 import req, s3
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    out: dict[str, set[str]] = {}
    for receipt in bulk_file.landed(client, bucket, f"{landing_root}/{SOURCE}/snapshot/data/"):
        out.setdefault(receipt["entity"], set()).add(receipt["source_url"])
    return out


def _inspect(p) -> int:
    from ingestion.common.report import annotate

    found, summary = discover()
    landed = held()
    summary["new_url"] = [e for e, s in sorted(found.items()) if s["url"] not in landed.get(e, set())]
    level = "error" if summary["problems"] else "notice"
    annotate(level, f"{SOURCE} landing inspect", {**summary, "found": sorted(found)})
    return 1 if level == "error" else 0


def extract(budget_minutes: float, landing_root: str = "01_landing") -> int:
    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    deadline = time.monotonic() + budget_minutes * 60
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    found, summary = discover()
    results, refused, deferred = [], [], 0
    todo = [found[e] for e in sorted(found)]
    for n, spec in enumerate(todo):
        if time.monotonic() > deadline:
            deferred = len(todo) - n
            break
        time.sleep(PAUSE)
        try:
            results.append(bulk_file.load_file(client, bucket, SOURCE, spec, landing_root))
        except bulk_file.PermanentError as exc:
            refused.append({"entity": spec["entity"], "error": str(exc)[:120]})
    summary.update(
        {
            "landed": [r["entity"] for r in results if r["outcome"] != "unchanged"],
            "unchanged": [r["entity"] for r in results if r["outcome"] == "unchanged"],
            "deferred_to_next_run": deferred,
            "refused": refused,
        }
    )
    level = "error" if summary["problems"] else ("warning" if refused else "notice")
    annotate(level, f"{SOURCE} landing extract", summary)
    return 1 if summary["problems"] or (refused and not results) else 0


def _extract(p) -> int:
    return extract(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "verify", specs=[{"entity": e} for e in sorted(held())])


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract, "landing.verify": _verify}
