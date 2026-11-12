"""ZUS statistical portal Landing activities (register PLS-009, psz.zus.pl).

The portal's category pages link one XLSX per topic table and period under
``/documents/493365/<folder>/<file name>.xlsx/<uuid>``; the folder id is stable for a topic table across years (a
"family"), the file name carries the period. A curated list of families is landed (FAMILIES): the December old-age
pension tables (number of pensioners by age and sex, average pension by sex), insured persons (pension insurance by age
and sex; sickness insurance by voivodeship; health insurance monthly; foreign citizens monthly), active contribution
payers by voivodeship and sickness absence by voivodeship. Entity = family × period (``<family>_<year>`` or
``<family>_<year>_q<n>``), version = the file's Last-Modified (written only when the SHA-256 changed, so a corrected
file lands as a new version). Files in the old Excel format (.xls, before 2014-2018 depending on the table) and the
English copies (file names starting ``EN``) are not landed.

landing.inspect  the periods the portal lists vs Landing (missing / held), per family; no writes
landing.extract  missing periods first, then the newest RECHECK periods of each family again (ZUS corrects files and
                 rewrites the current year's monthly files), one download at a time, PAUSE seconds apart
landing.verify   every receipt has its file (bulk_file pattern)

Quarterly families keep only the current year on the portal: earlier quarters stay in Landing/Archive only.
"""

from __future__ import annotations

import re
import time
from typing import Any
from urllib.parse import unquote_plus

SOURCE = "zus_psz"
BASE = "https://psz.zus.pl/kategorie/"
RECHECK = 2
PAUSE = 2.0
QUARTER = r"\b(?P<quarter>I|II|III|IV) kw\.? ?(?P<year>20\d\d)\b"
# family: (category page, folder id, file-name pattern with year and optionally quarter or month, header anchor)
# The anchor is the text that starts column A of the table's header row; Bronze rejects a workbook without it.
FAMILIES: dict[str, dict[str, str]] = {
    "pensioners_dec": {
        "page": "emerytury/stan-na-grudzien",
        "folder": "9888915",
        "name": r"wg wieku i płci - XII (?P<year>20\d\d)\b",
        "anchor": "Wiek w latach",
    },
    "pension_amount_dec": {
        "page": "emerytury/stan-na-grudzien",
        "folder": "9888921",
        "name": r"wysokości świadczeń i płci - XII (?P<year>20\d\d)\b",
        "anchor": "Przeciętna wysokość na osobę",
    },
    "insured_pension": {
        "page": "ubezpieczeni/ubezpieczenia-emerytalne-i-rentowe",
        "folder": "532853",
        "name": r"wg wieku i płci " + QUARTER,
        "anchor": "Wiek w latach",
    },
    "foreign_insured": {
        "page": "ubezpieczeni/ubezpieczenia-emerytalne-i-rentowe",
        "folder": "3456951",
        "name": r"\d\d\.(?P<year>20\d\d)_liczba zgł\.cudz",
        "anchor": "Według stanu na",
    },
    "insured_sickness_voiv": {
        "page": "ubezpieczeni/ubezpieczenia-chorobowe",
        "folder": "532793",
        "name": r"chorobowe wg woj\. " + QUARTER,
        "anchor": "Wyszczególnienie",
    },
    "insured_health": {
        "page": "ubezpieczeni/ubezpieczenie-zdrowotne",
        "folder": "1159389",
        "name": r"zdrowotne (?P<year>20\d\d)\.xlsx$",
        "anchor": "Okres",
    },
    "payers_voiv": {
        "page": "platnicy-skladek/ubezpieczenia-emerytalne-i-rentowe",
        "folder": "533093",
        "name": r"wg woj\. oraz liczby ubezp-stan na \d\d\.(?P<month>\d\d)\.(?P<year>20\d\d)\b",
        "anchor": "Województwo",
    },
    "absence_voiv": {
        "page": "absencja-chorobowa/absencja-chorobowa-z-tytulu-choroby-wlasnej-osob-ubezpieczonych-w-zus",
        "folder": "6744574",
        "name": r"Abs chor wg woj " + QUARTER,
        "anchor": "Województwo",
    },
}
LINK = re.compile(r"/documents/493365/(?P<folder>\d+)/(?P<name>[^/?#]+\.xlsx)(?:/|$)", re.IGNORECASE)
ROMAN = {"I": 1, "II": 2, "III": 3, "IV": 4}


def family_of(entity: str) -> str:
    """The family of an entity name (``absence_voiv_2026_q3`` → ``absence_voiv``)."""
    return re.sub(r"_20\d\d(_q[1-4])?$", "", entity)


def entity_of(family: str, file_name: str) -> str | None:
    """The entity a file name stands for, or None when the name does not match the family's pattern."""
    m = re.search(FAMILIES[family]["name"], " ".join(file_name.split()))
    if not m:
        return None
    parts = m.groupdict()
    if parts.get("quarter"):
        return f"{family}_{parts['year']}_q{ROMAN[parts['quarter']]}"
    if parts.get("month"):
        month = int(parts["month"])
        if month not in (3, 6, 9, 12):
            return None
        return f"{family}_{parts['year']}_q{month // 3}"
    return f"{family}_{parts['year']}"


def _page_links(url: str) -> list[str]:
    """Absolute URLs of every <a href> on a category page, in page order (retries like bulk_file)."""
    import requests

    from ingestion.to_landing.patterns import bulk_file

    for attempt in range(bulk_file.RETRIES):
        try:
            response = requests.get(url, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=60)
            if 400 <= response.status_code < 500 and response.status_code not in bulk_file.RETRY_STATUSES:
                raise bulk_file.PermanentError(f"page {response.status_code}: {url}")
            response.raise_for_status()
            return bulk_file.links_matching(response.text, response.url, r"\.xlsx")
        except bulk_file.PermanentError:
            raise
        except requests.RequestException:
            if attempt == bulk_file.RETRIES - 1:
                raise
            time.sleep(30 * (attempt + 1))
    raise RuntimeError("unreachable")


def discover() -> tuple[dict[str, str], dict[str, int]]:
    """({entity: XLSX URL} of every period the portal links for the declared families, {family: unmatched files}).

    One request per category page; within a family the first link of a period (page order: newest first) wins."""
    found: dict[str, str] = {}
    unmatched = {family: 0 for family in FAMILIES}
    pages = sorted({spec["page"] for spec in FAMILIES.values()})
    for n, page in enumerate(pages):
        if n:
            time.sleep(PAUSE)
        for url in _page_links(BASE + page):
            m = LINK.search(url)
            if not m:
                continue
            name = unquote_plus(m.group("name"))
            for family, spec in FAMILIES.items():
                if spec["page"] != page or spec["folder"] != m.group("folder") or name.startswith("EN"):
                    continue
                entity = entity_of(family, name)
                if entity is None:
                    unmatched[family] += 1
                else:
                    found.setdefault(entity, url)
    print({"pages_read": len(pages), "periods_found": len(found)}, flush=True)
    return dict(sorted(found.items())), unmatched


def held_entities(landing_root: str = "01_landing") -> set[str]:
    """Entities with a landed copy."""
    from ingestion.common.r2 import req, s3

    client, bucket, out = s3(), req("STORAGE_LANDING_BUCKET_NAME"), set()
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=f"{landing_root}/{SOURCE}/snapshot/data/", Delimiter="/"
    ):
        out |= {p["Prefix"].rstrip("/").rsplit("/", 1)[-1] for p in page.get("CommonPrefixes", [])}
    return out


def plan(found: dict[str, str], held: set[str]) -> list[dict[str, Any]]:
    """Missing periods first (oldest first), then the newest RECHECK held periods of every family."""
    spec = {"format": "raw", "version": "last_modified"}
    missing = [{"entity": e, "url": u, **spec} for e, u in found.items() if e not in held]
    recheck = []
    for family in FAMILIES:
        newest = sorted(e for e in found if family_of(e) == family)[-RECHECK:]
        recheck += [{"entity": e, "url": found[e], **spec} for e in newest if e in held]
    return missing + recheck


def _inspect(p) -> int:
    from ingestion.common.report import annotate

    held = held_entities()
    found, unmatched = discover()
    per_family = {
        f: {
            "listed": sum(family_of(e) == f for e in found),
            "held": sum(family_of(e) == f for e in held & set(found)),
            "newest": max((e for e in found if family_of(e) == f), default=None),
            "unmatched_files": unmatched[f],
        }
        for f in FAMILIES
    }
    empty = [f for f, row in per_family.items() if not row["listed"]]
    summary = {"periods_listed": len(found), "missing": sorted(set(found) - held)[:30], "families": per_family}
    annotate("notice" if not empty else "error", f"{SOURCE} landing inspect", {**summary, "families_empty": empty})
    return 1 if empty else 0


def extract(budget_minutes: float, landing_root: str = "01_landing") -> int:
    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    deadline = time.monotonic() + budget_minutes * 60
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    held = held_entities(landing_root)
    found, unmatched = discover()
    todo = plan(found, held)
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
        "periods_listed": len(found),
        "planned": len(todo),
        "landed": sum(r["outcome"] != "unchanged" for r in results),
        "unchanged": sum(r["outcome"] == "unchanged" for r in results),
        "deferred_to_next_run": deferred,
        "refused": refused,
        "unmatched_files": {f: c for f, c in unmatched.items() if c},
    }
    annotate("notice" if not refused else "warning", f"{SOURCE} landing extract", summary)
    return 1 if (refused and not results) or not found else 0


def _extract(p) -> int:
    return extract(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "verify", specs=[{"entity": e} for e in sorted(held_entities())])


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract, "landing.verify": _verify}
