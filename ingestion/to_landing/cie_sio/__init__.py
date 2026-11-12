"""SIO archival education statistics on cie.gov.pl: Landing activities (register PLS-022).

Centrum Informatyczne Edukacji keeps a closed archive of SIO compilations (reference dates 30 September 2010 to
2018): the index page https://cie.gov.pl/archiwalne-dane-statystyczne/ links six topic pages (``/ads-uczniowie/``,
``/ads-absolwenci/``, ``/ads-nauczyciele/``, ...), whose plain HTML links 94 workbooks (.xls and .xlsx) under
``/content/uploads/2020/09/``. Only the workbooks of the modelled families are landed (``SELECTION``, a regex on
the file name): schools and pupils by voivodeship, governing body and school type, graduates by voivodeship and
school type, and the newest teachers workbook (persons and full-time posts, 2006-2018). Entity = one workbook (the
file name without extension, lower case, ``[a-z0-9_]``); version = the server's Last-Modified. A workbook already
landed is downloaded again only when its Last-Modified is newer than the landed copy (a HEAD request each), and
lands only when its SHA-256 changed. Restart unit: one file. One request at a time, PAUSE seconds apart.

landing.inspect  the selected workbooks the pages link vs Landing (missing / stale / current); no writes
landing.extract  every selected workbook missing or stale in Landing
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

import re
import time
from typing import Any

SOURCE = "cie_sio"
INDEX = "https://cie.gov.pl/archiwalne-dane-statystyczne/"
TOPIC = re.compile(r"/ads-[a-z-]+/?$")
WORKBOOK = re.compile(r"/content/uploads/.*\.xlsx?$", re.IGNORECASE)
# The modelled families (warehouse/models/silver/cie_sio/); the other workbooks are listed by inspect, not landed.
SELECTION = re.compile(
    r"^(Szkoly_i_uczniowie_wg_organow|Liczba_szkol_i_uczniow_wg_wojewodztw_|Absolwenci_|NauczEtatyOsoby2006-2018)",
    re.IGNORECASE,
)
LINKED = 94  # workbooks the six topic pages linked on 12 Nov 2026 (Dev probe 43289510189); the archive is closed
PAUSE = 2.0


def entity_of(url: str) -> str:
    stem = url.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    return re.sub(r"[^a-z0-9]+", "_", stem.lower()).strip("_")


def discover() -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """{entity: bulk_file spec} of the selected workbooks, and a summary of the pages read."""
    from ingestion.common.gus_publications import _links

    topics = list(dict.fromkeys(h.rstrip("/") + "/" for h, _ in _links(INDEX) if TOPIC.search(h)))
    files: list[str] = []
    per_page: dict[str, int] = {}
    for page in topics:
        time.sleep(PAUSE)
        found = [h for h, _ in _links(page) if WORKBOOK.search(h)]
        per_page[page.rstrip("/").rsplit("/", 1)[-1]] = len(set(found))
        files += [h for h in found if h not in files]
    selected: dict[str, dict[str, Any]] = {}
    for url in files:
        if not SELECTION.search(url.rsplit("/", 1)[-1]):
            continue
        entity = entity_of(url)
        if entity in selected and selected[entity]["url"] != url:
            raise ValueError(f"{SOURCE}: two workbooks map to entity {entity}: {selected[entity]['url']}, {url}")
        selected[entity] = {"entity": entity, "url": url, "format": "raw", "version": "last_modified"}
    summary = {"topic_pages": per_page, "workbooks_linked": len(files), "selected": len(selected)}
    return selected, summary


def held(landing_root: str = "01_landing") -> dict[str, str]:
    """{entity: version token of the newest landed copy}."""
    from ingestion.common.r2 import req, s3
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    out: dict[str, str] = {}
    for receipt in bulk_file.landed(client, bucket, f"{landing_root}/{SOURCE}/snapshot/data/"):
        out[receipt["entity"]] = receipt["version_token"]  # receipts come oldest first: the newest wins
    return out


def state(spec: dict[str, Any], landed: dict[str, str]) -> tuple[str, str | None]:
    """('missing' | 'stale' | 'current' | 'unknown' | 'unreachable', server version) of one selected workbook."""
    from ingestion.to_landing.patterns import bulk_file

    if spec["entity"] not in landed:
        return "missing", None
    response, http = bulk_file.probe(spec["url"])
    if http != "200" or response is None:
        return "unreachable", None
    version = bulk_file.server_version(dict(response.headers))
    if version is None:
        return "unknown", None  # no validator: extract downloads it and compares the SHA-256
    return ("stale" if version > landed[spec["entity"]] else "current"), version


def level(summary: dict[str, Any], states: dict[str, list[str]]) -> str:
    """'error' when nothing is selected or a landed workbook is unreachable; 'warning' when the archive's link count
    changed (a new or removed file must be looked at); else 'notice'."""
    if not summary["selected"] or states.get("unreachable"):
        return "error"
    return "warning" if summary["workbooks_linked"] != LINKED else "notice"


def _inspect(p) -> int:
    from ingestion.common.report import annotate

    found, summary = discover()
    landed, states = held(), {}
    for entity, spec in sorted(found.items()):
        time.sleep(PAUSE if entity in landed else 0)
        states.setdefault(state(spec, landed)[0], []).append(entity)
    result = level(summary, states)
    annotate(result, f"{SOURCE} landing inspect", {**summary, **{k: v[:40] for k, v in states.items()}})
    return 1 if result == "error" else 0


def extract(budget_minutes: float, landing_root: str = "01_landing") -> int:
    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    deadline = time.monotonic() + budget_minutes * 60
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    found, summary = discover()
    landed = held(landing_root)
    states: dict[str, list[str]] = {}
    results, refused, deferred = [], [], 0
    todo = sorted(found.items())
    for n, (entity, spec) in enumerate(todo):
        if time.monotonic() > deadline:
            deferred = len(todo) - n
            break
        time.sleep(PAUSE)
        current, _ = state(spec, landed)
        states.setdefault(current, []).append(entity)
        if current in ("current", "unreachable"):
            continue
        time.sleep(PAUSE if entity in landed else 0)
        try:
            results.append(bulk_file.load_file(client, bucket, SOURCE, spec, landing_root))
        except bulk_file.PermanentError as exc:
            refused.append({"entity": entity, "error": str(exc)[:120]})
    summary.update(
        {
            "current": len(states.get("current", [])),
            "landed": [r["entity"] for r in results if r["outcome"] != "unchanged"],
            "unchanged": [r["entity"] for r in results if r["outcome"] == "unchanged"],
            "unreachable": states.get("unreachable", []),
            "deferred_to_next_run": deferred,
            "refused": refused,
        }
    )
    result = level(summary, {"unreachable": states.get("unreachable", []) + [r["entity"] for r in refused]})
    annotate(result, f"{SOURCE} landing extract", summary)
    return 1 if result == "error" else 0


def _extract(p) -> int:
    return extract(p.budget_minutes)


def _verify(p) -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "verify", specs=[{"entity": e} for e in sorted(held())])


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract, "landing.verify": _verify}
