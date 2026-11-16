"""Sejm API Landing activities (register PLG-024): terms 9 and 10 of https://api.sejm.gov.pl/sejm.

Entities (one JSON answer each, landed by the shared bulk_file pattern: exact bytes, parsed before landing, receipt
after the file, unchanged bytes skipped; the API sends no Last-Modified, so version = download time):
  terms                       /sejm/term: every term with its prints count (read every run)
  term<t>_proceedings         /sejm/term<t>/proceedings: sittings with their dates
  term<t>_voting_days         /sejm/term<t>/votings: days with votes (sitting, date, number of votings)
  term<t>_prints              /sejm/term<t>/prints: every print of the term (no paging: one answer)
  term<t>_votings_<sitting>   /sejm/term<t>/votings/<sitting>: every voting of one sitting with its totals
  term10_mps                  /sejm/term10/MP, reduced before Landing to the allowlist of MP_FIELDS (no names,
                              birth data, e-mails, professions or photos ever land)
Term 9 has ended: its entities are read once. In term 10 the lists are read every run; the votings of a sitting
are read once, except the newest OPEN_SITTINGS sittings, which are read again (votes of a running sitting).

landing.inspect  robots.txt, Landing per entity, one request to /sejm/term (no validator: load compares SHA-256)
landing.extract  robots.txt first; one request at a time, PAUSE_SECONDS apart, bounded retries (bulk_file)
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

import json
import time
import urllib.robotparser

SOURCE = "sejm"
HOST = "https://api.sejm.gov.pl"
TERMS = (9, 10)
CURRENT_TERM = 10
OPEN_SITTINGS = 2
PAUSE_SECONDS = 2.0
MP_FIELDS = ("id", "active", "club", "districtNum", "districtName", "voivodeship")
REDUCE_MPS = "ingestion.to_landing.sejm:reduce_mps"


def reduce_mps(body: bytes) -> bytes:
    """The MP list with only the allowlisted fields of each MP (ValueError on another layout: nothing lands)."""
    items = json.loads(body)
    if not isinstance(items, list) or not all(isinstance(i, dict) and "id" in i for i in items):
        raise ValueError("MP list: not a list of objects with an id")
    kept = [{k: i[k] for k in MP_FIELDS if k in i} for i in items]
    return json.dumps(kept, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def _spec(entity: str, path: str, closed: bool = False, **extra) -> dict:
    spec = {
        "entity": entity,
        "url": f"{HOST}{path}",
        "format": "json",  # parsed before landing: a cut-off answer is retried, never landed
        "version": "ingested",  # the API sends no Last-Modified
        "read_timeout": 120,
        "retries": 4,
        "wait": 30,
        **extra,
    }
    if closed:
        spec["closed"] = True  # read once: skipped while Landing holds a copy
    return spec


def base_specs() -> list[dict]:
    specs = [_spec("terms", "/sejm/term")]
    for term in TERMS:
        closed = term != CURRENT_TERM
        for name, path in (("proceedings", "proceedings"), ("voting_days", "votings"), ("prints", "prints")):
            specs.append(_spec(f"term{term}_{name}", f"/sejm/term{term}/{path}", closed))
    specs.append(_spec(f"term{CURRENT_TERM}_mps", f"/sejm/term{CURRENT_TERM}/MP", reduce=REDUCE_MPS))
    return specs


def sittings(landing_root: str = "01_landing") -> dict[int, list[int]]:
    """Sitting numbers with votes per term, from the newest landed voting_days answer of each term."""
    from ingestion.common.bulk_bronze import read_raw
    from ingestion.common.r2 import req, s3
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    found: dict[int, list[int]] = {}
    for term in TERMS:
        held = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, f"term{term}_voting_days", landing_root))
        if held:
            days = json.loads(read_raw(client, bucket, SOURCE, held[-1]))
            found[term] = sorted({int(d["proceeding"]) for d in days})
    return found


def sitting_specs(found: dict[int, list[int]]) -> list[dict]:
    specs = []
    for term, numbers in found.items():
        newest = set(numbers[-OPEN_SITTINGS:]) if term == CURRENT_TERM else set()
        for n in numbers:
            specs.append(_spec(f"term{term}_votings_{n}", f"/sejm/term{term}/votings/{n}", n not in newest))
    return specs


def specs(landing_root: str = "01_landing") -> list[dict]:
    """Every entity: the lists, and one votings answer per sitting listed in the landed voting_days."""
    return base_specs() + sitting_specs(sittings(landing_root))


def robots_allow() -> tuple[bool, str]:
    """(allowed, detail): /robots.txt read first; a disallow of /sejm/ stops the run (never worked around)."""
    import requests

    from ingestion.to_landing.patterns.bulk_file import USER_AGENT

    parser = urllib.robotparser.RobotFileParser()
    try:
        answer = requests.get(f"{HOST}/robots.txt", headers={"User-Agent": USER_AGENT}, timeout=60)
    except requests.RequestException as exc:
        return False, f"robots.txt unreachable: {type(exc).__name__}"
    if answer.status_code in (401, 403, 429):
        return False, f"robots.txt {answer.status_code}"
    parser.parse(answer.text.splitlines() if answer.status_code == 200 else [])
    ok = all(parser.can_fetch(USER_AGENT, f"{HOST}{p}") for p in ("/sejm/term", f"/sejm/term{CURRENT_TERM}/votings/1"))
    return ok, f"robots.txt {answer.status_code}: {'allows' if ok else 'disallows'} /sejm/"


def run(mode: str, landing_root: str = "01_landing") -> int:
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    if mode != "verify":
        ok, detail = robots_allow()
        if not ok:
            annotate("error", f"{SOURCE} landing {mode}: stopped", {"robots": detail})
            return 1
    if mode == "inspect":
        return _inspect(landing_root)
    if mode == "verify":
        return bulk_file.main(SOURCE, "verify", landing_root, specs=specs(landing_root))
    status = bulk_file.main(SOURCE, "load", landing_root, specs=base_specs(), pause_seconds=PAUSE_SECONDS)
    time.sleep(PAUSE_SECONDS)
    more = sitting_specs(sittings(landing_root))
    return max(status, bulk_file.main(SOURCE, "load", landing_root, specs=more, pause_seconds=PAUSE_SECONDS))


def _inspect(landing_root: str) -> int:
    import requests

    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    files = []
    for spec in specs(landing_root):
        held = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, spec["entity"], landing_root))
        state = "missing" if not held else ("held" if spec.get("closed") else "unknown")
        files.append({"entity": spec["entity"], "landed_versions": len(held), "state": state})
    try:
        answer = requests.get(f"{HOST}/sejm/term", headers={"User-Agent": bulk_file.USER_AGENT}, timeout=60)
        endpoint = answer.status_code
    except requests.RequestException as exc:
        endpoint = type(exc).__name__
    missing = [f["entity"] for f in files if f["state"] == "missing"]
    summary = {"source": SOURCE, "mode": "inspect", "endpoint": endpoint, "entities": len(files), "missing": missing}
    print(json.dumps(summary, sort_keys=True))
    annotate("notice" if endpoint == 200 else "error", f"{SOURCE} landing inspect", summary)
    return 0 if endpoint == 200 else 1


ACTIVITIES = {
    "landing.inspect": lambda p: run("inspect"),
    "landing.extract": lambda p: run("load"),
    "landing.verify": lambda p: run("verify"),
}
