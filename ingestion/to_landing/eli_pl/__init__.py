"""ELI Poland Landing activities (register PLG-026): acts of Dziennik Ustaw (DU) and Monitor Polski (MP) by year from
the Sejm's ELI API, https://api.sejm.gov.pl/eli.

Entities (one JSON answer each, landed by the shared bulk_file pattern: exact bytes, parsed before landing, receipt
after the file, unchanged bytes skipped; the API sends no Last-Modified, so version = download time):
  publishers          /eli/acts: both journals with their number of acts and the years they hold (every run)
  acts_<DU|MP>_<y>    /eli/acts/<DU|MP>/<y>: every act of the journal year in one answer (no paging), with
                      address, ELI id, position, title, type, status, promulgation and announcement dates
Which years: those the newest landed publishers answer lists for the journal (DU 1918-1939 and 1944 on, MP 1930-1939
and 1945 on). The current and the previous year are read every run (new positions); an older year is read when it
is missing and again every fourth week (years with year % 4 = ISO week % 4), so the status of old acts (repealed,
consolidated) follows within four weeks without reading every year each run. Act texts (PDF, HTML) and the per-act
details (entry into force, references) are not read.

landing.inspect  robots.txt, Landing per entity, one request to /eli/acts (no validator: load compares SHA-256)
landing.extract  robots.txt first; one request at a time, PAUSE_SECONDS apart, bounded retries (bulk_file)
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

from datetime import UTC, datetime
import json
import time
import urllib.robotparser

SOURCE = "eli_pl"
HOST = "https://api.sejm.gov.pl"
PUBLISHERS = ("DU", "MP")
FIRST_YEAR = 1918
OPEN_YEARS = 2
ROTATION = 4
PAUSE_SECONDS = 2.0


def _spec(entity: str, path: str, closed: bool = False, **extra) -> dict:
    spec = {
        "entity": entity,
        "url": f"{HOST}{path}",
        "format": "json",  # parsed before landing: a cut-off answer is retried, never landed
        "version": "ingested",  # the API sends no Last-Modified
        "read_timeout": 180,
        "retries": 4,
        "wait": 30,
        **extra,
    }
    if closed:
        spec["closed"] = True  # skipped while Landing holds a copy
    return spec


def current_year(today: datetime | None = None) -> int:
    from zoneinfo import ZoneInfo

    return (today or datetime.now(UTC)).astimezone(ZoneInfo("Europe/Warsaw")).year


def year_specs(years: dict[str, list[int]], today: datetime | None = None) -> list[dict]:
    """One spec per journal year; closed unless open (newest OPEN_YEARS) or in this week's rotation."""
    now = today or datetime.now(UTC)
    current, week = current_year(now), now.isocalendar().week
    specs = []
    for publisher, listed in years.items():
        for year in sorted(listed):
            reread = year > current - OPEN_YEARS or year % ROTATION == week % ROTATION
            specs.append(_spec(f"acts_{publisher}_{year}", f"/eli/acts/{publisher}/{year}", not reread))
    return specs


def all_year_specs(today: datetime | None = None) -> list[dict]:
    """Every possible journal year (Bronze reads what Landing holds; a year never landed is skipped)."""
    last = current_year(today)
    return year_specs({p: list(range(FIRST_YEAR, last + 1)) for p in PUBLISHERS}, today)


def listed_years(landing_root: str = "01_landing") -> dict[str, list[int]]:
    """Years per journal from the newest landed publishers answer (ValueError when it lists another journal)."""
    from ingestion.common.bulk_bronze import read_raw
    from ingestion.common.r2 import req, s3
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    held = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, "publishers", landing_root))
    if not held:
        return {}
    answer = json.loads(read_raw(client, bucket, SOURCE, held[-1]))
    found = {p["code"]: [int(y) for y in p["years"]] for p in answer}
    if set(found) != set(PUBLISHERS):
        raise ValueError(f"publishers answer lists {sorted(found)}, expected {list(PUBLISHERS)}")
    return found


def specs(landing_root: str = "01_landing") -> list[dict]:
    return [_spec("publishers", "/eli/acts"), *all_year_specs()]


def robots_allow() -> tuple[bool, str]:
    """(allowed, detail): /robots.txt read first; a disallow of /eli/ stops the run (never worked around)."""
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
    ok = all(parser.can_fetch(USER_AGENT, f"{HOST}{p}") for p in ("/eli/acts", "/eli/acts/DU/2025"))
    return ok, f"robots.txt {answer.status_code}: {'allows' if ok else 'disallows'} /eli/"


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
    status = bulk_file.main(SOURCE, "load", landing_root, specs=[_spec("publishers", "/eli/acts")])
    time.sleep(PAUSE_SECONDS)
    years = year_specs(listed_years(landing_root))
    return max(status, bulk_file.main(SOURCE, "load", landing_root, specs=years, pause_seconds=PAUSE_SECONDS))


def _inspect(landing_root: str) -> int:
    import requests

    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    wanted = year_specs(listed_years(landing_root))
    missing, held_count = [], 0
    for spec in [_spec("publishers", "/eli/acts"), *wanted]:
        if bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, spec["entity"], landing_root)):
            held_count += 1
        else:
            missing.append(spec["entity"])
    try:
        answer = requests.get(f"{HOST}/eli/acts", headers={"User-Agent": bulk_file.USER_AGENT}, timeout=60)
        endpoint = answer.status_code
    except requests.RequestException as exc:
        endpoint = type(exc).__name__
    summary = {"source": SOURCE, "mode": "inspect", "endpoint": endpoint, "held": held_count, "missing": missing}
    print(json.dumps(summary, sort_keys=True))
    annotate("notice" if endpoint == 200 else "error", f"{SOURCE} landing inspect", summary)
    return 0 if endpoint == 200 else 1


ACTIVITIES = {
    "landing.inspect": lambda p: run("inspect"),
    "landing.extract": lambda p: run("load"),
    "landing.verify": lambda p: run("verify"),
}
