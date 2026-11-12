"""Open KRS API: current extracts (OdpisAktualny) per KRS number, reduced to entity-level fields before Landing.

Selection: the KRS numbers of silver.gleif_legal_entities (registration authority RA000484 = KRS, id of exactly
ten digits); the API has no list of numbers, so the list comes from a source already loaded. Requests go one at
a time with a pause: register P (entrepreneurs) first and, when it answers 204 or 404 (no current extract there),
register S (associations and other organisations); no extract in either is recorded as a number without a
current extract (``http_status`` of the last answer), so it is not asked again every run. Many such answers in a
row stop the run and are not kept: that is what a moved endpoint would look like.

Personal data: a full answer names board members, partners, proxies and liquidators (with PESEL). ``reduce``
copies only the allowlisted entity-level fields into a new record; everything else, every section about
persons included, is dropped in memory and never written (Landing, Archive, logs).

Landing: 16 fixed entities ``extracts_00`` … ``extracts_15`` (KRS number modulo 16). A file is the complete
current state of the part: one JSON line per number held, sorted by number, so unchanged data gives the same
bytes. A changed part is landed as a new version (receipt after the file); Bronze keeps the newest version.

Incremental: ``/api/Krs/Biuletyn/<day>`` lists the KRS numbers with an entry on that day. A run first reads
the days after each part's ``bulletin_through`` (receipt detail) up to yesterday (Warsaw) and asks again for
every held number listed on a day not before its last request; a part's ``bulletin_through`` moves to
yesterday only when all of them were answered. Then it continues with selected numbers not held yet, in
ascending order, until the time budget ends; the next run continues from Landing.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
import json
import os
import time
from typing import Any
from zoneinfo import ZoneInfo

import requests

from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.ms_krs import PARTS, SOURCE, specs
from ingestion.to_landing.patterns import bulk_file

EXTRACT_URL = "https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/{krs}?rejestr={register}&format=json"
BULLETIN_URL = "https://api-krs.ms.gov.pl/api/Krs/Biuletyn/{day}"
KRS_AUTHORITY = "RA000484"  # GLEIF registration authority code of the Krajowy Rejestr Sądowy
WARSAW = ZoneInfo("Europe/Warsaw")
PAUSE_SECONDS = 1.0  # between requests; the API publishes no limit
BUDGET_MINUTES = 120.0  # per run, about 4,300 requests
BULLETIN_MAX_DAYS = 45  # bulletin days read in one run; older unread days are reported, not read
FLUSH_RECORDS = 500  # new records between Landing writes, so a failed run loses little
RETRY_WAITS = (10, 30)  # seconds before the second and third attempt
RETRY_STATUSES = {408, 429, 500, 502, 503, 504}
NO_EXTRACT = {204, 404}  # the register holds no current extract of the number (P answers 204, S answers 404)
NO_EXTRACT_RUN = 50  # numbers in a row without an extract that stop a run (about 1 in 4 has none)

# The allowlist: reduced field → path in the answer below ``odpis``. Nothing else is kept.
HEADER = {
    "register": "naglowekA.rejestr",
    "extract_kind": "rodzaj",
    "state_as_of": "naglowekA.stanZDnia",
    "registered_on": "naglowekA.dataRejestracjiWKRS",
    "last_entry_number": "naglowekA.numerOstatniegoWpisu",
    "last_entry_on": "naglowekA.dataOstatniegoWpisu",
    "position_state": "naglowekA.stanPozycji",
    "legal_form": "dane.dzial1.danePodmiotu.formaPrawna",
    "name": "dane.dzial1.danePodmiotu.nazwa",
    "nip": "dane.dzial1.danePodmiotu.identyfikatory.nip",
    "regon": "dane.dzial1.danePodmiotu.identyfikatory.regon",
    "has_opp_status": "dane.dzial1.danePodmiotu.czyPosiadaStatusOPP",
    "seat_country": "dane.dzial1.siedzibaIAdres.siedziba.kraj",
    "seat_voivodeship": "dane.dzial1.siedzibaIAdres.siedziba.wojewodztwo",
    "seat_powiat": "dane.dzial1.siedzibaIAdres.siedziba.powiat",
    "seat_gmina": "dane.dzial1.siedzibaIAdres.siedziba.gmina",
    "seat_town": "dane.dzial1.siedzibaIAdres.siedziba.miejscowosc",
    "share_capital": "dane.dzial1.kapital.wysokoscKapitaluZakladowego.wartosc",
    "share_capital_currency": "dane.dzial1.kapital.wysokoscKapitaluZakladowego.waluta",
}
# Derived from allowlisted paths: codes of the main activity, and how many entries a status section holds
# (and how many of them carry an end), never the entries themselves (they name liquidators and trustees).
DERIVED = (
    "main_pkd",
    "liquidation_entries",
    "liquidation_ended",
    "bankruptcy_entries",
    "bankruptcy_ended",
    "restructuring_entries",
    "dissolution_entry",
)
FIELDS = ("krs_number", "requested_on", "http_status", *HEADER, *DERIVED)
RESTRUCTURING = "postepowanieRestrukturyzacyjneNaprawczePrzymusowaRestrukturyzacjaUporzadkowanaLikwidacja"


class Stop(RuntimeError):
    """The API refused or kept failing: the run lands what it holds and ends with an error."""


def _at(node: Any, path: str) -> Any:
    for part in path.split("."):
        node = node.get(part) if isinstance(node, dict) else None
    return node


def _scalar(value: Any) -> Any:
    return value if isinstance(value, (str, int, bool)) else None


def _items(value: Any) -> list[dict]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def reduce(krs: str, document: Any, requested_on: str) -> dict[str, Any]:
    """The allowlisted entity-level record of one answer; raises ValueError on an answer for another number."""
    odpis = document.get("odpis") if isinstance(document, dict) else None
    if not isinstance(odpis, dict) or _at(odpis, "naglowekA.numerKRS") != krs:
        raise ValueError(f"answer is not the extract of {krs}")
    record: dict[str, Any] = {"krs_number": krs, "requested_on": requested_on, "http_status": 200}
    for field, path in HEADER.items():
        record[field] = _scalar(_at(odpis, path))
    main = _items(_at(odpis, "dane.dzial3.przedmiotDzialalnosci.przedmiotPrzewazajacejDzialalnosci"))
    record["main_pkd"] = [
        ".".join(str(item.get(key) or "") for key in ("kodDzial", "kodKlasa", "kodPodklasa")) for item in main
    ]
    status = _at(odpis, "dane.dzial6")
    status = status if isinstance(status, dict) else {}
    liquidation = _items(status.get("likwidacja"))
    bankruptcy = _items(status.get("postepowanieUpadlosciowe"))
    record["liquidation_entries"] = len(liquidation)
    record["liquidation_ended"] = sum(1 for item in liquidation if item.get("zakonczenieLikwidacji"))
    record["bankruptcy_entries"] = len(bankruptcy)
    record["bankruptcy_ended"] = sum(1 for item in bankruptcy if item.get("opisZakonczeniaProcesuUpadlosci"))
    record["restructuring_entries"] = len(_items(status.get(RESTRUCTURING)))
    record["dissolution_entry"] = bool(status.get("rozwiazanieUniewaznienie"))
    return record


def encode(records: dict[str, dict[str, Any]]) -> bytes:
    """One JSON line per KRS number, sorted by number and by key: the same records give the same bytes."""
    lines = [
        json.dumps(records[krs], ensure_ascii=False, sort_keys=True, separators=(",", ":")) for krs in sorted(records)
    ]
    return ("\n".join(lines) + "\n").encode("utf-8") if lines else b""


def decode(body: bytes) -> dict[str, dict[str, Any]]:
    records = {}
    for line in body.decode("utf-8").splitlines():
        record = json.loads(line)
        records[record["krs_number"]] = record
    return records


def part_of(krs: str) -> str:
    return f"extracts_{int(krs) % PARTS:02d}"


def selection(upto: str | None = None) -> list[str]:
    """KRS numbers of the GLEIF entities registered in KRS (exactly ten digits), ascending."""
    from ingestion.common.r2 import catalog

    table = catalog().load_table(("silver", "gleif_legal_entities"))
    rows = table.scan(
        row_filter=f"registration_authority_id == '{KRS_AUTHORITY}'",
        selected_fields=("registration_authority_entity_id",),
    ).to_arrow()
    numbers = {
        value
        for value in rows.column("registration_authority_entity_id").to_pylist()
        if value and len(value) == 10 and value.isascii() and value.isdigit()
    }
    return sorted(n for n in numbers if upto is None or n <= upto)


class Api:
    """One polite session: a request, then a pause; bounded retries with back-off; counts by outcome."""

    def __init__(self, pause_seconds: float = PAUSE_SECONDS) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": bulk_file.USER_AGENT, "Accept": "application/json"})
        self.pause_seconds = pause_seconds
        self.requests = 0

    def get(self, url: str) -> tuple[int, Any]:
        """(200, parsed JSON) or (204 or 404, None); anything else is retried or stops the run."""
        problem = ""
        for attempt in range(len(RETRY_WAITS) + 1):
            if attempt:
                time.sleep(RETRY_WAITS[attempt - 1])
            self.requests += 1
            try:
                response = self.session.get(url, timeout=(20, 60))
            except requests.RequestException as exc:
                problem = type(exc).__name__
                continue
            finally:
                time.sleep(self.pause_seconds)
            if response.status_code in NO_EXTRACT:
                return response.status_code, None
            if response.status_code == 200:
                try:
                    return 200, response.json()
                except ValueError:
                    problem = "200 without JSON"
                    continue
            problem = f"HTTP {response.status_code}"
            if response.status_code not in RETRY_STATUSES:
                break  # a refusal (403, 404 …) is not asked again and never worked around
        raise Stop(f"{problem}: {url}")

    def extract(self, krs: str, requested_on: str) -> tuple[dict[str, Any], str]:
        """(reduced record, outcome) for one number: register P, then S when P holds no current extract."""
        for register in ("P", "S"):
            status, document = self.get(EXTRACT_URL.format(krs=krs, register=register))
            if status == 200:
                try:
                    return reduce(krs, document, requested_on), register
                except ValueError as exc:
                    raise Stop(str(exc)) from exc
        return {"krs_number": krs, "requested_on": requested_on, "http_status": status}, "none"

    def bulletin(self, day: date) -> set[str]:
        """KRS numbers (ten digits) with an entry on that day; a day without entries is empty."""
        status, listed = self.get(BULLETIN_URL.format(day=day.isoformat()))
        if status == 204:
            return set()
        if status != 200 or not isinstance(listed, list):
            raise Stop(f"bulletin of {day} is not a list")
        return {f"{int(value):010d}" for value in listed if str(value).isdigit()}


def _land(client: Any, bucket: str, part: dict[str, Any], landing_root: str) -> bool:
    """Write the part's current records as a new version unless the bytes are those of its newest copy."""
    body = encode(part["records"])
    digest, held = sha256(body).hexdigest(), part["held"]
    if not body or (held and held[-1]["sha256"] == digest):
        return False
    ingested = bulk_file.stamp(datetime.now(UTC))
    key = f"{bulk_file.prefix(SOURCE, part['entity'], landing_root)}{ingested}__{ingested}.jsonl"
    client.put_object(
        Bucket=bucket, Key=key, Body=body, ContentType="application/x-ndjson", Metadata={"sha256": digest}
    )
    head = client.head_object(Bucket=bucket, Key=key)
    if head["ContentLength"] != len(body) or (head.get("Metadata") or {}).get("sha256") != digest:
        raise RuntimeError(f"Landing write could not be verified: {key}")
    through = part["through"]
    receipt = {
        "schema_version": 3,
        "kind": f"{SOURCE}_landing",
        "source_id": SOURCE,
        "entity": part["entity"],
        "content": "snapshot",
        "area": "data",
        "version_token": ingested,
        "version_basis": "download time",
        "ingested_at": ingested,
        "source_url": EXTRACT_URL.format(krs="<krs>", register="<P|S>"),
        "final_url": EXTRACT_URL.format(krs="<krs>", register="<P|S>"),
        "sha256": digest,
        "size_bytes": len(body),
        "object": {"bucket": bucket, "key": key},
        "replaces_sha256": held[-1]["sha256"] if held else None,
        "code_sha": os.environ.get("GITHUB_SHA", ""),
        "detail": {
            "pattern": "ms_krs",
            "format": "jsonl",
            "reduced": "entity-level allowlist (ingestion/to_landing/ms_krs/extract.py)",
            "records": len(part["records"]),
            "with_extract": sum(1 for r in part["records"].values() if r["http_status"] == 200),
            "bulletin_through": through.isoformat() if through else None,
        },
    }
    client.put_object(
        Bucket=bucket,
        Key=key.rsplit(".", 1)[0] + ".receipt.json",
        ContentType="application/json",
        Body=json.dumps(receipt, indent=2, sort_keys=True).encode(),
    )
    part["held"] = [*held, receipt]
    return True


def held_parts(client: Any, bucket: str, landing_root: str) -> dict[str, dict[str, Any]]:
    """Every part with its receipts, the records of its newest copy and the bulletin day it is settled through."""
    from ingestion.common.bulk_bronze import read_raw

    parts = {}
    for spec in specs():
        held = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, spec["entity"], landing_root))
        records = decode(read_raw(client, bucket, SOURCE, held[-1])) if held else {}
        through = (held[-1].get("detail") or {}).get("bulletin_through") if held else None
        parts[spec["entity"]] = {
            "entity": spec["entity"],
            "held": held,
            "records": records,
            "through": date.fromisoformat(through) if through else None,
        }
    return parts


def extract(
    budget_minutes: float,
    landing_root: str = "01_landing",
    upto: str | None = None,
    today: date | None = None,
    pause_seconds: float = PAUSE_SECONDS,
) -> int:
    """One run. ``today`` (Warsaw date of the run) and ``landing_root`` exist for Dev trials."""
    from ingestion.common.r2 import req, s3

    started = time.monotonic()
    deadline = started + min(budget_minutes, BUDGET_MINUTES) * 60
    today = today or datetime.now(WARSAW).date()
    yesterday, requested_on = today - timedelta(days=1), today.isoformat()
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    numbers = selection(upto)
    parts = held_parts(client, bucket, landing_root)
    held = {krs: record for part in parts.values() for krs, record in part["records"].items()}
    api = Api(pause_seconds)
    outcomes = {"P": 0, "S": 0, "none": 0}
    summary: dict[str, Any] = {"source": SOURCE, "mode": "load", "selection": len(numbers), "held_before": len(held)}
    landed, error, since_flush = 0, None, 0

    without: list[tuple[str, dict[str, Any] | None]] = []  # the current run of numbers without an extract

    def ask(krs: str) -> None:
        record, outcome = api.extract(krs, requested_on)
        outcomes[outcome] += 1
        records = parts[part_of(krs)]["records"]
        if outcome == "none":
            without.append((krs, records.get(krs)))
        else:
            without.clear()
        records[krs] = record
        if len(without) >= NO_EXTRACT_RUN:
            for number, previous in without:  # not kept: put back what was held before
                if previous is None:
                    del parts[part_of(number)]["records"][number]
                else:
                    parts[part_of(number)]["records"][number] = previous
            raise Stop(f"{NO_EXTRACT_RUN} numbers in a row without an extract, from {without[0][0]}: not kept")

    def flush() -> int:
        return sum(_land(client, bucket, part, landing_root) for part in parts.values())

    try:
        # 1. Held numbers with an entry since their last request (the daily bulletins).
        starts = [
            part["through"] + timedelta(days=1)
            if part["through"]
            else min(date.fromisoformat(r["requested_on"]) for r in part["records"].values())
            for part in parts.values()
            if part["records"]
        ]
        first = min(starts, default=today)
        unread = max((yesterday - first).days + 1 - BULLETIN_MAX_DAYS, 0)
        first += timedelta(days=unread)
        days = [first + timedelta(days=n) for n in range((yesterday - first).days + 1)]
        again: set[str] = set()
        for day in days:
            listed = api.bulletin(day) & set(held)
            again |= {krs for krs in listed if date.fromisoformat(held[krs]["requested_on"]) <= day}
        settled = True
        for krs in sorted(again):
            if time.monotonic() > deadline:
                settled = False
                break
            ask(krs)
        if settled:  # every part is settled through yesterday, also one without listed numbers
            for part in parts.values():
                part["through"] = yesterday
        summary.update(
            {
                "bulletin_days": [days[0].isoformat(), days[-1].isoformat()] if days else [],
                "bulletin_days_unread": unread,
                "listed_again": len(again),
                "asked_again": sum(outcomes.values()),
                "bulletin_settled": settled,
            }
        )
        # 2. Selected numbers not held yet, ascending.
        missing = [krs for krs in numbers if krs not in held]
        asked = 0
        for krs in missing:
            if time.monotonic() > deadline:
                break
            ask(krs)
            asked += 1
            since_flush += 1
            if since_flush >= FLUSH_RECORDS:
                landed += flush()
                since_flush = 0
        summary.update({"missing_before": len(missing), "asked_new": asked, "remaining": len(missing) - asked})
    except Stop as exc:
        error = str(exc)[:300]
    finally:
        landed += flush()
    summary.update(
        {
            "answers": {
                "register_P": outcomes["P"],
                "register_S": outcomes["S"],
                "no_current_extract": outcomes["none"],
            },
            "requests": api.requests,
            "parts_landed": landed,
            "held_after": sum(len(part["records"]) for part in parts.values()),
            "seconds": round(time.monotonic() - started),
        }
    )
    if error:
        summary["error"] = error
    print(json.dumps(summary, sort_keys=True))
    if unread:
        annotate("warning", f"{SOURCE} landing load: bulletin days not read", {"days": unread, "before": str(first)})
    annotate("error" if error else "notice", f"{SOURCE} landing load", summary)
    step_summary(f"## {SOURCE} · landing load\n\n```json\n{json.dumps(summary, indent=2)}\n```\n")
    return 1 if error else 0


def inspect(landing_root: str = "01_landing") -> int:
    from ingestion.common.r2 import req, s3

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    files = []
    for spec in specs():
        held = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, spec["entity"], landing_root))
        detail = (held[-1].get("detail") or {}) if held else {}
        files.append(
            {
                "entity": spec["entity"],
                "landed_versions": len(held),
                "newest": held[-1]["version_token"] if held else None,
                "records": detail.get("records"),
                "bulletin_through": detail.get("bulletin_through"),
            }
        )
    summary: dict[str, Any] = {"source": SOURCE, "mode": "inspect", "files": files}
    try:
        summary["selection"] = len(selection())
    except Exception as exc:
        summary["selection"] = f"{type(exc).__name__}: {str(exc)[:200]}"
    yesterday = datetime.now(WARSAW).date() - timedelta(days=1)
    try:
        summary["bulletin"] = {"day": yesterday.isoformat(), "numbers": len(Api().bulletin(yesterday))}
        status = 0
    except Stop as exc:
        summary["bulletin"] = {"day": yesterday.isoformat(), "error": str(exc)[:300]}
        status = 1
    print(json.dumps(summary, sort_keys=True))
    annotate("notice" if status == 0 else "error", f"{SOURCE} landing inspect", summary)
    return status
