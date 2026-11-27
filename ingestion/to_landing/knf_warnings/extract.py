#!/usr/bin/env python3
"""KNF public warning list Landing: one page, reduced before Landing to the fields the register allows.

Route (docs/sources/knf_warnings.md, register PLG-019): https://www.knf.gov.pl/dla_konsumenta/ostrzezenia_publiczne
holds the whole list (about 800 KB of HTML, 668 entries on 27 Nov 2026) in one table per legal-basis section, each
headed "Zawiadomienia o podejrzeniu popełnienia przestępstwa z art. ..."; robots.txt allows every agent. One GET per
run, long timeouts, at most RETRIES tries; a refusal or an unknown layout stops the run, nothing is guessed.

Reduced before Landing (data minimisation, register build limits): an entry names the entity, and an entity can be a
natural person; the entries are notifications of a suspected offence, not findings. So the name, the free-text
columns and every identifier other than a KRS number are never stored, also not in Landing or the Archive. Per entry
only: the section (legal basis) and the entry's number in it (Lp.), the kind of identifier the KNF gives (KRS,
NIP/REGON, not registered in Poland, no data, other), the KRS numbers (exactly ten digits after "KRS"), the
prosecutor's office (its name before any note in parentheses, checked against a closed character set; the cell must
name a "Prokurat…") and whether the column "Wzmianki o prawomocnych orzeczeniach" mentions anything (not "-"). The
entries read must equal the page's own "Liczba znalezionych ostrzeżeń". Entity = ``warning_list``; version =
download time; a new copy is written only when the reduced document's SHA-256 changed (an accumulating snapshot:
Bronze keeps every copy). Modes: inspect, load, verify.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, datetime
from hashlib import sha256
from html.parser import HTMLParser
import json
import os
import re
import time
from typing import Any
import unicodedata

import requests

from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.patterns import bulk_file

SOURCE = "knf_warnings"
ENTITY = "warning_list"
URL = "https://www.knf.gov.pl/dla_konsumenta/ostrzezenia_publiczne"
TIMEOUT = (30, 120)
RETRIES = 3
FIELDS = ["section_key", "lp", "identifier_kind", "krs_numbers", "prosecutor_office", "final_ruling_mentioned"]
HEADER = ("Lp", "Nazwa podmiotu", "Numer identyfikuj", "Właściwa prokuratura", "Wzmianki o prawomocnych",
          "Informacje istotne")  # fmt: skip
SECTION = "Zawiadomienia o podejrzeniu popełnienia przestępstwa"
COUNT = re.compile(r"Liczba znalezionych ostrzeżeń\s*:\s*(\d+)")
LP = re.compile(r"^(\d{1,4})\s*\.?$")
KRS = re.compile(r"KRS\s*:?\s*(\d{10})(?!\d)")
NIP_REGON = re.compile(r"(NIP|REGON)\s*:?\s*\d{9,10}(?!\d)")
NOT_REGISTERED = {"podmiot nie jest zarejestrowany w Polsce", "podmioty nie są zarejestrowane w Polsce"}
NO_DATA = {"brak danych"}
OFFICE_CHAR = re.compile(r"[A-Za-zĄĆĘŁŃÓŚŹŻąćęłńóśźż0-9 .,;:/„”\"'§–—-]")
OFFICE = re.compile(rf"^{OFFICE_CHAR.pattern}{{3,160}}$")
IDENTIFIER_KINDS = ("krs", "nip_regon", "not_registered_pl", "no_data", "other")


class Refused(RuntimeError):
    """A 4xx answer other than 408/429: the run stops, nothing is tried another way."""


class Layout(ValueError):
    """A page of a shape the parser does not know: nothing is guessed from it."""


class Page(HTMLParser):
    """The page's tables (rows of cell texts) and, for each table, the text between the previous table and it."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tables: list[dict[str, Any]] = []
        self.between: list[str] = []
        self.depth, self.skip, self.cell = 0, 0, None

    def handle_starttag(self, tag: str, attrs: list) -> None:
        if tag in ("script", "style"):
            self.skip += 1
        elif tag == "table":
            self.depth += 1
            if self.depth > 1:
                raise Layout("a table inside a table")
            self.tables.append({"before": " ".join(" ".join(self.between).split()), "rows": []})
            self.between = []
        elif tag == "tr" and self.depth:
            self.tables[-1]["rows"].append([])
        elif tag in ("td", "th") and self.depth:
            if not self.tables[-1]["rows"]:
                raise Layout("a cell outside a row")
            self.cell = []
            self.tables[-1]["rows"][-1].append(self.cell)

    def handle_endtag(self, tag: str) -> None:
        if tag in ("script", "style"):
            self.skip = max(0, self.skip - 1)
        elif tag == "table":
            self.depth -= 1
        elif tag in ("td", "th"):
            self.cell = None

    def handle_data(self, data: str) -> None:
        if self.skip:
            return
        if self.cell is not None:
            self.cell.append(data)
        elif not self.depth:
            self.between.append(data)


def fetch() -> tuple[bytes, dict[str, Any]]:
    """One polite GET of the list page: bounded retries with backoff, a 4xx (not 408/429) stops the run."""
    session = requests.Session()
    session.headers.update({"User-Agent": bulk_file.USER_AGENT, "Accept": "text/html"})
    for attempt in range(RETRIES):
        started = time.monotonic()
        try:
            response = session.get(URL, timeout=TIMEOUT)
            if 400 <= response.status_code < 500 and response.status_code not in bulk_file.RETRY_STATUSES:
                raise Refused(f"HTTP {response.status_code}: {URL}")
            if response.status_code != 200:
                raise RuntimeError(f"HTTP {response.status_code}")
            return response.content, {"bytes": len(response.content), "tries": attempt + 1,
                                      "seconds": round(time.monotonic() - started, 1)}  # fmt: skip
        except Refused:
            raise
        except (requests.RequestException, RuntimeError) as exc:
            if attempt == RETRIES - 1:
                raise RuntimeError(f"{type(exc).__name__}: {str(exc)[:200]}") from exc
            print(json.dumps({"status": "retry", "error": str(exc)[:200]}), flush=True)
            time.sleep(30 * (attempt + 1))
    raise RuntimeError("unreachable")


def text(cell: list[str]) -> str:
    return " ".join(" ".join(cell).split())


def slug(value: str) -> str:
    ascii_value = unicodedata.normalize("NFKD", value.replace("ł", "l").replace("Ł", "L")).encode("ascii", "ignore")
    return re.sub(r"[^a-z0-9]+", "_", ascii_value.decode().lower()).strip("_")[:80]


def section_of(before: str) -> tuple[str, str]:
    """The section title (from the last "Zawiadomienia o podejrzeniu ..." before the table) and its key: the legal
    basis up to the first parenthesis, as a slug (art_171_ust_1_3_ustawy_prawo_bankowe)."""
    at = before.rfind(SECTION)
    if at < 0:
        raise Layout(f"a table without a heading starting {SECTION!r}")
    title = before[at:].strip()
    basis = title[len(SECTION) :].strip()
    basis = re.sub(r"^z\s+", "", basis).split(" (", 1)[0].strip()
    if len(title) > 600 or not basis.lower().startswith("art"):
        raise Layout(f"section heading of unknown shape ({len(title)} characters)")
    return title, slug(basis)


def identifier(value: str) -> tuple[str, str]:
    """The kind of identifier the KNF lists and the KRS numbers in it; no other number is kept."""
    krs = KRS.findall(value)
    if krs:
        return "krs", " ".join(dict.fromkeys(krs))
    if NIP_REGON.search(value):
        return "nip_regon", ""
    if value in NOT_REGISTERED:
        return "not_registered_pl", ""
    if value.lower() in NO_DATA:
        return "no_data", ""
    return "other", ""


def parse(body: bytes) -> dict[str, Any]:
    """The reduced document: sections (key, title, entries) and one row of FIELDS per entry, checked against the
    page's stated count; raises Layout on any shape the parser does not know."""
    page_html = body.decode("utf-8")
    page = Page()
    page.feed(page_html)
    stated = COUNT.search(" ".join(re.sub(r"<[^>]+>", " ", page_html).split()))
    if not stated:
        raise Layout("the page states no 'Liczba znalezionych ostrzeżeń'")
    sections, rows, kinds = [], [], Counter()
    for position, table in enumerate(page.tables, start=1):
        title, key = section_of(table["before"])
        if any(s["key"] == key for s in sections):
            raise Layout(f"two sections share the key {key}")
        header, *entries = table["rows"]
        names = [text(c) for c in header]
        if len(names) != len(HEADER) or any(not n.startswith(h) for n, h in zip(names, HEADER, strict=True)):
            raise Layout(f"section {position}: header of unknown shape ({len(names)} columns)")
        numbers = set()
        for n, cells in enumerate(entries, start=1):
            if len(cells) != len(HEADER):
                raise Layout(f"section {position}, entry {n}: {len(cells)} cells")
            lp_match = LP.match(text(cells[0]))
            if not lp_match or int(lp_match.group(1)) in numbers:
                raise Layout(f"section {position}, entry {n}: a number (Lp.) of unknown shape or repeated")
            numbers.add(int(lp_match.group(1)))
            kind, krs = identifier(text(cells[2]))
            listed = text(cells[3])
            office = listed.split("(", 1)[0].strip(" ,;-–")  # the office's name; a note in parentheses is not kept
            if not OFFICE.match(office) or "rokurat" not in listed:
                odd = sorted({c for c in office if not OFFICE_CHAR.match(c)})
                raise Layout(f"section {position}, entry {n}: office of unknown shape ({len(office)}, {odd})")
            ruling = text(cells[4])
            if not ruling:
                raise Layout(f"section {position}, entry {n}: empty ruling column (the page writes '-' for none)")
            kinds[kind] += 1
            rows.append({"section_key": key, "lp": str(int(lp_match.group(1))), "identifier_kind": kind,
                         "krs_numbers": krs, "prosecutor_office": office,
                         "final_ruling_mentioned": "false" if ruling == "-" else "true"})  # fmt: skip
        sections.append({"position": position, "key": key, "title": title, "entries": len(entries)})
    if not sections or len(rows) != int(stated.group(1)):
        raise Layout(f"{len(rows)} entries read in {len(sections)} sections, the page states {stated.group(1)}")
    return {"source": SOURCE, "url": URL, "stated_total": int(stated.group(1)), "fields": FIELDS,
            "sections": sections, "rows": rows, "identifier_kinds": dict(sorted(kinds.items()))}  # fmt: skip


def document(doc: dict[str, Any]) -> bytes:
    """A stable byte form: rows by section position and number, keys sorted (the same list gives the same SHA-256)."""
    order = {s["key"]: s["position"] for s in doc["sections"]}
    rows = sorted(doc["rows"], key=lambda r: (order[r["section_key"]], int(r["lp"])))
    body = {k: v for k, v in doc.items() if k != "identifier_kinds"} | {"rows": rows}
    return json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def land(client: Any, bucket: str, body: bytes, held: list, doc: dict[str, Any], landing_root: str) -> str:
    """Write the reduced document and its receipt unless the newest landed copy has the same SHA-256."""
    digest = sha256(body).hexdigest()
    if held and held[-1]["sha256"] == digest:
        return "unchanged"
    ingested = bulk_file.stamp(datetime.now(UTC))
    key = f"{bulk_file.prefix(SOURCE, ENTITY, landing_root)}{ingested}__{ingested}.json"
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json", Metadata={"sha256": digest})
    head = client.head_object(Bucket=bucket, Key=key)
    if head["ContentLength"] != len(body) or (head.get("Metadata") or {}).get("sha256") != digest:
        raise RuntimeError(f"Landing write could not be verified: {key}")
    receipt = {
        "schema_version": 3, "kind": f"{SOURCE}_landing", "source_id": SOURCE, "entity": ENTITY,
        "content": "snapshot", "area": "data", "version_token": ingested, "version_basis": "download time",
        "ingested_at": ingested, "source_url": URL, "final_url": URL, "sha256": digest, "size_bytes": len(body),
        "object": {"bucket": bucket, "key": key}, "replaces_sha256": held[-1]["sha256"] if held else None,
        "code_sha": os.environ.get("GITHUB_SHA", ""),
        "detail": {"pattern": "html_page", "format": "json", "reduced": "no names, no free text",
                   "stated_total": doc["stated_total"], "sections": len(doc["sections"])},
    }  # fmt: skip
    client.put_object(Bucket=bucket, Key=key[: -len(".json")] + ".receipt.json", ContentType="application/json",
                      Body=json.dumps(receipt, indent=2, sort_keys=True).encode())  # fmt: skip
    return "landed_changed" if held else "landed_new"


def run(mode: str, landing_root: str) -> int:
    """inspect: fetch and parse, compare with the newest landed copy, no writes. load: the same, then land."""
    from ingestion.common.r2 import req, s3

    summary: dict[str, Any] = {"source": SOURCE, "mode": mode}
    failed = ""
    try:
        raw, http = fetch()
        doc = parse(raw)
        body = document(doc)
        client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
        held = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, ENTITY, landing_root))
        digest = sha256(body).hexdigest()
        summary.update({"http": http, "stated_total": doc["stated_total"], "entries": len(doc["rows"]),
                        "sections": {s["key"]: s["entries"] for s in doc["sections"]},
                        "identifier_kinds": doc["identifier_kinds"], "reduced_bytes": len(body), "sha256": digest,
                        "landed_copies": len(held)})  # fmt: skip
        if mode == "inspect":
            summary["outcome"] = "missing" if not held else "current" if held[-1]["sha256"] == digest else "changed"
        else:
            summary["outcome"] = land(client, bucket, body, held, doc, landing_root)
    except (Refused, Layout, RuntimeError, UnicodeDecodeError) as exc:
        failed = f"{type(exc).__name__}: {str(exc)[:300]}"
        summary["failed"] = failed
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True), flush=True)
    annotate("error" if failed else "notice", f"{SOURCE} landing {mode}", summary)
    step_summary(f"## {SOURCE} · landing {mode}\n\n```json\n{json.dumps(summary, indent=2, ensure_ascii=False)}\n```\n")
    return 1 if failed else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--mode", choices=["inspect", "load", "verify"], required=True)
    parser.add_argument("--landing-root", default="01_landing")
    args = parser.parse_args(argv)
    if args.mode == "verify":  # every receipt has its file in Landing or the Archive (bulk_file pattern)
        return bulk_file.main(SOURCE, "verify", landing_root=args.landing_root, specs=[{"entity": ENTITY}])
    return run(args.mode, args.landing_root)


if __name__ == "__main__":
    raise SystemExit(main())
