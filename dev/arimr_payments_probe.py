"""Read-only probe of ARiMR statistics (register PLS-034): attachment inventory of the gov.pl scheme pages and
ARiMR's datasets on dane.gov.pl. GET only, one request at a time, PAUSE seconds apart, robots.txt honoured,
bounded request budget. Nothing is written to storage; the inventory goes to the job log and DEV_OUTPUT_DIR."""

from __future__ import annotations

import json
import os
import re
import time
import urllib.robotparser
from urllib.parse import urlparse

import requests

from ingestion.common.gus_publications import _links
from ingestion.to_landing.patterns.bulk_file import USER_AGENT

GOV = "https://www.gov.pl"
INDEX = GOV + "/web/arimr/wdrazane-programy-i-dzialania---dane-liczbowe"
HOME = GOV + "/web/arimr"
PAUSE = 1.5
BUDGET = [200]
OUT = os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output")
RP = urllib.robotparser.RobotFileParser()
inventory: dict = {"pages": {}, "files": [], "dane": {}}


def spend() -> bool:
    if BUDGET[0] <= 0:
        return False
    BUDGET[0] -= 1
    time.sleep(PAUSE)
    return True


def links(url: str) -> list[tuple[str, str]]:
    if not RP.can_fetch(USER_AGENT, url) or not spend():
        print("skip (robots or budget)", url)
        return []
    try:
        return _links(url)
    except Exception as exc:  # noqa: BLE001
        print(f"ERR {url}: {type(exc).__name__} {str(exc)[:150]}")
        return []


def arimr(href: str) -> bool:
    p = urlparse(href)
    return p.netloc.endswith("gov.pl") and p.path.startswith("/web/arimr/") and "/attachment/" not in p.path


def sample(url: str, text: str, page: str) -> None:
    if not spend():
        return
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=90)
    except Exception as exc:  # noqa: BLE001
        print(f"ERR {url}: {exc}")
        return
    body = resp.content
    keep = ("content-type", "last-modified", "content-disposition", "etag")
    hdr = {k: v for k, v in resp.headers.items() if k.lower() in keep}
    print(f"\n--- FILE {text!r} ({page})\n    {url} {resp.status_code} {len(body)} bytes {hdr}")
    inventory["files"].append({"url": url, "text": text, "page": page, "bytes": len(body), "headers": hdr})
    try:
        if body[:2] == b"PK":
            from ingestion.common import xlsx_cells

            rows: dict = {}
            sheetrows: dict = {}
            for c in xlsx_cells.cells(body):
                sheetrows[c["sheet_name"]] = max(sheetrows.get(c["sheet_name"], 0), c["row_number"])
                if c["row_number"] <= 16:
                    rows.setdefault((c["sheet_name"], c["row_number"]), []).append(
                        f"{c['cell_ref']}={c['value_text'][:45]}"
                    )
            print("    sheets (max row):", sheetrows)
            shown = 0
            for (s, n), cs in rows.items():
                if list(sheetrows).index(s) < 3 and shown < 60:
                    shown += 1
                    print(f"    [{s}] r{n}: {' | '.join(cs[:16])}")
        elif body[:4] == b"\xd0\xcf\x11\xe0":
            import xlrd

            wb = xlrd.open_workbook(file_contents=body)
            print("    sheets:", [(s.name, s.nrows, s.ncols) for s in wb.sheets()])
            for sh in wb.sheets()[:3]:
                for n in range(min(16, sh.nrows)):
                    print(f"    [{sh.name}] r{n + 1}: {' | '.join(str(v)[:40] for v in sh.row_values(n)[:16])}")
        else:
            print("    head:", body[:800].decode("utf-8", "replace"))
    except Exception as exc:  # noqa: BLE001
        print(f"    parse ERR {type(exc).__name__}: {exc}")


def dane() -> None:
    api = "https://api.dane.gov.pl/1.4"
    ids = []
    for q in ("Agencja Restrukturyzacji i Modernizacji Rolnictwa", "ARiMR"):
        if not spend():
            return
        try:
            j = requests.get(
                f"{api}/institutions", params={"q": q, "per_page": 20}, headers={"User-Agent": USER_AGENT}, timeout=60
            ).json()
            for inst in j.get("data", []):
                a = inst["attributes"]
                print("\n=== DANE institution", inst.get("id"), a.get("title"), a.get("datasets_count"), a.get("website"))
                ids.append(inst.get("id"))
        except Exception as exc:  # noqa: BLE001
            print(f"dane ERR {type(exc).__name__}: {str(exc)[:200]}")
    for q in ("ARiMR", "płatności bezpośrednie", "Agencja Restrukturyzacji"):
        if not spend():
            return
        try:
            j = requests.get(
                f"{api}/datasets", params={"q": q, "per_page": 30}, headers={"User-Agent": USER_AGENT}, timeout=60
            ).json()
            print(f"\n=== DANE dataset search {q!r}: {j.get('meta', {}).get('count')}")
            for ds in j.get("data", []):
                a = ds["attributes"]
                print(f"  HIT {ds['id']} {a.get('title')!r} inst={a.get('institution', {})} modified={a.get('modified')}")
        except Exception as exc:  # noqa: BLE001
            print(f"dane ERR {type(exc).__name__}: {str(exc)[:200]}")
    for iid in list(dict.fromkeys(ids))[:3]:
        if not spend():
            return
        try:
            j = requests.get(
                f"{api}/institutions/{iid}/datasets",
                params={"per_page": 100},
                headers={"User-Agent": USER_AGENT},
                timeout=60,
            ).json()
        except Exception as exc:  # noqa: BLE001
            print(f"dane ERR {exc}")
            continue
        for ds in j.get("data", []):
            a = ds["attributes"]
            print(
                f"  DATASET {ds['id']} {a.get('title')!r} modified={a.get('modified')} "
                f"freq={a.get('update_frequency')} licence={a.get('license_name') or a.get('license_chosen')} "
                f"formats={a.get('formats')}"
            )
            inventory["dane"].setdefault("datasets", []).append({"id": ds["id"], "title": a.get("title")})
            if not spend():
                continue
            try:
                rj = requests.get(
                    f"{api}/datasets/{ds['id']}/resources",
                    params={"per_page": 50},
                    headers={"User-Agent": USER_AGENT},
                    timeout=60,
                ).json()
                for res in rj.get("data", [])[:25]:
                    ra = res["attributes"]
                    print(
                        f"    RES {res['id']} {ra.get('title')!r} {ra.get('format')} "
                        f"{ra.get('data_date') or ra.get('modified')} {ra.get('download_url') or ra.get('link')}"
                    )
            except Exception as exc:  # noqa: BLE001
                print(f"    res ERR {exc}")


def main() -> None:
    os.makedirs(OUT, exist_ok=True)
    r = requests.get(GOV + "/robots.txt", headers={"User-Agent": USER_AGENT}, timeout=30)
    print("robots.txt", r.status_code, r.text[:800])
    RP.parse(r.text.splitlines() if r.ok else [])
    nav = {h.split("#")[0] for h, _ in links(HOME)}
    idx = links(INDEX)
    print(f"\n=== INDEX {INDEX}: {len(idx)} links")
    scheme: list[str] = []
    for h, t in idx:
        h = h.split("#")[0]
        if arimr(h) and h not in nav and h != INDEX and h not in scheme:
            scheme.append(h)
            print(f"  SCHEME {t!r} -> {h}")
        elif "/attachment/" in h:
            print(f"  ATT {t!r} -> {h}")
    seen = set(scheme) | {INDEX}
    queue = [(h, 1) for h in scheme]
    while queue:
        page, depth = queue.pop(0)
        ls = links(page)
        atts = [(h, t) for h, t in ls if "/attachment/" in h]
        subs = {h.split("#")[0] for h, _ in ls if arimr(h) and h.split("#")[0] not in nav} - seen
        inventory["pages"][page] = {"attachments": atts, "subpages": sorted(subs)}
        print(f"\n=== PAGE d{depth} {page}: {len(atts)} attachments, {len(subs)} new subpages")
        for h, t in atts:
            print(f"  ATT {t!r} -> {h}")
        for s in sorted(subs):
            print(f"  SUB {s}")
            seen.add(s)
            if depth < 2 and re.search(r"dane|statyst|platnosc|wnios|kampani|liczb|zestawien|realizac", s):
                queue.append((s, depth + 1))
    json.dump(inventory, open(os.path.join(OUT, "inventory.json"), "w"), ensure_ascii=False, indent=1)

    sheets: list[tuple[str, str, str]] = []
    for page, d in inventory["pages"].items():
        for h, t in d["attachments"]:
            if not re.search(r"\bpdf\b", t, re.I) and h not in {s[0] for s in sheets}:
                sheets.append((h, t, page))
    print(f"\n=== {len(sheets)} spreadsheet attachments")
    # one or two per page first, so every scheme is sampled before the budget runs out
    per_page: dict[str, int] = {}
    for h, t, page in sheets:
        if per_page.get(page, 0) < 2:
            per_page[page] = per_page.get(page, 0) + 1
            sample(h, t, page)
    dane()
    json.dump(inventory, open(os.path.join(OUT, "inventory.json"), "w"), ensure_ascii=False, indent=1)
    print("\nrequests left", BUDGET[0])


if __name__ == "__main__":
    main()
