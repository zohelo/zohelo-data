"""Read-only probe 1 of the NFZ JGP statistics API (PLS-012, #2929): GET only, one request at a time, 1 s apart,
at most 80 requests. Settles the index-of-tables name form, the answer shapes, the small-count markers and the
terms page. Prints to the job log. Deleted before merge (never on the work branch)."""

from __future__ import annotations

from html.parser import HTMLParser
import json
import re
import time
from urllib.parse import quote

import requests

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
BASE = "https://api.nfz.gov.pl/app-stat-api-jgp"
Q = "format=json&api-version=1.1"
LEFT = [80]
S = requests.Session()
S.headers.update({"User-Agent": UA, "Accept": "application/json"})


def get(url: str):
    if LEFT[0] <= 0:
        print("BUDGET USED", url, flush=True)
        return None, None, 0.0
    LEFT[0] -= 1
    time.sleep(1.0)
    t = time.monotonic()
    try:
        r = S.get(url, timeout=60)
    except requests.RequestException as exc:
        print(f"ERR {url}: {type(exc).__name__}", flush=True)
        return None, None, time.monotonic() - t
    took = time.monotonic() - t
    hdr = {k: v[:100] for k, v in r.headers.items() if k.lower() in ("content-type", "content-length", "last-modified", "etag", "retry-after", "api-supported-versions", "api-deprecated-versions") or "rate" in k.lower()}
    print(f"\n=== GET {url}\n--- {r.status_code} {took:.2f}s {len(r.content)}B {hdr}", flush=True)
    return r, r.status_code, took


def js(url: str, head: int = 1500):
    r, st, _ = get(url)
    if r is None:
        return None
    try:
        d = r.json()
    except ValueError:
        print(r.text[:head])
        return None
    print(json.dumps(d, ensure_ascii=False)[:head], flush=True)
    return d


class Text(HTMLParser):
    def __init__(self):
        super().__init__()
        self.out, self.skip = [], 0

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self.skip += 1

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self.skip -= 1

    def handle_data(self, data):
        if not self.skip and data.strip():
            self.out.append(data.strip())


def main() -> None:
    # 1. terms page (once)
    r, st, _ = get(f"{BASE}/terms")
    if r is not None:
        p = Text()
        p.feed(r.text)
        text = re.sub(r"\s+", " ", " ".join(p.out))
        print("TERMS TEXT:", text[:6000], flush=True)
    # 2. dictionaries: every page of every catalog (1a 36, 1b 6, 1c 11, 1d 2, 1w 1 = 56 requests)
    dictionary: dict[str, list] = {}
    for cat in ("1a", "1b", "1c", "1d", "1w"):
        page, items = 1, []
        while True:
            r, st, _ = get(f"{BASE}/benefits?catalog={cat}&page={page}&limit=25&{Q}")
            if r is None or st != 200:
                print("DICT STOP", cat, page, st, (r.text[:300] if r is not None else ""))
                break
            d = r.json()
            if page == 1:
                print("DICT META", cat, json.dumps(d.get("meta"), ensure_ascii=False)[:600], json.dumps(d.get("links"), ensure_ascii=False)[:600])
            items += d.get("data") or []
            if not (d.get("links") or {}).get("next"):
                break
            page += 1
        dictionary[cat] = items
        print(f"DICT {cat} {len(items)} keys={sorted({k for i in items for k in i})}", flush=True)
        for i in items:
            print(f"D|{cat}|{i.get('code')}|{i.get('name')}")
    # 3. index name form: first 1a entry, by code, by name, by "code name"
    first = dictionary["1a"][0] if dictionary.get("1a") else {"code": "A01", "name": "A01"}
    for label, name in (("name", first.get("name")), ("code", first.get("code"))):
        d = js(f"{BASE}/index-of-tables?catalog=1a&name={quote(str(name))}&{Q}", 300)
        if d and d.get("data"):
            print("INDEX FORM WORKS:", label, flush=True)
            break
    # find a childbirth-like group (N section) from the dictionary for the shapes
    target = next((i for i in dictionary.get("1a", []) if str(i.get("code", "")).startswith("N")), first)
    idx = js(f"{BASE}/index-of-tables?catalog=1a&name={quote(str(target.get('name')))}&{Q}", 12000)
    if not (idx and idx.get("data")):
        idx = js(f"{BASE}/index-of-tables?catalog=1a&name={quote(str(target.get('code')))}&{Q}", 12000)
    tables = []
    try:
        for y in idx["data"]["attributes"]["years"]:
            for t in y.get("tables") or []:
                tables.append((y.get("year"), t.get("id"), t.get("type"), (t.get("attributes") or {}).get("resource-name")))
            print("YEAR", y.get("year"), "tables", len(y.get("tables") or []), "periods", len(y.get("periods") or []))
    except Exception as exc:  # noqa: BLE001
        print("index parse", type(exc).__name__, exc)
    print("TABLE TYPES", sorted({(t[2], t[3]) for t in tables}))
    basic = [t for t in tables if (t[3] or t[2] or "").lower().find("basic") >= 0]
    print("BASIC TABLES", basic)
    if basic:
        newest = max(basic)
        oldest = min(basic)
        for tid in (newest[1], oldest[1]):
            js(f"{BASE}/basic-data/{tid}?branch=false&hospitalType=false&page=1&limit=25&{Q}", 4000)
            js(f"{BASE}/basic-data/{tid}?branch=true&hospitalType=false&page=1&limit=25&{Q}", 9000)
        js(f"{BASE}/basic-data/{newest[1]}?{Q}", 3000)
        age = [t for t in tables if t[0] == newest[0] and "age" in (t[3] or t[2] or "").lower()]
        if age:
            js(f"{BASE}/hospitalizations-by-patient-age/{age[0][1]}?branch=false&hospitalType=false&page=1&limit=25&{Q}", 3000)
    # 4. one benefit of each other catalog, and a rare 1a group (small counts → markers)
    for cat in ("1b", "1c", "1d", "1w"):
        if dictionary.get(cat):
            e = dictionary[cat][len(dictionary[cat]) // 2]
            d = js(f"{BASE}/index-of-tables?catalog={cat}&name={quote(str(e.get('name')))}&{Q}", 2500)
            if not (d and d.get("data")):
                js(f"{BASE}/index-of-tables?catalog={cat}&name={quote(str(e.get('code')))}&{Q}", 2500)
    rare = dictionary.get("1a", [first])[-1]
    d = js(f"{BASE}/index-of-tables?catalog=1a&name={quote(str(rare.get('name')))}&{Q}", 3000)
    try:
        ys = d["data"]["attributes"]["years"]
        tid = [t for y in ys for t in (y.get("tables") or []) if "basic" in ((t.get("attributes") or {}).get("resource-name") or t.get("type") or "").lower()]
        if tid:
            js(f"{BASE}/basic-data/{tid[-1]['id']}?branch=true&hospitalType=false&page=1&limit=25&{Q}", 9000)
    except Exception as exc:  # noqa: BLE001
        print("rare parse", type(exc).__name__, exc)
    print("REQUESTS LEFT", LEFT[0])


if __name__ == "__main__":
    main()
