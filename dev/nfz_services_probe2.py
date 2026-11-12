"""Read-only probe 2 of the NFZ JGP statistics API (PLS-012, #2929): GET only, one request at a time, 1 s apart,
at most 80 requests. basic-data shapes (Poland, by branch), small-count markers, stability of the ``data``
members between two requests, the age/sex tables' size, and a sample of indexes for the volume estimate.
Prints to the job log. Deleted before merge (never on the work branch)."""

from __future__ import annotations

from collections import Counter
from hashlib import sha256
import json
import random
import time
from urllib.parse import quote

import requests

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
BASE = "https://api.nfz.gov.pl/app-stat-api-jgp"
Q = "format=json&api-version=1.1"
LEFT = [80]
S = requests.Session()
S.headers.update({"User-Agent": UA, "Accept": "application/json"})

KPI = {  # catalog, product code, dictionary name (from probe 43311426786)
    "N01": ("1a", "5.51.01.0013001", "N01 PORÓD *"),
    "H13": ("1a", "5.51.01.0008013", "H13 ENDOPROTEZOPLASTYKA PIERWOTNA CAŁKOWITA BIODRA *"),
    "H15": ("1a", "5.51.01.0008015", "H15 ENDOPROTEZOPLASTYKA PIERWOTNA CAŁKOWITA KOLANA *"),
    "B19": ("1a", "5.51.01.0002019", "B19 USUNIĘCIE ZAĆMY NIEPOWIKŁANEJ Z JEDNOCZESNYM WSZCZEPIENIEM SOCZEWKI"),
    "A48": ("1a", "5.51.01.0001048", "A48 KOMPLEKSOWE LECZENIE UDARÓW MÓZGU > 7 DNI W ODDZIALE UDAROWYM"),
    "1w": ("1w", "03.4663.033.02", "PRZESZCZEPIENIE SERCA I PŁUCA"),
}


def get(url: str):
    if LEFT[0] <= 0:
        print("BUDGET USED", url, flush=True)
        return None
    LEFT[0] -= 1
    time.sleep(1.0)
    t = time.monotonic()
    try:
        r = S.get(url, timeout=60)
    except requests.RequestException as exc:
        print(f"ERR {url}: {type(exc).__name__}", flush=True)
        return None
    print(f"\n=== GET {url}\n--- {r.status_code} {time.monotonic() - t:.2f}s {len(r.content)}B", flush=True)
    try:
        return r.json()
    except ValueError:
        print(r.text[:500])
        return None


def dg(x) -> str:
    return sha256(json.dumps(x, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16]


def general(index) -> list[tuple[int, str]]:
    out = []
    for y in ((index or {}).get("data") or {}).get("attributes", {}).get("years") or []:
        ids = [t["id"] for t in y.get("tables") or [] if t.get("type") == "general-data"]
        out += [(y["year"], i) for i in ids]
        if len(ids) != 1:
            print("YEAR WITH", len(ids), "general-data tables:", y["year"])
        if y.get("periods"):
            print("PERIODS in", y["year"], json.dumps(y["periods"], ensure_ascii=False)[:600])
    return sorted(out)


def index(cat: str, name: str):
    return get(f"{BASE}/index-of-tables?catalog={cat}&name={quote(name, safe='')}&{Q}")


def show(doc, n=6000):
    print(json.dumps(doc, ensure_ascii=False)[:n], flush=True)


def main() -> None:
    cat, code, name = KPI["N01"]
    idx = index(cat, name)
    idx2 = index(cat, name)
    print("INDEX DATA DIGEST equal on two requests:", dg((idx or {}).get("data")) == dg((idx2 or {}).get("data")),
          "meta.date-modified", (idx or {}).get("meta", {}).get("date-modified"), (idx2 or {}).get("meta", {}).get("date-modified"))
    tabs = general(idx)
    print("N01 general-data", tabs)
    newest, oldest = tabs[-1], tabs[0]
    a = get(f"{BASE}/basic-data/{newest[1]}?branch=false&hospitalType=false&page=1&limit=25&{Q}")
    show(a, 4000)
    b = get(f"{BASE}/basic-data/{newest[1]}?branch=true&hospitalType=false&page=1&limit=25&{Q}")
    show(b, 9000)
    b2 = get(f"{BASE}/basic-data/{newest[1]}?branch=true&hospitalType=false&page=1&limit=25&{Q}")
    print("BASIC DATA DIGEST equal on two requests:", dg((b or {}).get("data")) == dg((b2 or {}).get("data")))
    show(get(f"{BASE}/basic-data/{oldest[1]}?branch=false&hospitalType=false&page=1&limit=25&{Q}"), 3000)
    show(get(f"{BASE}/basic-data/{oldest[1]}?branch=true&hospitalType=false&page=1&limit=25&{Q}"), 9000)
    show(get(f"{BASE}/basic-data/{newest[1]}?branch=true&hospitalType=false&page=2&limit=25&{Q}"), 1500)
    # age and sex tables (size, for the report)
    years = (idx.get("data") or {}).get("attributes", {}).get("years") or []
    last = [y for y in years if y["year"] == newest[0]][0]
    for kind, path in (("hospitalization-by-age", "hospitalizations-by-patient-age"), ("hospitalization-by-gender", "hospitalizations-by-patient-gender")):
        t = [x for x in last["tables"] if x["type"] == kind]
        if t:
            d = get(f"{BASE}/{path}/{t[0]['id']}?branch=false&hospitalType=false&page=1&limit=25&{Q}")
            print(kind, "meta.count", (d or {}).get("meta", {}).get("count"))
            show(d, 2500)
    # the other KPI groups and a rare benefit (small counts by branch)
    for key in ("H13", "H15", "B19", "A48", "1w"):
        c, _code, nm = KPI[key]
        t = general(index(c, nm))
        print(key, "general-data years", [y for y, _ in t])
        if t:
            show(get(f"{BASE}/basic-data/{t[-1][1]}?branch=true&hospitalType=false&page=1&limit=25&{Q}"), 7000)
    # a random sample of the dictionary for the volume estimate (dictionary names fetched once more, 1 page each)
    random.seed(2929)
    sample = []
    for c, pages in (("1a", 36), ("1b", 6), ("1c", 11), ("1d", 2)):
        for p in random.sample(range(1, pages + 1), 2):
            d = get(f"{BASE}/benefits?catalog={c}&page={p}&limit=25&{Q}")
            sample += [(c, i["name"]) for i in random.sample((d or {}).get("data") or [], min(3, len((d or {}).get("data") or [])))]
    stats = Counter()
    for c, nm in sample:
        if LEFT[0] <= 1:
            break
        t = general(index(c, nm))
        ys = [y for y, _ in t]
        stats[c] += 1
        print(f"SAMPLE|{c}|{nm[:60]}|{len(ys)}|{min(ys) if ys else None}|{max(ys) if ys else None}")
    print("SAMPLED", dict(stats), "REQUESTS LEFT", LEFT[0])


if __name__ == "__main__":
    main()
