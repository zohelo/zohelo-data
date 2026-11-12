"""Second read-only probe of the NFZ contracts API (PLS-014, Task #2955): sizes and shapes.

GET only, one request at a time, 0.5 s apart, at most 420 requests. No R2 access, nothing written.
- agreements per year and branch (meta.count with limit=1), 2008-2026: the size of a full load;
- one agreement detail with its plans, one plan, one month (shapes of the deeper levels);
- provider dictionaries (/providers?year&branch, /providers/{year}?branch) counts and a sample;
- updatedAt filter and sort behaviour; the time of a full page of 25.
"""

import json
import time
import urllib.error
import urllib.parse
import urllib.request

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
APP = "https://api.nfz.gov.pl/app-umw-api"
BUDGET = [420]
BRANCHES = [f"{n:02d}" for n in range(1, 17)]


def get(url):
    if BUDGET[0] <= 0:
        return None, "budget used", 0.0
    BUDGET[0] -= 1
    time.sleep(0.5)
    t0 = time.monotonic()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=90) as r:
            return r.read(), f"{r.status}", time.monotonic() - t0
    except urllib.error.HTTPError as e:
        return e.read(800), f"HTTP {e.code}", time.monotonic() - t0
    except Exception as e:  # noqa: BLE001
        return None, f"{type(e).__name__}: {str(e)[:120]}", time.monotonic() - t0


def q(path, **params):
    params.setdefault("format", "json")
    params.setdefault("api-version", "1.2")
    return f"{APP}{path}?{urllib.parse.urlencode(params)}"


def js(url, show=0):
    body, st, secs = get(url)
    doc = None
    if body is not None:
        try:
            doc = json.loads(body)
        except ValueError:
            pass
    if show:
        print(f"\n=== GET {url}\n--- {st} {len(body or b'')} bytes {secs:.2f}s")
        print(json.dumps(doc, ensure_ascii=False)[:show] if doc is not None else (body or b"")[:400])
    return doc, st, secs, len(body or b"")


def count(doc):
    return (doc or {}).get("meta", {}).get("count") if isinstance(doc, dict) else None


def main():
    # Full page timing and the order of paging
    doc, st, secs, size = js(q("/agreements", year=2025, branch="07", page=1, limit=25), 600)
    print(f"FULL PAGE 25: {st} {secs:.2f}s {size} bytes")
    doc2, st, secs, size = js(q("/agreements", year=2025, branch="07", page=2, limit=25))
    print(f"PAGE 2: {st} {secs:.2f}s {size} bytes")
    agr = (doc or {}).get("data", {}).get("agreements") or []
    print("PAGE1 origin-codes", [a["attributes"]["origin-code"] for a in agr][:25])
    print("AMOUNTS", [a["attributes"]["amount"] for a in agr])
    print("SERVICE TYPES", [a["attributes"]["service-type"] for a in agr])
    print("UPDATED", [a["attributes"]["updated-at"] for a in agr][:10])
    # Detail of an agreement with an amount: plans, then one plan, then one month
    with_amount = [a for a in agr if a["attributes"]["amount"] is not None]
    if with_amount:
        detail, *_ = js(q(f"/agreements/{with_amount[0]['id']}", page=1, limit=25), 5000)
        plans = (detail or {}).get("data", {}).get("plans") or []
        if plans:
            pid = plans[0].get("id")
            plan, *_ = js(q(f"/plans/{pid}", page=1, limit=25), 4000)
            months = (plan or {}).get("data", {}).get("months") or []
            if months:
                js(q(f"/months/{months[0].get('id')}", page=1, limit=25), 3000)
        print("PLANS IN DETAIL", len(plans), "meta.count", count(detail))
    # updatedAt filter
    for date in ("2025-06-01", "2026-01-01", "2026-10-01"):
        doc, st, *_ = js(q("/agreements", year=2025, branch="07", updatedAt=date, page=1, limit=1))
        print(f"UPDATEDAT {date}: {st} count {count(doc)}")
    # sort
    doc, st, *_ = js(q("/agreements", year=2025, branch="07", sort="code", page=1, limit=3), 800)
    # Providers dictionaries
    for path, params in (
        ("/providers", {"year": 2025, "branch": "07"}),
        ("/providers/2025", {"branch": "07"}),
        ("/providers", {"year": 2025}),
        ("/providers/2025", {}),
    ):
        doc, st, secs, size = js(q(path, page=1, limit=25, **params), 1500)
        print(f"PROVIDERS {path} {params}: {st} count {count(doc)} {secs:.2f}s")
    doc, st, *_ = js(q("/providers/2025/070000001", branch="07", page=1, limit=25), 2500)
    # Agreements per year and branch
    sizes = {}
    t_start = time.monotonic()
    for year in range(2008, 2027):
        row = {}
        for b in BRANCHES:
            doc, st, *_ = js(q("/agreements", year=year, branch=b, page=1, limit=1))
            row[b] = count(doc) if st == "200" else st
        sizes[year] = row
        total = sum(v for v in row.values() if isinstance(v, int))
        print(f"YEAR {year}: total {total} pages {sum(-(-v // 25) for v in row.values() if isinstance(v, int))} {row}", flush=True)
    print(f"count requests took {time.monotonic() - t_start:.0f}s")
    # Service types and products per year (dictionary sizes)
    for year in (2008, 2015, 2026):
        for path in ("/service-types", "/contract-products"):
            doc, st, *_ = js(q(path, year=year, page=1, limit=1))
            print(f"DICT {path} {year}: {st} count {count(doc)}")
    print("\nrequests left", BUDGET[0])


if __name__ == "__main__":
    main()
