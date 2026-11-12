"""Read-only probe of the NFZ contracts API (PLS-014, Task #2955).

GET only, one request at a time, 0.6 s apart, at most 140 requests; robots.txt read first and honoured.
Prints the API description (paths, parameters, schemas) and the shape and size of answers to the job log.
No R2 access, nothing written.
"""

import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import urllib.robotparser

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
BASE = "https://api.nfz.gov.pl"
APP = BASE + "/app-umw-api"
BUDGET = [140]
RP = urllib.robotparser.RobotFileParser()


def get(url, n=8_000_000):
    if BUDGET[0] <= 0:
        return None, "budget used", 0.0
    BUDGET[0] -= 1
    if not RP.can_fetch(UA, url):
        return None, "robots disallow", 0.0
    time.sleep(0.6)
    t0 = time.monotonic()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=90) as r:
            body = r.read(n)
            hdr = {
                k: v[:80]
                for k, v in r.headers.items()
                if k.lower() in ("content-type", "last-modified", "etag", "retry-after", "content-length")
                or "ratelimit" in k.lower()
            }
            return body, f"{r.status} {hdr}", time.monotonic() - t0
    except urllib.error.HTTPError as e:
        return e.read(800), f"HTTP {e.code}", time.monotonic() - t0
    except Exception as e:  # noqa: BLE001
        return None, f"{type(e).__name__}: {str(e)[:120]}", time.monotonic() - t0


def show(url, head=2500):
    body, st, secs = get(url)
    print(f"\n=== GET {url}\n--- {st} {len(body or b'')} bytes {secs:.1f}s", flush=True)
    if body is None:
        return None
    text = body.decode("utf-8", "replace")
    try:
        doc = json.loads(text)
    except ValueError:
        print(text[:head])
        return text
    print(json.dumps(doc, ensure_ascii=False)[:head])
    return doc


def swagger():
    page = show(APP + "/", 6000)
    cands = []
    if isinstance(page, str):
        cands += re.findall(r'["\']([^"\']*swagger[^"\']*\.json)["\']', page)
        for js in re.findall(r'src=["\']([^"\']+\.js)["\']', page)[:3]:
            jsurl = urllib.parse.urljoin(APP + "/", js)
            body, st, _ = get(jsurl)
            print(f"\n=== JS {jsurl}: {st}")
            if body:
                cands += re.findall(r'["\']([^"\']*swagger[^"\']*\.json)["\']', body.decode("utf-8", "replace"))
    cands += [
        "swagger/v1.2/swagger.json",
        "swagger/v1/swagger.json",
        "swagger/v1.1/swagger.json",
        "swagger/docs/v1",
    ]
    seen = set()
    for c in cands:
        url = urllib.parse.urljoin(APP + "/", c)
        if url in seen:
            continue
        seen.add(url)
        body, st, _ = get(url)
        print(f"\n=== SWAGGER {url}: {st}", flush=True)
        if body is None or not st.startswith("200"):
            continue
        try:
            doc = json.loads(body)
        except ValueError:
            print(body[:300])
            continue
        print("info", json.dumps(doc.get("info"), ensure_ascii=False)[:800])
        for p, ops in (doc.get("paths") or {}).items():
            for verb, op in ops.items():
                params = []
                for q in op.get("parameters", []):
                    s = q.get("schema") or q
                    params.append(
                        f"{q.get('name')}[{q.get('in')}{'*' if q.get('required') else ''}:{s.get('type')}"
                        f"{'=' + str(s.get('enum')) if s.get('enum') else ''}"
                        f"{' max ' + str(s.get('maximum')) if s.get('maximum') is not None else ''}"
                        f"{' def ' + str(s.get('default')) if s.get('default') is not None else ''}]"
                    )
                print(f"PATH {verb.upper()} {p} | {(op.get('summary') or '')[:160]} | {' '.join(params)}")
                for q in op.get("parameters", []):
                    if q.get("description"):
                        print(f"   - {q.get('name')}: {q['description'][:300]}")
        defs = doc.get("definitions") or (doc.get("components") or {}).get("schemas") or {}
        for name, d in defs.items():
            props = {k: (v.get("type") or v.get("$ref", "")[-40:]) for k, v in (d.get("properties") or {}).items()}
            print(f"SCHEMA {name}: {json.dumps(props, ensure_ascii=False)[:900]}")
        return doc
    return None


def q(path, **params):
    params.setdefault("format", "json")
    params.setdefault("api-version", "1.2")
    return f"{APP}{path}?{urllib.parse.urlencode(params)}"


def main():
    try:
        req = urllib.request.Request(BASE + "/robots.txt", headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=30) as r:
            txt = r.read(20000).decode("utf-8", "replace")
        print("=== robots.txt\n" + txt[:1500])
        RP.parse(txt.splitlines() if "<html" not in txt[:300].lower() else [])
    except urllib.error.HTTPError as e:
        print(f"=== robots.txt HTTP {e.code}")
        if e.code in (401, 403):
            print("refused: stop")
            return
        RP.parse([])
    except Exception as e:  # noqa: BLE001
        print(f"=== robots.txt {type(e).__name__}: {e}")
        return

    doc = swagger()
    show(q("/version"), 600)
    show(APP + "/terms", 3000)
    if not doc:
        print("no swagger found")
    # Parameter-free and year/branch list endpoints: first page of each
    ids = {}
    if doc:
        for p, ops in (doc.get("paths") or {}).items():
            if "get" not in ops or "{" in p:
                continue
            params = {}
            ok = True
            for prm in ops["get"].get("parameters", []):
                name = prm.get("name")
                if not prm.get("required") or name in ("api-version", "format", "page", "limit"):
                    continue
                if name == "year":
                    params["year"] = 2024
                elif name == "branch":
                    params["branch"] = "07"
                else:
                    ok = False
            if not ok:
                print(f"\nSKIP {p}: required params")
                continue
            d = show(q(p, page=1, limit=25, **params), 3000)
            if isinstance(d, dict):
                print("META", json.dumps(d.get("meta"), ensure_ascii=False)[:400])
                print("LINKS", json.dumps(d.get("links"), ensure_ascii=False)[:400])
                data = d.get("data")
                if isinstance(data, dict):
                    data = data.get("agreements") or data.get("data") or [data]
                if isinstance(data, list) and data:
                    ids[p] = data[0].get("id") if isinstance(data[0], dict) else None
                    print("FIRST", json.dumps(data[0], ensure_ascii=False)[:1500])
    print("\nIDS", ids)
    # Detail endpoints with one path parameter, filled from a list id of the same resource
    if doc:
        for p, ops in (doc.get("paths") or {}).items():
            if "get" not in ops or p.count("{") != 1:
                continue
            base = p.split("/{")[0]
            sample = ids.get(base)
            if not sample:
                print(f"\nNO SAMPLE for {p}")
                continue
            path = re.sub(r"\{[^}]+\}", urllib.parse.quote(str(sample), safe=""), p)
            d = show(q(path, page=1, limit=25), 3000)
    # Size: agreements per year for Poland and for one branch; page-size limit
    for limit in (25, 100, 1000):
        d = show(q("/agreements", year=2024, branch="07", page=1, limit=limit), 300)
        if isinstance(d, dict):
            data = d.get("data")
            if isinstance(data, dict):
                data = data.get("agreements")
            print(f"LIMIT {limit}: items {len(data) if isinstance(data, list) else data!r} meta {d.get('meta')}")
    counts = {}
    for year in range(2008, 2028):
        d = show(q("/agreements", year=year, page=1, limit=1), 200)
        if isinstance(d, dict):
            counts[year] = (d.get("meta") or {}).get("count")
    print("\nAGREEMENTS PER YEAR (all branches)", counts)
    counts = {}
    for branch in ("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16"):
        d = show(q("/agreements", year=2025, branch=branch, page=1, limit=1), 200)
        if isinstance(d, dict):
            counts[branch] = (d.get("meta") or {}).get("count")
    print("\nAGREEMENTS 2025 PER BRANCH", counts)
    print("\nrequests left", BUDGET[0])


if __name__ == "__main__":
    main()
