"""Read-only probe of the two NFZ statistics APIs (PLS-012, PLS-013), Tasks #2929 and #2930.

GET only, one request at a time, one second apart, at most 60 requests; robots.txt read first and honoured.
Prints the API description (paths and parameters) and the shape of a few answers to the job log.
"""

import json
import time
import urllib.error
import urllib.request
import urllib.robotparser

UA = "zohelo-data/1.0 (+https://zohelo.com)"
BASE = "https://api.nfz.gov.pl"
BUDGET = [60]
RP = urllib.robotparser.RobotFileParser()


def get(url, n=4_000_000):
    if BUDGET[0] <= 0:
        return None, "budget used"
    BUDGET[0] -= 1
    if not RP.can_fetch(UA, url):
        return None, "robots disallow"
    time.sleep(1.0)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=60) as r:
            body = r.read(n)
            hdr = {k: v[:80] for k, v in r.headers.items() if k.lower() in ("content-type", "last-modified", "etag", "retry-after", "content-length") or "ratelimit" in k.lower()}
            return body, f"{r.status} {hdr}"
    except urllib.error.HTTPError as e:
        return e.read(600), f"HTTP {e.code}"
    except Exception as e:  # noqa: BLE001
        return None, f"{type(e).__name__}: {str(e)[:120]}"


def show(url, head=2500):
    body, st = get(url)
    print(f"\n=== GET {url}\n--- {st}", flush=True)
    if body is None:
        return None
    text = body.decode("utf-8", "replace")
    try:
        doc = json.loads(text)
    except ValueError:
        print(text[:head])
        return None
    print(json.dumps(doc, ensure_ascii=False)[:head])
    return doc


def swagger(app):
    for path in ("swagger/v1/swagger.json", "swagger/v1.1/swagger.json", "swagger/v1.0/swagger.json", "swagger/docs/v1"):
        body, st = get(f"{BASE}/{app}/{path}")
        print(f"\n=== SWAGGER {app}/{path}: {st}", flush=True)
        if body is None or not st.startswith("200"):
            continue
        try:
            doc = json.loads(body)
        except ValueError:
            print(body[:300])
            continue
        print("info", json.dumps(doc.get("info"), ensure_ascii=False)[:600])
        for p, ops in (doc.get("paths") or {}).items():
            for verb, op in ops.items():
                params = [f"{q.get('name')}[{q.get('in')}{'*' if q.get('required') else ''}:{(q.get('schema') or q).get('type')}{'=' + str((q.get('schema') or q).get('enum')) if (q.get('schema') or q).get('enum') else ''}]" for q in op.get("parameters", [])]
                print(f"PATH {verb.upper()} {p} | {(op.get('summary') or '')[:120]} | {' '.join(params)}")
                for q in op.get("parameters", []):
                    if q.get("description"):
                        print(f"   - {q.get('name')}: {q['description'][:300]}")
        defs = doc.get("definitions") or (doc.get("components") or {}).get("schemas") or {}
        for name, d in defs.items():
            props = {k: (v.get("type") or v.get("$ref", "")[-40:]) for k, v in (d.get("properties") or {}).items()}
            print(f"SCHEMA {name}: {json.dumps(props, ensure_ascii=False)[:700]}")
        return doc
    return None


def main():
    body, st = get(BASE + "/robots.txt") if False else (None, None)
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

    # --- services (JGP)
    j = "app-stat-api-jgp"
    swagger(j)
    show(f"{BASE}/{j}/version?format=json&api-version=1.1")
    cats = {}
    for cat in ("1a", "1b", "1c", "1d", "1w", "2", "3", "1e"):
        d = show(f"{BASE}/{j}/benefits?catalog={cat}&page=1&limit=25&format=json&api-version=1.1", 900)
        if d:
            cats[cat] = (d.get("meta") or {}).get("count")
    print("\nBENEFIT COUNTS", cats)
    d = show(f"{BASE}/{j}/benefits?page=1&limit=25&format=json&api-version=1.1", 1500)
    first = None
    try:
        first = d["data"][0]
    except Exception:  # noqa: BLE001
        pass
    print("first benefit", first)
    for cat, name in (("1a", "N01"), ("1a", "A01")):
        idx = show(f"{BASE}/{j}/index-of-tables?catalog={cat}&name={name}&format=json&api-version=1.1", 6000)
        try:
            years = idx["data"]["attributes"]["years"]
            print("years", [(y.get("year"), len(y.get("tables") or [])) for y in years])
            last = years[-1] if years[-1].get("year", 0) >= years[0].get("year", 0) else years[0]
            for t in (last.get("tables") or [])[:12]:
                print("TABLE", json.dumps(t, ensure_ascii=False)[:300])
                link = (t.get("links") or {}).get("related")
                if link and name == "N01":
                    sep = "&" if "?" in link else "?"
                    show(link + f"{sep}page=1&limit=25&format=json&api-version=1.1" if "limit" not in link else link, 2200)
        except Exception as e:  # noqa: BLE001
            print("index parse", type(e).__name__, e)

    # --- pharmacy refunds
    ra = "app-stat-api-ra"
    doc = swagger(ra)
    show(f"{BASE}/{ra}/version?format=json&api-version=1.0")
    if doc:
        n = 0
        for p, ops in (doc.get("paths") or {}).items():
            if "{" in p or n >= 8 or "get" not in ops:
                continue
            if any(q.get("required") and q.get("name") not in ("api-version", "format", "page", "limit") for q in ops["get"].get("parameters", [])):
                continue
            n += 1
            path = p if p.startswith("/") else "/" + p
            show(f"{BASE}/{ra}{path}?page=1&limit=25&format=json&api-version=1.0", 2500)
    print("\nrequests left", BUDGET[0])


if __name__ == "__main__":
    main()
