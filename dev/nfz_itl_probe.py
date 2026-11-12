"""Read-only probe of the NFZ waiting-times API v1.4 (PLS-015, Task #2956).

GET only, one request at a time, at least 1.2 s apart (terms: max 10 requests/second per IP), at most 120
requests; robots.txt read first and honoured. Writes nothing to R2. Prints to the job log and saves the swagger,
the HTML of the documentation pages and one sample page to DEV_OUTPUT_DIR (run artifact).
"""

import json
import os
import re
import time
import urllib.error
import urllib.request
import urllib.robotparser
from pathlib import Path

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
BASE = "https://apinfz.nfz.gov.pl/app-itl-api-pcus"
OLD = "https://api.nfz.gov.pl/app-itl-api"
OUT = Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output"))
BUDGET = [120]
RP = {}


def robots(host):
    if host in RP:
        return RP[host]
    rp = urllib.robotparser.RobotFileParser()
    try:
        req = urllib.request.Request(host + "/robots.txt", headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=30) as r:
            txt = r.read(20000).decode("utf-8", "replace")
        print(f"=== {host}/robots.txt\n{txt[:800]}", flush=True)
        rp.parse([] if "<html" in txt[:300].lower() else txt.splitlines())
    except urllib.error.HTTPError as e:
        print(f"=== {host}/robots.txt HTTP {e.code}", flush=True)
        rp.parse(["User-agent: *", "Disallow: /"] if e.code in (401, 403) else [])
    except Exception as e:  # noqa: BLE001
        print(f"=== {host}/robots.txt {type(e).__name__}: {e}", flush=True)
        rp.parse([])
    RP[host] = rp
    return rp


def get(url, n=60_000_000):
    if BUDGET[0] <= 0:
        return None, "budget used", 0.0
    BUDGET[0] -= 1
    host = "/".join(url.split("/")[:3])
    if not robots(host).can_fetch(UA, url):
        return None, "robots disallow", 0.0
    time.sleep(1.2)
    t = time.monotonic()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json, text/html;q=0.8"})
        with urllib.request.urlopen(req, timeout=90) as r:
            body = r.read(n)
            hdr = {k: v[:100] for k, v in r.headers.items() if k.lower() in ("content-type", "last-modified", "etag", "retry-after", "content-length", "content-encoding") or "ratelimit" in k.lower()}
            return body, f"{r.status} {hdr}", time.monotonic() - t
    except urllib.error.HTTPError as e:
        return e.read(800), f"HTTP {e.code}", time.monotonic() - t
    except Exception as e:  # noqa: BLE001
        return None, f"{type(e).__name__}: {str(e)[:160]}", time.monotonic() - t


def show(url, head=1500, save=None):
    body, st, dt = get(url)
    print(f"\n=== GET {url}\n--- {st} {len(body or b'')} bytes {dt:.2f}s", flush=True)
    if body is None:
        return None, st
    if save:
        (OUT / save).write_bytes(body)
    text = body.decode("utf-8", "replace")
    try:
        return json.loads(text), st
    except ValueError:
        plain = re.sub(r"<script.*?</script>|<style.*?</style>", " ", text, flags=re.S)
        plain = re.sub(r"<[^>]+>", " ", plain)
        plain = re.sub(r"\s+", " ", plain)
        print("TEXT:", plain[:head])
        links = sorted(set(re.findall(r'href="([^"]+)"', text)))
        print("LINKS:", links[:60])
        return None, st


def swagger(doc):
    print("info", json.dumps(doc.get("info"), ensure_ascii=False)[:2000])
    for p, ops in (doc.get("paths") or {}).items():
        for verb, op in ops.items():
            print(f"PATH {verb.upper()} {p} | {(op.get('summary') or '')[:200]} | {(op.get('description') or '')[:400]}")
            for q in op.get("parameters", []):
                s = q.get("schema") or q
                print(f"   - {q.get('name')} in={q.get('in')} req={q.get('required')} type={s.get('type')} enum={s.get('enum')} min={s.get('minimum')} max={s.get('maximum')} default={s.get('default')} | {(q.get('description') or '')[:400]}")
    defs = doc.get("definitions") or (doc.get("components") or {}).get("schemas") or {}
    for name, d in defs.items():
        props = {k: (v.get("type") or v.get("$ref", "")[-40:]) + (f"/{v.get('format')}" if v.get("format") else "") for k, v in (d.get("properties") or {}).items()}
        print(f"SCHEMA {name}: {json.dumps(props, ensure_ascii=False)[:1500]}")


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    # documentation and terms of the exact v1.4 site
    for path in ("", "/", "/terms", "/regulamin", "/index.html", "/swagger", "/swagger/index.html"):
        show(BASE + path, 3500, save=f"page_{re.sub('[^a-z]', '_', path) or 'root'}.html")
    doc, st = show(BASE + "/swagger/v1.4/swagger.json", 200, save="swagger_v1_4.json")
    if doc:
        swagger(doc)
    # old site terms (v1.3) for comparison
    show(OLD + "/terms", 6000, save="old_terms.html")
    show(OLD + "/", 1500, save="old_root.html")
    # dictionaries
    for path in ("/provinces", "/benefits?page=1&limit=25", "/localities?page=1&limit=25&province=07"):
        sep = "&" if "?" in path else "?"
        d, st = show(f"{BASE}{path}{sep}format=json&api-version=1.4", 2500)
        if d:
            print("DICT", path, json.dumps(d, ensure_ascii=False)[:2500])
    # queues: page size limit, shape, and counts per province x case
    for lim in (25, 100, 1000):
        d, st = show(f"{BASE}/queues?case=1&province=07&page=1&limit={lim}&format=json&api-version=1.4", 300,
                     save=f"queues_07_1_limit{lim}.json")
        if d:
            print("META", lim, json.dumps(d.get("meta"), ensure_ascii=False), "LINKS", json.dumps(d.get("links"), ensure_ascii=False),
                  "n", len(d.get("data") or []))
            if lim == 25:
                print("RECORD0", json.dumps((d.get("data") or [None])[0], ensure_ascii=False)[:4000])
    d, st = show(f"{BASE}/queues?case=1&page=1&limit=25&format=json&api-version=1.4", 300)
    if d:
        print("META no-province", json.dumps(d.get("meta"), ensure_ascii=False))
    counts = {}
    for case in (1, 2):
        for prov in range(1, 17):
            d, st = show(f"{BASE}/queues?case={case}&province={prov:02d}&page=1&limit=1&format=json&api-version=1.4", 200)
            counts[f"{case}/{prov:02d}"] = (d or {}).get("meta", {}).get("count") if d else st
    print("COUNTS", json.dumps(counts))
    total = sum(v for v in counts.values() if isinstance(v, int))
    print("TOTAL_RECORDS", total)
    print("BUDGET_LEFT", BUDGET[0])


if __name__ == "__main__":
    main()
