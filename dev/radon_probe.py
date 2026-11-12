"""Read-only probe of RAD-on (PLS-023): OpenAPI spec, sample non-person resources, terms pages.

One request at a time, 3 s pause, identifying user agent, bounded retries. Writes nothing to R2; the
spec and samples go to DEV_OUTPUT_DIR (the run's artifact). Values of person-like keys are redacted.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import time

import requests

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
BASE = "https://radon.nauka.gov.pl"
OUT = Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output"))
OUT.mkdir(parents=True, exist_ok=True)
S = requests.Session()
S.headers.update({"User-Agent": UA})
PERSON = re.compile(r"(name|surname|first|last|person|rector|email|phone|pesel|orcid|fax|www)", re.I)


def get(url, **kw):
    for attempt in range(3):
        time.sleep(3)
        try:
            r = S.get(url, timeout=60, **kw)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(10 * (attempt + 1))
                continue
            return r
        except requests.RequestException as exc:
            print(f"  error {type(exc).__name__}: {exc}"[:300])
            time.sleep(10 * (attempt + 1))
    return None


def show(url, n=1500):
    r = get(url)
    if r is None:
        print(f"=== {url}: no answer")
        return None
    print(f"=== {url}: {r.status_code} {r.headers.get('Content-Type')} len={len(r.content)} "
          f"lm={r.headers.get('Last-Modified')} rl={ {k: v for k, v in r.headers.items() if 'rate' in k.lower() or 'limit' in k.lower()} }")
    print(r.text[:n])
    return r


def shape(obj, path="", out=None, depth=0):
    out = out if out is not None else {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            shape(v, f"{path}.{k}", out, depth + 1)
    elif isinstance(obj, list):
        out.setdefault(f"{path}[]", f"list len={len(obj)}")
        for v in obj[:3]:
            shape(v, f"{path}[]", out, depth + 1)
    else:
        key = path.rsplit(".", 1)[-1]
        val = "<redacted>" if PERSON.search(key) and isinstance(obj, str) else repr(obj)[:80]
        out.setdefault(path, val)
    return out


def main():
    show(f"{BASE}/swagger-ui/index.html", 1200)
    show(f"{BASE}/swagger-ui/swagger-initializer.js", 2000)
    spec = None
    for p in ["/v3/api-docs/swagger-config", "/v3/api-docs", "/v2/api-docs", "/api-docs", "/swagger.json",
              "/opendata/v3/api-docs", "/api/v3/api-docs"]:
        r = show(f"{BASE}{p}", 800)
        if r is not None and r.status_code == 200 and r.text.lstrip().startswith("{"):
            try:
                body = r.json()
            except ValueError:
                continue
            name = p.strip("/").replace("/", "_")
            (OUT / f"{name}.json").write_text(json.dumps(body, indent=1, ensure_ascii=False))
            if "urls" in body or "url" in body:
                for u in body.get("urls") or [{"url": body.get("url")}]:
                    if u.get("url"):
                        r2 = show(BASE + u["url"] if u["url"].startswith("/") else u["url"], 300)
                        if r2 is not None and r2.status_code == 200:
                            try:
                                sp = r2.json()
                                fn = re.sub(r"\W+", "_", u.get("name") or u["url"])
                                (OUT / f"spec_{fn}.json").write_text(json.dumps(sp, indent=1, ensure_ascii=False))
                                spec = spec or sp
                                print(f"--- spec {u}: {len(sp.get('paths', {}))} paths")
                                for path, ops in sp.get("paths", {}).items():
                                    for m, op in ops.items():
                                        params = [x.get("name") for x in op.get("parameters", []) if isinstance(x, dict)]
                                        print(f"  {m.upper()} {path} :: {op.get('summary','')[:90]} :: {params}")
                            except ValueError:
                                pass
            if "paths" in body:
                spec = spec or body
                print(f"--- spec {p}: {len(body['paths'])} paths; servers={body.get('servers')}; info={json.dumps(body.get('info'), ensure_ascii=False)[:800]}")
                for path, ops in body["paths"].items():
                    for m, op in ops.items():
                        params = [x.get("name") for x in op.get("parameters", []) if isinstance(x, dict)]
                        print(f"  {m.upper()} {path} :: {op.get('summary','')[:90]} :: {params}")
    # sample calls on non-person resources
    for res in ["institutions", "courses", "branches", "doctoralSchools", "projects", "investments", "patents",
                "disciplines", "evaluations", "students", "graduates", "privateUniversities"]:
        for url in [f"{BASE}/opendata/polon/{res}?resultNumbers=2"]:
            r = get(url)
            if r is None:
                print(f"=== {url}: no answer")
                continue
            print(f"=== {url}: {r.status_code} {r.headers.get('Content-Type')} len={len(r.content)}")
            if r.status_code == 200:
                try:
                    body = r.json()
                except ValueError:
                    print(r.text[:300])
                    continue
                if isinstance(body, dict):
                    print("  top keys:", list(body.keys()), "pagination:", json.dumps(body.get("pagination"))[:300])
                for k, v in shape(body).items():
                    print(f"  {k} = {v}")
            else:
                print(r.text[:300])
    # terms
    for url in [f"{BASE}/pomoc/", f"{BASE}/pomoc/tag/api/", f"{BASE}/regulamin", f"{BASE}/pomoc/regulamin/",
                f"{BASE}/pomoc/?s=licencja", f"{BASE}/pomoc/?s=regulamin", f"{BASE}/pomoc/?s=limit"]:
        r = get(url)
        if r is None:
            continue
        text = re.sub(r"<[^>]+>", " ", r.text)
        text = re.sub(r"\s+", " ", text)
        print(f"=== TERMS {url}: {r.status_code} len={len(r.text)}")
        for m in re.finditer(r"[^.]{0,200}(licencj|licenc|CC BY|Creative|ponown|warunk|regulamin|limit|zapyta[ńn]|token|ograniczen)[^.]{0,250}", text, re.I):
            print("  >>", m.group(0)[:450])
        links = sorted(set(re.findall(r'href="([^"]+)"', r.text)))
        for l in links:
            if re.search(r"regulamin|licenc|warunk|zasad|api|dane|otwart|ponown|token|limit", l, re.I):
                print("  link:", l)


if __name__ == "__main__":
    main()
