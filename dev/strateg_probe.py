"""Read-only probe of GUS STRATEG (register PLS-003): API documentation, API base, spec, sizes. No writes.

Sequential GET requests, one at a time, a 1.5 s pause (anonymous quota 5 req/s, 100 per 15 min is read from
the API page itself), bounded retries. Everything is printed to the log; a compact summary goes to annotations.
"""

from __future__ import annotations

import html
import json
import re
import socket
import time
from urllib.parse import urljoin, urlparse

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
S = requests.Session()
S.headers["User-Agent"] = UA
CALLS = {"n": 0}
SEEN: dict[str, dict] = {}


def get(url: str, accept: str = "*/*") -> requests.Response | None:
    if CALLS["n"] >= 85:  # stay below the anonymous 100 per 15 minutes
        print("budget reached, skip", url, flush=True)
        return None
    for attempt in range(3):
        time.sleep(1.5)
        CALLS["n"] += 1
        try:
            r = S.get(url, timeout=(15, 90), headers={"Accept": accept})
            print(f"GET {url} -> {r.status_code} {r.headers.get('Content-Type')} {len(r.content)}B", flush=True)
            if r.status_code < 500:
                return r
        except requests.RequestException as exc:
            print("error", url, repr(exc)[:300], flush=True)
            if isinstance(exc, requests.ConnectionError) and "Name or service" in repr(exc):
                return None
        time.sleep(5 * (attempt + 1))
    return None


def text_of(body: str) -> str:
    body = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", body)
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", body))).strip()


def note(url: str, r: requests.Response | None, keep: int = 600) -> dict:
    if r is None:
        out = {"status": None}
    else:
        out = {"status": r.status_code, "type": (r.headers.get("Content-Type") or "")[:40], "bytes": len(r.content),
               "head": r.text[:keep]}
    SEEN[url] = out
    return out


def main() -> int:
    for host in ("strateg.stat.gov.pl", "api-strateg.stat.gov.pl", "api.stat.gov.pl", "bdl.stat.gov.pl"):
        try:
            print("dns", host, socket.gethostbyname_ex(host), flush=True)
        except OSError as exc:
            print("dns", host, "fails", exc, flush=True)

    # 1. The API page on api.stat.gov.pl: full text and every link.
    links: set[str] = set()
    for page in ("https://api.stat.gov.pl/Home/StrategApi?lang=en", "https://api.stat.gov.pl/Home/StrategApi"):
        r = get(page)
        if r is None:
            continue
        body = r.text
        txt = text_of(body)
        print("PAGE TEXT", page, "\n", txt[:12000], flush=True)
        found = set(re.findall(r'(?:href|src)="([^"#]+)"', body)) | set(re.findall(r"https?://[^\s\"'<>)]+", body))
        links |= {urljoin(page, h) for h in found}
        start = max(txt.lower().find("strateg"), 0)
        annotate("notice", f"api page text {page[-7:]}", txt[start:start + 3800])
    rel = sorted(u for u in links if "strateg" in u.lower() or "swagger" in u.lower() or "apidoc" in u.lower())
    print("LINKS", json.dumps(sorted(links), indent=0), flush=True)
    annotate("notice", "api page strateg links", rel[:80])

    # 2. strateg.stat.gov.pl: robots, home page scripts, apidocs and its spec.
    robots = get("https://strateg.stat.gov.pl/robots.txt")
    note("robots", robots, 1500)
    home = get("https://strateg.stat.gov.pl/")
    api_strings: set[str] = set()
    if home is not None:
        scripts = [urljoin("https://strateg.stat.gov.pl/", s) for s in re.findall(r'<script[^>]+src="([^"]+)"', home.text)]
        print("home scripts", scripts, flush=True)
        api_strings |= set(re.findall(r"""["'](/?api[^"'\s]{0,120})["']""", home.text))
        for js in scripts[:8]:
            r = get(js)
            if r is not None and r.status_code == 200:
                api_strings |= set(re.findall(r"""["'`](/?(?:api|Api|API)[^"'`\s]{0,120})["'`]""", r.text))
                api_strings |= {u for u in re.findall(r"https?://[A-Za-z0-9.\-]+(?:/[^\s\"'`<>]*)?", r.text) if "stat.gov.pl" in u}
    print("API STRINGS", json.dumps(sorted(api_strings), indent=0), flush=True)
    annotate("notice", "strateg home api strings", sorted(api_strings)[:120])

    docs = get("https://strateg.stat.gov.pl/apidocs/?lang=en")
    spec_urls = set()
    if docs is not None:
        print("APIDOCS HTML\n", docs.text[:6000], flush=True)
        spec_urls |= {urljoin("https://strateg.stat.gov.pl/apidocs/", u) for u in re.findall(r"""["']([^"']*\.json[^"']*)["']""", docs.text)}
        for js in re.findall(r'<script[^>]+src="([^"]+)"', docs.text):
            if "flasgger" in js or "swagger" in js.lower():
                continue
            r = get(urljoin("https://strateg.stat.gov.pl/apidocs/", js))
            if r is not None:
                spec_urls |= {urljoin("https://strateg.stat.gov.pl/", u) for u in re.findall(r"""["']([^"']*\.json[^"']*)["']""", r.text)}
    for cand in ("https://strateg.stat.gov.pl/apispec_1.json", "https://strateg.stat.gov.pl/apidocs/apispec_1.json",
                 "https://strateg.stat.gov.pl/api/apispec_1.json", "https://strateg.stat.gov.pl/swagger.json",
                 "https://strateg.stat.gov.pl/api/swagger.json", "https://api-strateg.stat.gov.pl/apispec_1.json"):
        spec_urls.add(cand)
    print("SPEC CANDIDATES", sorted(spec_urls), flush=True)
    spec = None
    for u in sorted(spec_urls):
        r = get(u, "application/json")
        note(u, r, 300)
        if r is not None and r.status_code == 200:
            try:
                parsed = r.json()
            except ValueError:
                continue
            if isinstance(parsed, dict) and "paths" in parsed:
                spec = (u, parsed)
                break
    if spec:
        u, parsed = spec
        print("SPEC", u, json.dumps(parsed, ensure_ascii=False)[:60000], flush=True)
        paths = {}
        for p, ops in parsed["paths"].items():
            for verb, op in ops.items():
                params = [f"{x.get('name')}:{x.get('in')}{'*' if x.get('required') else ''}" for x in op.get("parameters", []) if isinstance(x, dict)]
                paths[f"{verb.upper()} {p}"] = params
        info = {k: parsed.get(k) for k in ("host", "basePath", "schemes", "servers", "info")}
        annotate("notice", f"strateg spec {u}", {"info": info, "n_paths": len(paths)})
        blob = json.dumps(paths, ensure_ascii=False, separators=(",", ":"))
        for i in range(0, min(len(blob), 3800 * 3), 3800):
            annotate("notice", f"strateg spec paths {i // 3800}", blob[i:i + 3800])
    # 3. Candidate API bases (whatever answers is printed; small answers only).
    bases = set()
    for s in api_strings:
        if s.startswith("http"):
            p = urlparse(s)
            bases.add(f"{p.scheme}://{p.netloc}{p.path}")
        else:
            bases.add(urljoin("https://strateg.stat.gov.pl/", s))
    tried = 0
    for u in sorted(bases):
        if "{" in u or tried >= 25:
            continue
        tried += 1
        note(u, get(u, "application/json"), 500)
    # 4. Open-data portal mirror.
    r = get("https://api.dane.gov.pl/1.4/search?q=strateg&per_page=20")
    if r is not None and r.status_code == 200:
        try:
            hits = [{"t": d["attributes"].get("title"), "m": d["attributes"].get("model"), "u": d.get("links", {}).get("self")}
                    for d in r.json().get("data", [])]
        except (ValueError, KeyError, AttributeError):
            hits = r.text[:1500]
        annotate("notice", "dane.gov.pl search strateg", hits)
    annotate("notice", "strateg probe responses", {k: {kk: (vv[:160] if isinstance(vv, str) else vv) for kk, vv in v.items()} for k, v in SEEN.items()})
    print("ALL", json.dumps(SEEN, ensure_ascii=False, indent=1)[:40000], flush=True)
    print("calls", CALLS["n"], flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
