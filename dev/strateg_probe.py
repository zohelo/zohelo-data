"""Read-only probe of GUS STRATEG (register PLS-003): the OpenAPI spec and one call per list endpoint. No writes.

Sequential GET requests, one at a time, a 1.5 s pause (anonymous quota 5 req/s and 100 per 15 min, read from
https://api.stat.gov.pl/Home/StrategApi), at most 80 calls, bounded retries. Compact summary to annotations.
Args: optional extra URLs to GET (relative to the API base found in the spec, or absolute).
"""

from __future__ import annotations

import json
import socket
import sys
import time
from urllib.parse import urljoin

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
S = requests.Session()
S.headers["User-Agent"] = UA
CALLS = {"n": 0}
SITE = "https://strateg.stat.gov.pl/"


def get(url: str, accept: str = "application/json") -> requests.Response | None:
    if CALLS["n"] >= 80:
        print("budget reached, skip", url, flush=True)
        return None
    for attempt in range(3):
        time.sleep(1.5)
        CALLS["n"] += 1
        try:
            r = S.get(url, timeout=(15, 120), headers={"Accept": accept})
            print(f"GET {url} -> {r.status_code} {r.headers.get('Content-Type')} {len(r.content)}B "
                  f"{ {k: v for k, v in r.headers.items() if 'limit' in k.lower() or k.lower() in ('last-modified', 'etag')} }",
                  flush=True)
            if r.status_code == 429:
                time.sleep(30)
                continue
            if r.status_code < 500:
                return r
        except requests.RequestException as exc:
            print("error", url, repr(exc)[:300], flush=True)
            if "Name or service" in repr(exc) or "NameResolution" in repr(exc):
                return None
        time.sleep(5 * (attempt + 1))
    return None


def shape(obj, depth=0):
    """A compact description of a JSON value: keys, list lengths, first element."""
    if depth > 4:
        return "..."
    if isinstance(obj, dict):
        return {k: shape(v, depth + 1) for k, v in list(obj.items())[:25]}
    if isinstance(obj, list):
        return [f"len={len(obj)}", shape(obj[0], depth + 1)] if obj else ["len=0"]
    if isinstance(obj, str):
        return obj[:60]
    return obj


def main(extra: list[str]) -> int:
    for host in ("strateg.stat.gov.pl", "api-strateg.stat.gov.pl"):
        try:
            print("dns", host, socket.gethostbyname_ex(host), flush=True)
        except OSError as exc:
            print("dns", host, "fails", exc, flush=True)
    spec = None
    for name in ("strateg_en.json", "strateg_pl.json"):
        r = get(urljoin(SITE, name))
        if r is not None and r.status_code == 200:
            try:
                parsed = r.json()
            except ValueError:
                print("not json", r.text[:500], flush=True)
                continue
            print("SPEC", name, json.dumps(parsed, ensure_ascii=False)[:80000], flush=True)
            if spec is None:
                spec = parsed
    if spec is None:
        annotate("error", "strateg spec", "no spec")
        return 1
    info = {k: spec.get(k) for k in ("host", "basePath", "schemes", "servers", "info", "securityDefinitions",
                                      "components")}
    info["components"] = list((spec.get("components") or {}).get("securitySchemes", {}) or [])
    annotate("notice", "strateg spec info", info)
    paths = {}
    for p, ops in spec.get("paths", {}).items():
        for verb, op in ops.items():
            if not isinstance(op, dict):
                continue
            params = [f"{x.get('name')}:{x.get('in')}{'*' if x.get('required') else ''}" for x in op.get("parameters", [])
                      if isinstance(x, dict)]
            paths[f"{verb.upper()} {p}"] = {"s": (op.get("summary") or "")[:70], "p": params}
    blob = json.dumps(paths, ensure_ascii=False, separators=(",", ":"))
    for i in range(0, min(len(blob), 3800 * 4), 3800):
        annotate("notice", f"strateg spec paths {i // 3800}", blob[i:i + 3800])

    if spec.get("servers"):
        base = spec["servers"][0]["url"]
    else:
        base = f"{(spec.get('schemes') or ['https'])[0]}://{spec.get('host') or 'strateg.stat.gov.pl'}{spec.get('basePath') or '/'}"
    base = urljoin(SITE, base)
    if not base.endswith("/"):
        base += "/"
    print("BASE", base, flush=True)
    results = {}
    # One call per GET path without required path parameters, plus extra URLs from the arguments.
    targets = [p for p in spec.get("paths", {}) if "{" not in p and "get" in spec["paths"][p]]
    targets = [urljoin(base, p.lstrip("/")) for p in targets][:40] + [u if u.startswith("http") else urljoin(base, u.lstrip("/")) for u in extra]
    for u in targets:
        r = get(u)
        if r is None:
            results[u] = None
            continue
        try:
            body = r.json()
            results[u] = {"st": r.status_code, "b": len(r.content), "shape": shape(body)}
            print("BODY", u, json.dumps(body, ensure_ascii=False)[:6000], flush=True)
        except ValueError:
            results[u] = {"st": r.status_code, "b": len(r.content), "text": r.text[:300]}
    blob = json.dumps(results, ensure_ascii=False, separators=(",", ":"))
    for i in range(0, min(len(blob), 3800 * 5), 3800):
        annotate("notice", f"strateg calls {i // 3800}", blob[i:i + 3800])
    print("calls", CALLS["n"], flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
