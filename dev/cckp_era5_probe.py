"""Read-only probe for GL-036 (CCKP ERA5, #2421): robots.txt, terms of use, sub-national codes, newer period ids.

GET only, one request at a time, 5 s apart, an identifying User-Agent, bounded retries honouring Retry-After.
Nothing is written to R2. Full text goes to DEV_OUTPUT_DIR (artifact), compact results to at most ten notices.
"""

from __future__ import annotations

import html
import json
import os
import re
import time
import urllib.parse

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
API = "https://cckpapi.worldbank.org/cckp/v1/"
OUT = os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output")
S = requests.Session()
S.headers.update({"User-Agent": UA})
FULL: dict = {}


def ds(var: str, agg: str, period: str = "1950-2024") -> str:
    return f"era5-x0.25_timeseries_{var}_timeseries_{agg}_{period}_mean_historical_era5_x0.25_mean"


def get(url: str, keep: int = 400) -> dict:
    last = {}
    for attempt in range(4):
        time.sleep(5 if attempt == 0 else 20 * attempt)
        try:
            r = S.get(url, timeout=(20, 120))
        except requests.RequestException as exc:
            last = {"err": type(exc).__name__}
            continue
        last = {"st": r.status_code, "ct": r.headers.get("content-type", "")[:40], "n": len(r.content),
                "lm": r.headers.get("last-modified"), "ra": r.headers.get("retry-after")}
        if r.status_code in (429, 500, 502, 503, 504):
            ra = r.headers.get("Retry-After", "")
            if ra.isdigit():
                time.sleep(min(120, int(ra)))
            last["body"] = r.text[:keep]
            continue
        last["text"] = r.text
        return last
    return last


def plain(t: str) -> str:
    t = re.sub(r"(?is)<(script|style|noscript).*?</\1>", " ", t)
    t = re.sub(r"(?s)<[^>]+>", " ", t)
    return re.sub(r"\s+", " ", html.unescape(t)).strip()


def around(t: str, pattern: str, before=100, after=300, limit=6) -> list[str]:
    p = plain(t)
    out, last = [], -9999
    for m in re.finditer(pattern, p, re.I):
        if m.start() - last < after:
            continue
        last = m.start()
        out.append(p[max(0, m.start() - before): m.start() + after])
        if len(out) >= limit:
            break
    return out


def short(r: dict) -> dict:
    return {k: v for k, v in r.items() if k != "text"}


def main() -> int:
    os.makedirs(OUT, exist_ok=True)
    # 1. robots.txt
    robots = {}
    for host in ("https://cckpapi.worldbank.org", "https://datacatalogapi.worldbank.org",
                 "https://climateknowledgeportal.worldbank.org", "https://worldbank.github.io"):
        r = get(host + "/robots.txt")
        robots[host] = {**short(r), "head": (r.get("text") or r.get("body") or "")[:300]}
        FULL["robots " + host] = r.get("text") or r.get("body")
    annotate("notice", "probe robots", robots)

    # 2. documentation and terms
    docs = {}
    r = get("https://worldbank.github.io/climateknowledgeportal/README.html")
    FULL["readme"] = r.get("text")
    t = r.get("text") or ""
    docs["readme"] = {**short(r), "geo": around(t, r"subnational|sub-national|geocode|all_countries|ADM1|USA\.2593", limit=8)}
    r = get("https://climateknowledgeportal.worldbank.org/download-data")
    FULL["download"] = r.get("text")
    t = r.get("text") or ""
    links = sorted({urllib.parse.urljoin("https://climateknowledgeportal.worldbank.org/download-data", h)
                    for h in re.findall(r'href="([^"]+)"', t) if re.search(r"term|licen|legal|disclaim", h, re.I)})
    docs["download"] = {**short(r), "terms_links": links[:10], "terms_text": around(t, r"terms of use|licen|attribution", limit=4)}
    annotate("notice", "probe docs", docs)
    terms = {}
    for link in (links or ["https://climateknowledgeportal.worldbank.org/terms-of-use"])[:3]:
        r = get(link)
        FULL["terms " + link] = r.get("text")
        t = r.get("text") or ""
        terms[link] = {**short(r), "text": plain(t)[:600],
                       "licence": around(t, r"creative commons|CC BY|licen|attribut|commercial", limit=5)}
    annotate("notice", "probe terms", terms)

    # 3. newer period ids and the documented alternative host (POL, small answers)
    periods = {}
    for var, agg, period in (("tas", "annual", "1950-2024"), ("tas", "annual", "1950-2025"),
                             ("tas", "monthly", "1950-2025"), ("pr", "annual", "1950-2025"),
                             ("tas", "annual", "1950-2023"), ("tas", "annual", "1940-2025")):
        r = get(f"{API}{ds(var, agg, period)}/POL?_format=json")
        info = short(r)
        try:
            body = json.loads(r.get("text") or "")
            data = body.get("data") or {}
            info["meta"] = body.get("metadata")
            info["geos"] = len(data)
            pol = data.get("POL") or {}
            info["keys"] = [min(pol), max(pol), len(pol)] if pol else []
        except (ValueError, AttributeError):
            info["head"] = (r.get("text") or r.get("body") or "")[:200]
        periods[f"{var} {agg} {period}"] = info
    r = get(f"https://datacatalogapi.worldbank.org/ddhxext/v3/cckp/{ds('tas', 'annual')}/POL?_format=json")
    periods["datacatalogapi v3"] = {**short(r), "head": (r.get("text") or r.get("body") or "")[:300]}
    annotate("notice", "probe periods", periods)

    # 4. sub-national codes: candidate geocodes on the small annual id
    sub = {}
    for geo in ("all_subnationals", "all_subnational", "POL_all", "POL.all", "POL_subnationals", "subnationals_POL",
                "all_countries_subnationals", "POL.2421", "POL.2593"):
        r = get(f"{API}{ds('tas', 'annual')}/{geo}?_format=json")
        info = short(r)
        try:
            body = json.loads(r.get("text") or "")
            data = body.get("data") or {}
            keys = list(data)
            info["geos"] = len(keys)
            info["sample"] = keys[:5]
            info["pol"] = [k for k in keys if k.startswith("POL")][:30]
            if len(keys) > 1:
                FULL["sub " + geo] = r.get("text")
        except (ValueError, AttributeError):
            info["head"] = (r.get("text") or r.get("body") or "")[:200]
        sub[geo] = info
    annotate("notice", "probe subnational", sub)

    # 5. public S3 bucket: top level and anything named like a geocode list
    s3 = {}
    for prefix in ("", "cckp/", "data/"):
        r = get(f"https://wbg-cckp.s3.amazonaws.com/?list-type=2&delimiter=/&max-keys=200&prefix={prefix}")
        t = r.get("text") or ""
        s3[prefix or "/"] = {**short(r), "prefixes": re.findall(r"<Prefix>([^<]*)</Prefix>", t)[:40],
                             "keys": re.findall(r"<Key>([^<]*)</Key>", t)[:40]}
    annotate("notice", "probe s3", s3)

    with open(os.path.join(OUT, "probe.json"), "w", encoding="utf-8") as handle:
        json.dump(FULL, handle, ensure_ascii=False, indent=1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
