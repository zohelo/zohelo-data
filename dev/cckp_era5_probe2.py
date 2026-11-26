"""Read-only probe 2 for GL-036 (CCKP ERA5, #2421): sub-national unit code list, terms text, the v3 host.

GET only, one request at a time, 5 s apart, an identifying User-Agent, bounded retries honouring Retry-After.
Nothing is written to R2. Text goes to the job log (### lines), compact results to notices.
"""

from __future__ import annotations

import html
import json
import re
import time
import urllib.parse

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
V3 = "https://datacatalogapi.worldbank.org/ddhxext/v3/cckp/"
S = requests.Session()
S.headers.update({"User-Agent": UA})
PAGE = "https://climateknowledgeportal.worldbank.org/download-data"


def ds(var: str, agg: str, period: str = "1950-2025") -> str:
    return f"era5-x0.25_timeseries_{var}_timeseries_{agg}_{period}_mean_historical_era5_x0.25_mean"


def get(url: str) -> requests.Response | None:
    for attempt in range(4):
        time.sleep(5 if attempt == 0 else 20 * attempt)
        try:
            r = S.get(url, timeout=(20, 180))
        except requests.RequestException as exc:
            print("###", url, type(exc).__name__, flush=True)
            continue
        if r.status_code in (429, 500, 502, 503, 504):
            print("###", url, r.status_code, r.headers.get("Retry-After"), flush=True)
            ra = r.headers.get("Retry-After", "")
            if ra.isdigit():
                time.sleep(min(120, int(ra)))
            continue
        return r
    return None


def plain(t: str) -> str:
    t = re.sub(r"(?is)<(script|style|noscript).*?</\1>", " ", t)
    t = re.sub(r"(?s)<[^>]+>", " ", t)
    return re.sub(r"\s+", " ", html.unescape(t)).strip()


def info(r) -> dict:
    if r is None:
        return {"err": "no answer"}
    return {"st": r.status_code, "n": len(r.content), "ct": r.headers.get("content-type", "")[:40], "url": r.url[:160]}


def main() -> int:
    out: dict = {}
    r = get(PAGE)
    t = r.text if r is not None else ""
    p = plain(t)
    i = p.find("Terms of Use Terms of Use")
    print("### TERMS SECTION:", p[i: i + 3500] if i >= 0 else "not found", flush=True)
    i = p.find("Spatially Aggregated Data CCKP offers")
    print("### AGGREGATED SECTION:", p[i: i + 3000] if i >= 0 else "not found", flush=True)
    hrefs = sorted({urllib.parse.urljoin(PAGE, h) for h in re.findall(r'href="([^"]+)"', t)})
    codes = [h for h in hrefs if re.search(r"json|xls|csv|code|unit|subnat|geo", h, re.I)]
    print("### LINKS:", json.dumps(codes, indent=0), flush=True)
    for m in re.finditer(r"Unit Codes", p):
        print("### UNIT CODES CONTEXT:", p[max(0, m.start() - 300): m.start() + 200], flush=True)
    for m in re.finditer(r'<a[^>]+href="([^"]+)"[^>]*>\s*(JSON Download|Excel Download)', t):
        print("### DOWNLOAD LINK:", m.group(1), m.group(2), "| before:", plain(t[max(0, m.start() - 600): m.start()])[-160:], flush=True)
    out["links"] = codes[:25]

    # the sub-national code list (JSON links near "Unit Codes")
    lists = []
    for m in re.finditer(r'<a[^>]+href="([^"]+)"[^>]*>\s*JSON Download', t):
        lists.append(urllib.parse.urljoin(PAGE, m.group(1)))
    found = {}
    for link in lists[:4]:
        rr = get(link)
        entry = info(rr)
        if rr is not None and rr.status_code == 200:
            body = rr.text
            entry["head"] = body[:300]
            pol = [m.group(0) for m in re.finditer(r'.{0,160}POL\.19823\d\d.{0,200}', body)]
            entry["pol_matches"] = len(pol)
            for line in pol[:20]:
                print("### POL ENTRY", link, ":", line, flush=True)
            try:
                parsed = json.loads(body)
                entry["type"] = type(parsed).__name__
                sample = parsed[:2] if isinstance(parsed, list) else {k: parsed[k] for k in list(parsed)[:2]}
                entry["sample"] = json.dumps(sample, ensure_ascii=False)[:600]
            except ValueError:
                entry["json"] = False
        found[link] = entry
    out["code_lists"] = found
    annotate("notice", "probe2 page and code lists", out)

    # terms of use for datasets (final URL and the CC BY passage)
    r = get("https://www.worldbank.org/en/about/legal/terms-of-use-for-datasets")
    tt = plain(r.text) if r is not None else ""
    terms = {**info(r), "history": [h.status_code for h in (r.history if r is not None else [])]}
    i = tt.find("Creative Commons")
    print("### DATASET TERMS (final", r.url if r is not None else None, "):", tt[max(0, i - 1500): i + 3000] if i >= 0 else tt[:3000], flush=True)
    terms["cc_by"] = tt[max(0, i - 200): i + 900] if i >= 0 else None
    annotate("notice", "probe2 dataset terms", terms)

    # the v3 host: robots, newer periods, all countries, one sub-national code, several codes in one request
    v3 = {}
    r = get("https://datacatalogapi.worldbank.org/robots.txt")
    v3["robots"] = info(r)
    for label, url in (
        ("annual tas 1950-2026 POL", f"{V3}{ds('tas', 'annual', '1950-2026')}/POL?_format=json"),
        ("annual tas 1950-2025 all_countries", f"{V3}{ds('tas', 'annual')}/all_countries?_format=json"),
        ("monthly pr 1950-2025 POL.1982370", f"{V3}{ds('pr', 'monthly')}/POL.1982370?_format=json"),
        ("annual pr 1950-2025 two codes comma", f"{V3}{ds('pr', 'annual')}/POL.1982370,POL.1982371?_format=json"),
        ("monthly pr 1950-2025 POL", f"{V3}{ds('pr', 'monthly')}/POL?_format=json"),
        ("annual pr 1950-2025 POL", f"{V3}{ds('pr', 'annual')}/POL?_format=json"),
    ):
        rr = get(url)
        e = info(rr)
        try:
            body = rr.json()
            data = body.get("data") or {}
            e["geos"] = len(data)
            e["sample_geos"] = list(data)[:4]
            first = next(iter(data.values()), {}) if data else {}
            e["periods"] = [min(first), max(first), len(first)] if first else []
            e["value_types"] = sorted({type(v).__name__ for v in first.values()}) if first else []
            e["nulls"] = sum(1 for g in data.values() for v in g.values() if v is None or v == "")
            if label.endswith("POL"):
                pol = data.get("POL") or {}
                e["2024"] = {k: v for k, v in pol.items() if k.startswith("2024")}
        except (ValueError, AttributeError) as exc:
            e["not_json"] = str(exc)[:80]
        v3[label] = e
    annotate("notice", "probe2 v3 host", v3)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
