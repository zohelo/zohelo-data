"""Read-only Dev probe 2 of GIOŚ Portal Jakości Powietrza (PL-ENV-010, #2960): OpenAPI document, v1 endpoints
(paging, response shape), archive endpoint, annual ZIP links. One request at a time, 31 s between API requests
(below the 2 per minute limit of the list endpoints), bounded retries. Writes nothing. Deleted before merge."""

from __future__ import annotations

import html
import json
import re
import sys
import time

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
API = "https://api.gios.gov.pl/pjp-api/v1/rest"
PAUSE = 31
NOTES = 0


def note(title, data, level="notice"):
    global NOTES
    text = json.dumps(data, ensure_ascii=False, default=str)
    print(f"### {title}: {text}", flush=True)
    if NOTES < 9:
        NOTES += 1
        annotate(level, title, json.loads(text) if len(text) < 3800 else {"cut": text[:3700]})


def get(url, params=None, accept="application/ld+json, application/json;q=0.9, */*;q=0.5"):
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, headers={"User-Agent": UA, "Accept": accept}, timeout=(30, 120))
            print(f"GET {r.url} -> {r.status_code} {len(r.content)} bytes; headers {dict(r.headers)}", flush=True)
            if r.status_code in (429, 500, 502, 503, 504) and attempt < 2:
                time.sleep(65 * (attempt + 1))
                continue
            return r
        except requests.RequestException as exc:
            print(f"GET {url} error {exc}", flush=True)
            if attempt == 2:
                return None
            time.sleep(30 * (attempt + 1))


def shape(r):
    if r is None:
        return {"error": "no response"}
    out = {"status": r.status_code, "bytes": len(r.content), "ctype": r.headers.get("Content-Type"),
           "hdr": {k: v for k, v in r.headers.items() if re.search("limit|retry|etag|modified|cache", k, re.I)}}
    try:
        j = r.json()
    except ValueError:
        out["text"] = r.text[:300]
        return out
    if isinstance(j, dict):
        for k, v in j.items():
            if isinstance(v, list):
                out[f"list[{k}]"] = {"len": len(v), "first": v[:2]}
            elif isinstance(v, dict):
                out[f"dict[{k}]"] = json.dumps(v, ensure_ascii=False)[:400]
            else:
                out[k] = v
    else:
        out["list_len"] = len(j)
        out["first"] = j[:2]
    return out


def items(r):
    try:
        j = r.json()
        return next(v for v in j.values() if isinstance(v, list))
    except Exception:
        return []


def main():
    # 1. OpenAPI document
    spec = None
    for path in ("/pjp-api/v3/api-docs", "/pjp-api/v3/api-docs/swagger-config", "/pjp-api/v2/api-docs"):
        r = get("https://api.gios.gov.pl" + path, accept="application/json, */*;q=0.5")
        if r is not None and r.status_code == 200:
            try:
                j = r.json()
            except ValueError:
                continue
            print("OPENAPI", path, json.dumps(j, ensure_ascii=False)[:60000], flush=True)
            if "paths" in j:
                spec = j
                break
            for u in j.get("urls", []) or []:
                r2 = get("https://api.gios.gov.pl" + u["url"] if u["url"].startswith("/") else u["url"], accept="application/json")
                if r2 is not None and r2.status_code == 200:
                    print("OPENAPI-URL", u, r2.text[:60000], flush=True)
                    try:
                        spec = r2.json()
                    except ValueError:
                        pass
                    time.sleep(3)
            if spec:
                break
        time.sleep(3)
    summary = {}
    if spec:
        for p, ops in spec.get("paths", {}).items():
            for m, op in ops.items():
                summary[p] = {"summary": (op.get("summary") or "")[:120],
                              "params": [f"{x.get('name')}:{x.get('in')}:{(x.get('schema') or {}).get('type')}"
                                         f"{'=' + str((x.get('schema') or {}).get('default')) if (x.get('schema') or {}).get('default') is not None else ''}"
                                         f"{'<=' + str((x.get('schema') or {}).get('maximum')) if (x.get('schema') or {}).get('maximum') is not None else ''}"
                                         for x in op.get("parameters", [])]}
    note("gios openapi paths", summary or {"spec": None})
    # 2. Archive ZIP links
    r = get("https://powietrze.gios.gov.pl/pjp/archives", accept="text/html")
    links = []
    if r is not None:
        for m in re.finditer(r"<a[^>]+href=\"([^\"]+)\"[^>]*>(?:(?!</a>).){0,300}?Wyniki pomiarów z (\d{4})", r.text, re.S):
            links.append((m.group(2), html.unescape(m.group(1))))
        i = r.text.find("Wyniki pomiarów z 2025")
        print("ARCHIVE HTML NEAR 2025:", r.text[max(0, i - 1500): i + 300], flush=True)
        j = r.text.find("na temat plików zip")
        print("ARCHIVE HTML ZIP INFO:", r.text[max(0, j - 800): j + 300], flush=True)
    heads = []
    for year, link in [x for x in links if x[0] in ("2025", "2024", "2000")]:
        url = requests.compat.urljoin("https://powietrze.gios.gov.pl/pjp/archives", link)
        try:
            h = requests.head(url, headers={"User-Agent": UA}, timeout=60, allow_redirects=True)
            heads.append({"year": year, "url": url, "final": h.url, "status": h.status_code,
                          "len": h.headers.get("Content-Length"), "type": h.headers.get("Content-Type"),
                          "lm": h.headers.get("Last-Modified"), "disp": h.headers.get("Content-Disposition")})
        except requests.RequestException as exc:
            heads.append({"year": year, "url": url, "error": str(exc)[:200]})
        time.sleep(5)
    note("gios archive zip links", {"links": len(links), "years": [y for y, _ in links], "sample": links[-3:], "heads": heads})
    # 3. v1 endpoints
    r = get(f"{API}/station/findAll", {"page": 0, "size": 500})
    s = shape(r)
    stations = items(r)
    note("v1 station/findAll page=0 size=500", s)
    print("STATIONS", len(stations), json.dumps(stations[:3], ensure_ascii=False), flush=True)
    if stations:
        keys = list(stations[0])
        print("STATION KEYS", keys, flush=True)
        for k in keys:
            vals = sorted({str(st.get(k)) for st in stations})
            print(f"STATION FIELD {k}: {len(vals)} distinct: {vals[:40]}", flush=True)
    time.sleep(PAUSE)
    r = get(f"{API}/station/findAll", {"page": 1, "size": 500})
    note("v1 station/findAll page=1", shape(r))
    time.sleep(PAUSE)
    r = get(f"{API}/station/findAll")
    s = shape(r)
    note("v1 station/findAll default", {k: v for k, v in s.items() if not k.startswith("list[")} | {"n": len(items(r))})
    time.sleep(PAUSE)
    sid = None
    for st in stations:
        sid = st.get("Identyfikator stacji")
        if sid:
            break
    r = get(f"{API}/station/sensors/{sid}")
    sens = items(r)
    note("v1 station/sensors", {"station": sid, **shape(r)})
    time.sleep(PAUSE)
    r = get(f"{API}/station/sensors/{sid}")
    note("v1 station/sensors again after 31 s", {"status": getattr(r, "status_code", None)})
    time.sleep(3)
    t0 = time.monotonic()
    r = get(f"{API}/station/sensors/{stations[1].get('Identyfikator stacji') if len(stations) > 1 else sid}")
    note("v1 station/sensors after 3 s", {"status": getattr(r, "status_code", None), "s": round(time.monotonic() - t0, 2),
                                          "text": (r.text[:300] if r is not None and r.status_code != 200 else "")})
    out = []
    for x in sens[:3]:
        sensor = x.get("Identyfikator stanowiska")
        time.sleep(2)
        r = get(f"{API}/data/getData/{sensor}", {"size": 500})
        vals = items(r)
        try:
            print("DATA", sensor, r.text[:4000], flush=True)
        except Exception:
            pass
        d = shape(r)
        d.pop([k for k in d if k.startswith("list[")][0], None) if any(k.startswith("list[") for k in d) else None
        out.append({"sensor": sensor, "param": x.get("Wskaźnik - kod"), **d, "n": len(vals),
                    "first": vals[:1], "last": vals[-1:], "nulls": sum(1 for v in vals if v.get("Wartość") is None)})
        time.sleep(2)
        r = get(f"{API}/data/getData/{sensor}")
        out[-1]["default_n"] = len(items(r))
        out[-1]["default_keys"] = {k: v for k, v in shape(r).items() if not k.startswith("list[")}
    note("v1 data/getData", out)
    time.sleep(2)
    r = get(f"{API}/aqindex/getIndex/{sid}")
    note("v1 aqindex/getIndex", {"station": sid, **shape(r)})
    time.sleep(PAUSE)
    # 4. archive endpoint(s) from the OpenAPI document, first sensor, 3 days
    tried = []
    for p in [p for p in (spec or {}).get("paths", {}) if "archiv" in p.lower()][:2]:
        url = "https://api.gios.gov.pl" + (spec.get("servers", [{}])[0].get("url", "").replace("https://api.gios.gov.pl", "") if spec.get("servers") else "") + re.sub(r"\{[^}]+\}", str(sens[0].get("Identyfikator stanowiska") if sens else sid), p)
        r = get(url, {"dateFrom": "2025-01-01 00:00", "dateTo": "2025-01-03 23:00", "size": 500})
        tried.append({"url": url, **shape(r)})
        if r is not None:
            print("ARCHIVE API", url, r.text[:4000], flush=True)
        time.sleep(PAUSE)
    note("v1 archive endpoint", tried)
    return 0


if __name__ == "__main__":
    sys.exit(main())
