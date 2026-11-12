"""Read-only Dev probe of GIOŚ Portal Jakości Powietrza (PL-ENV-010, #2960): API v1 endpoints, paging, limits,
archive routes and the terms page. One request at a time, 31 s between API requests (below 2 per minute),
bounded retries. Writes nothing. Deleted before merge."""

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


def get(url, params=None, stream=False):
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, headers={"User-Agent": UA, "Accept": "application/json, text/html"},
                             timeout=(30, 120), stream=stream)
            print(f"GET {r.url} -> {r.status_code} {len(r.content) if not stream else '-'} bytes; headers {dict(r.headers)}", flush=True)
            if r.status_code in (429, 500, 502, 503, 504) and attempt < 2:
                time.sleep(60 * (attempt + 1))
                continue
            return r
        except requests.RequestException as exc:
            print(f"GET {url} error {exc}", flush=True)
            if attempt == 2:
                return None
            time.sleep(30 * (attempt + 1))


def text_of(body):
    body = re.sub(r"(?is)<(script|style).*?</\1>", " ", body)
    body = re.sub(r"(?s)<[^>]+>", "\n", body)
    lines = [html.unescape(x).strip() for x in body.splitlines()]
    return "\n".join(x for x in lines if x)


def summarize(r):
    if r is None:
        return {"error": "no response"}
    out = {"status": r.status_code, "bytes": len(r.content), "ctype": r.headers.get("Content-Type"),
           "rate_headers": {k: v for k, v in r.headers.items() if "limit" in k.lower() or "retry" in k.lower()}}
    try:
        j = r.json()
    except ValueError:
        out["text"] = r.text[:400]
        return out
    if isinstance(j, dict):
        out["keys"] = list(j)
        for k, v in j.items():
            if isinstance(v, list):
                out[f"len[{k}]"] = len(v)
                out[f"first[{k}]"] = v[:2]
            elif not isinstance(v, (dict, list)):
                out[k] = v
            else:
                out[f"dict[{k}]"] = str(v)[:300]
    else:
        out["list_len"] = len(j)
        out["first"] = j[:2]
    return out


def main():
    # 1. API documentation page
    r = get("https://powietrze.gios.gov.pl/pjp/content/api")
    page = text_of(r.text) if r is not None else ""
    print("=== API PAGE TEXT\n" + page, flush=True)
    paths = sorted(set(re.findall(r"/pjp-api/[A-Za-z0-9_/{}.\-]+", r.text if r is not None else "")))
    limits = [ln for ln in page.splitlines() if re.search(r"limit|żądań|minut|size|stron|archiw", ln, re.I)]
    note("gios api page", {"status": getattr(r, "status_code", None), "paths": paths[:60], "limit_lines": limits[:40]})
    time.sleep(5)
    # 2. Terms
    r = get("https://powietrze.gios.gov.pl/pjp/content/terms_of_service")
    terms = text_of(r.text) if r is not None else ""
    print("=== TERMS TEXT\n" + terms, flush=True)
    i = terms.find("Licenc")
    note("gios terms", {"status": getattr(r, "status_code", None), "excerpt": terms[max(0, i - 1500): i + 2000]})
    time.sleep(5)
    # 3. Archive page
    r = get("https://powietrze.gios.gov.pl/pjp/archives")
    arch_links = []
    if r is not None:
        print("=== ARCHIVE TEXT\n" + text_of(r.text), flush=True)
        arch_links = sorted(set(re.findall(r'href="([^"]*(?:download|archiv|\.zip)[^"]*)"', r.text, re.I)))
    heads = []
    for link in arch_links[:4]:
        url = requests.compat.urljoin("https://powietrze.gios.gov.pl/pjp/archives", html.unescape(link))
        try:
            h = requests.head(url, headers={"User-Agent": UA}, timeout=60, allow_redirects=True)
            heads.append({"url": url, "status": h.status_code, "len": h.headers.get("Content-Length"),
                          "type": h.headers.get("Content-Type"), "disp": h.headers.get("Content-Disposition"),
                          "lm": h.headers.get("Last-Modified")})
        except requests.RequestException as exc:
            heads.append({"url": url, "error": str(exc)[:200]})
        time.sleep(5)
    note("gios archive page", {"status": getattr(r, "status_code", None), "links": len(arch_links),
                               "sample": arch_links[:40], "heads": heads})
    # 4. API v1 endpoints, one at a time, 31 s apart
    r = get(f"{API}/station/findAll", {"page": 0, "size": 500})
    s = summarize(r)
    note("v1 station/findAll size=500", s)
    stations = []
    try:
        j = r.json()
        stations = next(v for v in j.values() if isinstance(v, list))
    except Exception:
        pass
    sid = None
    for st in stations:
        sid = st.get("Identyfikator stacji") or st.get("id")
        if sid:
            break
    print("STATIONS", len(stations), json.dumps(stations[:3], ensure_ascii=False), flush=True)
    voiv = sorted({str(st.get("Województwo")) for st in stations})
    print("VOIVODESHIPS", voiv, flush=True)
    time.sleep(PAUSE)
    r = get(f"{API}/station/findAll", {"page": 1, "size": 500})
    note("v1 station/findAll page=1", summarize(r))
    time.sleep(PAUSE)
    r = get(f"{API}/station/sensors/{sid}")
    s = summarize(r)
    note("v1 station/sensors", {"station": sid, **s})
    sensors = []
    try:
        sensors = next(v for v in r.json().values() if isinstance(v, list))
    except Exception:
        pass
    sens = [x.get("Identyfikator stanowiska") or x.get("id") for x in sensors]
    time.sleep(PAUSE)
    data_summary = []
    for sensor in sens[:2]:
        r = get(f"{API}/data/getData/{sensor}", {"size": 500})
        s = summarize(r)
        try:
            j = r.json()
            vals = next(v for v in j.values() if isinstance(v, list))
            s["n"] = len(vals)
            s["dates"] = [vals[0], vals[-1]] if vals else []
            s["nulls"] = sum(1 for v in vals if v.get("Wartość") is None)
            print("DATA", sensor, json.dumps(j, ensure_ascii=False)[:3000], flush=True)
        except Exception:
            pass
        data_summary.append({"sensor": sensor, **s})
        time.sleep(PAUSE)
    note("v1 data/getData", data_summary)
    r = get(f"{API}/aqindex/getIndex/{sid}")
    note("v1 aqindex/getIndex", {"station": sid, **summarize(r)})
    time.sleep(PAUSE)
    # 5. Archive endpoints named on the API page (tried with the first sensor, a short range)
    tried = []
    for path in [p for p in paths if "archiv" in p.lower()][:3]:
        url = "https://api.gios.gov.pl" + re.sub(r"\{[^}]+\}", str(sens[0] if sens else sid), path)
        r = get(url, {"dateFrom": "2025-01-01 00:00", "dateTo": "2025-01-03 23:00", "size": 500})
        tried.append({"url": url, **summarize(r)})
        time.sleep(PAUSE)
    note("v1 archive endpoints", tried)
    return 0


if __name__ == "__main__":
    sys.exit(main())
