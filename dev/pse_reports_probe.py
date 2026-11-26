"""Read-only Dev probe for #2870 (PSE Reports API v2, register PL-ENV-019; test branch only, never on main).

One request at a time, at least 1.5 s apart, timeouts, no writes anywhere. Answers: robots.txt, the API's own
catalogue or metadata, terms pages of raporty.pse.pl, field names of rce-pln and his-wlk-cal, a date-range filter
over a month and a year, paging (nextLink, $top, $skip), DST days (92/100 periods), publication_ts per day, nulls.
"""

from __future__ import annotations

from collections import Counter
import html
import re
import time

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
API = "https://api.raporty.pse.pl/api"
S = requests.Session()
S.headers.update({"User-Agent": UA})
LAST = [0.0]
COUNT = [0]


def get(url: str, params=None, accept="application/json"):
    wait = 1.5 - (time.monotonic() - LAST[0])
    if wait > 0:
        time.sleep(wait)
    LAST[0] = time.monotonic()
    COUNT[0] += 1
    try:
        return S.get(url, params=params, timeout=(20, 180), headers={"Accept": accept})
    except requests.RequestException as exc:
        return exc


def text_of(body: str) -> str:
    body = re.sub(r"(?s)<(script|style)[^>]*>.*?</\1>", " ", body)
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", body))).strip()


def brief(r) -> dict:
    if isinstance(r, Exception):
        return {"error": f"{type(r).__name__}: {str(r)[:200]}"}
    keep = ("server", "x-ratelimit-limit", "x-ratelimit-remaining", "retry-after", "last-modified", "etag",
            "cache-control", "odata-version", "content-encoding")  # fmt: skip
    return {"status": r.status_code, "type": r.headers.get("Content-Type"), "bytes": len(r.content),
            "headers": {k: v for k, v in r.headers.items() if k.lower() in keep}}  # fmt: skip


def rows(url, flt, extra=None):
    params = {"$filter": flt, **(extra or {})} if flt else dict(extra or {})
    r = get(url, params)
    info = brief(r)
    if isinstance(r, Exception) or r.status_code != 200:
        info["text"] = r.text[:400] if not isinstance(r, Exception) else None
        return info, []
    try:
        doc = r.json()
    except ValueError:
        info["text"] = r.text[:400]
        return info, []
    info["top_keys"] = sorted(doc) if isinstance(doc, dict) else type(doc).__name__
    vals = doc.get("value", []) if isinstance(doc, dict) else []
    info["rows"] = len(vals)
    info["nextLink"] = next((doc[k] for k in doc if "next" in k.lower()), None) if isinstance(doc, dict) else None
    return info, vals


def summary_of(vals, key="business_date"):
    by = Counter(v.get(key) for v in vals)
    return {"days": len(by), "first": min(by) if by else None, "last": max(by) if by else None,
            "periods_per_day": dict(Counter(by.values()))}  # fmt: skip


def stage(title, call):
    try:
        annotate("notice", title, call())
    except Exception as exc:  # each stage reports on its own
        annotate("error", title, f"{type(exc).__name__}: {str(exc)[:1500]}")


def roots():
    out = {}
    for url in ("https://api.raporty.pse.pl/robots.txt", "https://raporty.pse.pl/robots.txt"):
        r = get(url, accept="text/plain")
        out[url] = {**brief(r), "text": None if isinstance(r, Exception) else r.text[:600]}
    for url in ("https://api.raporty.pse.pl/", f"{API}/", f"{API}/$metadata", f"{API}/swagger.json",
                "https://api.raporty.pse.pl/swagger", "https://api.raporty.pse.pl/openapi.json",
                "https://api.raporty.pse.pl/docs"):  # fmt: skip
        r = get(url, accept="*/*")
        b = brief(r)
        if not isinstance(r, Exception):
            b["head"] = text_of(r.text)[:300]
            b["links"] = sorted(set(re.findall(r'href="([^"]+)"', r.text)))[:30]
        out[url] = b
    return out


def terms():
    out = {}
    r = get("https://raporty.pse.pl/", accept="text/html")
    out["root"] = brief(r)
    if not isinstance(r, Exception) and r.status_code == 200:
        links = sorted(set(re.findall(r'href="([^"]+)"', r.text)))
        out["root_links"] = links[:60]
        out["root_text"] = text_of(r.text)[:600]
        pattern = r"regulamin|warunk|licenc|nota|prawn|terms|polityk|zasad|api|dokument|instrukc|faq|pomoc"
        cand = [link for link in links if re.search(pattern, link, re.I)]
        out["candidates"] = cand[:15]
        for link in cand[:6]:
            u = requests.compat.urljoin("https://raporty.pse.pl/", link)
            p = get(u, accept="*/*")
            t = text_of(p.text) if not isinstance(p, Exception) else ""
            hit_re = r"licenc|warunk|wykorzyst|regulamin|źródł|prawa autor|copyright|CC BY|terms"
            hits = [t[max(0, m.start() - 150) : m.start() + 350] for m in re.finditer(hit_re, t, re.I)][:3]
            out[u] = {**brief(p), "hits": hits, "text_head": t[:200]}
    for u in ("https://www.pse.pl/nota-prawna", "https://www.pse.pl/regulamin-serwisu"):
        p = get(u, accept="text/html")
        t = text_of(p.text) if not isinstance(p, Exception) else ""
        hit_re = r"licenc|wykorzyst|prawa autor|kopiow|źródł"
        hits = [t[max(0, m.start() - 150) : m.start() + 450] for m in re.finditer(hit_re, t, re.I)][:3]
        out[u] = {**brief(p), "hits": hits}
    return out


def one_day():
    out = {}
    for ent in ("rce-pln", "his-wlk-cal"):
        info, vals = rows(f"{API}/{ent}", "business_date eq '2024-06-14'")
        info["fields"] = sorted(vals[0]) if vals else None
        info["first_row"] = vals[0] if vals else None
        info.update(summary_of(vals))
        info["null_counts"] = {k: n for k in (vals[0] if vals else {}) if (n := sum(v.get(k) is None for v in vals))}
        info["types"] = {k: type(v).__name__ for k, v in (vals[0].items() if vals else [])}
        out[ent] = info
        info2, _ = rows(f"{API}/{ent}", "business_date eq '2024-06-13'")
        out[ent + " 2024-06-13"] = {k: info2.get(k) for k in ("status", "rows", "text")}
    return out


def ranges():
    out = {}
    for ent in ("rce-pln", "his-wlk-cal"):
        info, vals = rows(f"{API}/{ent}", "business_date ge '2024-10-01' and business_date le '2024-10-31'")
        info.update(summary_of(vals))
        dst = [v for v in vals if v.get("business_date") == "2024-10-27"]
        info["dst_2024_10_27_period_dtime_utc"] = [[v.get("period"), v.get("dtime"), v.get("dtime_utc")] for v in dst[7:13]]
        pubs = Counter((v.get("business_date"), v.get("publication_ts")) for v in vals)
        info["publication_ts_distinct_per_day_max"] = max(Counter(d for d, _ in pubs).values()) if pubs else None
        info["publication_ts_sample"] = sorted({p for _, p in pubs})[:3]
        ordered = [(v.get("business_date"), v.get("dtime_utc")) for v in vals]
        info["sorted_by_date_dtime_utc"] = ordered == sorted(ordered)
        info["dup_date_dtime_utc"] = len(ordered) - len(set(ordered))
        info["dup_date_period"] = len(vals) - len({(v.get("business_date"), v.get("period")) for v in vals})
        info["null_counts"] = {k: n for k in (vals[0] if vals else {}) if (n := sum(v.get(k) is None for v in vals))}
        out[ent + " 2024-10"] = info
        _, vals_d = rows(f"{API}/{ent}", "business_date eq '2024-10-15'")
        inside = [v for v in vals if v.get("business_date") == "2024-10-15"]
        out[ent + " eq vs range 2024-10-15"] = {"eq": len(vals_d), "range": len(inside), "equal": vals_d == inside}
    for extra in ({"$top": "10"}, {"$top": "10", "$skip": "10"}, {"$orderby": "dtime_utc desc", "$top": "3"},
                  {"$select": "dtime_utc,rce_pln", "$top": "2"}, {"$count": "true", "$top": "1"}):  # fmt: skip
        info, vals = rows(f"{API}/rce-pln", "business_date eq '2025-01-02'", extra)
        keys = ("status", "rows", "nextLink", "text", "top_keys")
        out[f"rce-pln {extra}"] = {k: info.get(k) for k in keys} | {"first": vals[0] if vals else None}
    info, vals = rows(f"{API}/rce-pln", None)
    out["rce-pln no filter"] = {k: info.get(k) for k in ("status", "rows", "nextLink", "text", "bytes")} | summary_of(vals)
    return out


def year_and_dst():
    out = {}
    info, vals = rows(f"{API}/rce-pln", "business_date ge '2025-01-01' and business_date le '2025-12-31'")
    out["rce-pln 2025"] = info | summary_of(vals)
    info, vals = rows(f"{API}/his-wlk-cal", "business_date ge '2025-01-01' and business_date le '2025-03-31'")
    out["his-wlk-cal 2025-Q1"] = info | summary_of(vals)
    for day in ("2025-03-30", "2025-10-26", "2026-03-29"):
        for ent in ("rce-pln", "his-wlk-cal"):
            _, vals = rows(f"{API}/{ent}", f"business_date eq '{day}'")
            ok = len(vals) > 10
            out[f"{ent} {day}"] = {
                "rows": len(vals),
                "period_dtime_utc": [[vals[i].get("period"), vals[i].get("dtime"), vals[i].get("dtime_utc")]
                                     for i in (7, 8, 9, -1)] if ok else None,  # fmt: skip
            }
    return out


def newest():
    out = {}
    for ent in ("rce-pln", "his-wlk-cal"):
        info, vals = rows(f"{API}/{ent}", "business_date ge '2026-11-15'")
        info.update(summary_of(vals))
        pubs: dict = {}
        for v in vals:
            pubs.setdefault(v["business_date"], set()).add(v.get("publication_ts"))
        info["publication_ts_by_day"] = {d: [len(p), min(p), max(p)] for d, p in sorted(pubs.items())}
        nonnum = Counter()
        skip = ("business_date", "dtime", "dtime_utc", "period", "publication_ts", "period_utc")
        for v in vals:
            for k, x in v.items():
                if k not in skip and not isinstance(x, int | float):
                    nonnum[f"{k}={x!r}"[:60]] += 1
        info["non_numeric_values"] = dict(nonnum.most_common(15))
        info["last_row"] = vals[-1] if vals else None
        out[ent] = info
    out["requests"] = COUNT[0]
    return out


def main() -> int:
    stage("pse probe · robots and API roots", roots)
    stage("pse probe · terms pages", terms)
    stage("pse probe · one day, fields", one_day)
    stage("pse probe · range filter, paging", ranges)
    stage("pse probe · a year in one request, DST days", year_and_dst)
    stage("pse probe · newest days", newest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
