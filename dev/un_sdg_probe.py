"""Read-only probe for GL-018 (UN Global SDG Indicators Database, SDG API v1), #3316.

GET (and one small POST data query) to unstats.un.org only, one request at a time, PAUSE s apart, an identifying
User-Agent, bounded retries. Nothing is written to R2. Compact results go to at most ten notices.

Part 1: robots.txt, swagger (paths and parameters), Series/List, the database archive page (bulk files?), one
Series/Data answer for two areas (shape, attributes, dimensions, paging, repeated areaCode), DataCSV.
Part 2 (argument `count`): totalElements per series for Poland and the 27 EU countries (pageSize=1), time-boxed.
Deleted before merge (never on the PR branch).
"""

from __future__ import annotations

from collections import Counter
import json
import re
import sys
import time

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
HOST = "https://unstats.un.org"
API = f"{HOST}/SDGAPI/v1/sdg"
PAUSE = 1.5
EU = [40, 56, 100, 191, 196, 203, 208, 233, 246, 250, 276, 300, 348, 372, 380, 428, 440, 442, 470, 528, 616, 620,
      642, 703, 705, 724, 752]
S = requests.Session()
S.headers.update({"User-Agent": UA, "Accept": "application/json"})


def call(url: str, params=None, post=None, accept=None) -> dict:
    last: dict = {}
    for attempt in range(4):
        time.sleep(PAUSE if attempt == 0 else 20 * attempt)
        try:
            headers = {"Accept": accept} if accept else None
            if post is not None:
                r = S.post(url, data=post, timeout=(20, 300), headers=headers)
            else:
                r = S.get(url, params=params, timeout=(20, 300), headers=headers)
        except requests.RequestException as exc:
            last = {"err": type(exc).__name__}
            continue
        last = {"st": r.status_code, "ct": r.headers.get("content-type", "")[:60], "n": len(r.content),
                "url": r.url[:300], "lm": r.headers.get("last-modified"), "etag": r.headers.get("etag")}
        if r.status_code in (429, 500, 502, 503, 504):
            last["body"] = r.text[:300]
            continue
        last["text"] = r.text
        return last
    return last


def short(r: dict, keep: int = 0) -> dict:
    out = {k: v for k, v in r.items() if k != "text" and v is not None}
    if keep and "text" in r:
        out["head"] = r["text"][:keep]
    return out


def part1() -> None:
    robots = call(f"{HOST}/robots.txt")
    swagger = call(f"{HOST}/SDGAPI/swagger/v1/swagger.json")
    paths = {}
    try:
        for path, ops in json.loads(swagger["text"]).get("paths", {}).items():
            for method, op in ops.items():
                params = [p.get("name") for p in op.get("parameters", [])]
                body = op.get("requestBody", {}).get("content", {})
                if body:
                    schema = next(iter(body.values())).get("schema", {})
                    params.append("body:" + ",".join(schema.get("properties", {}).keys()))
                paths[f"{method.upper()} {path}"] = params
    except (KeyError, ValueError) as exc:
        paths = {"error": str(exc)}
    annotate("notice", "sdg probe 1 robots swagger", {"robots": short(robots, 800), "swagger": short(swagger),
                                                       "paths": paths})
    series = call(f"{API}/Series/List", {"allreleases": "false"})
    info: dict = short(series)
    try:
        items = json.loads(series["text"])
        info["series"] = len(items)
        info["keys"] = sorted(items[0].keys())
        info["first"] = {k: (str(v)[:120]) for k, v in items[0].items()}
        info["releases"] = dict(Counter(str(i.get("release")) for i in items))
        info["goals"] = dict(Counter(str((i.get("goal") or ["?"])[0]) for i in items))
        info["codes_sample"] = [i.get("code") for i in items[:15]]
        info["distinct_codes"] = len({i.get("code") for i in items})
    except (KeyError, ValueError, IndexError) as exc:
        info["error"] = str(exc)
    annotate("notice", "sdg probe 2 series list", info)
    pages = {}
    for url in (f"{HOST}/sdgs/indicators/database/archive", f"{HOST}/sdgs/indicators/database/",
                f"{HOST}/sdgs/dataportal/download"):
        r = call(url, accept="text/html")
        links = sorted(set(re.findall(r'href="([^"]+\.(?:zip|csv|xlsx)[^"]*)"', r.get("text", ""))))
        pages[url] = {**short(r), "data_links": len(links), "links_sample": links[:12]}
    annotate("notice", "sdg probe 3 archive pages", pages)
    data = call(f"{API}/Series/Data", [("seriesCode", "SH_DYN_MORT"), ("areaCode", "616"), ("areaCode", "276"),
                                         ("pageSize", "5")])
    d: dict = short(data)
    try:
        j = json.loads(data["text"])
        d["top_keys"] = sorted(j.keys())
        d["paging"] = {k: j.get(k) for k in ("size", "totalElements", "totalPages", "pageNumber")}
        d["attributes"] = j.get("attributes")
        d["dimensions"] = j.get("dimensions")
        d["row0"] = j["data"][0] if j.get("data") else None
        d["areas_in_page"] = sorted({x.get("geoAreaCode") for x in j.get("data", [])})
    except (KeyError, ValueError) as exc:
        d["error"] = str(exc)
    annotate("notice", "sdg probe 4 series data", {"truncated": json.dumps(d, default=str)[:3800]})
    big = call(f"{API}/Series/Data", [("seriesCode", "SH_DYN_MORT"), *[("areaCode", str(a)) for a in EU],
                                        ("pageSize", "100000")])
    b: dict = short(big)
    try:
        j = json.loads(big["text"])
        b["paging"] = {k: j.get(k) for k in ("size", "totalElements", "totalPages", "pageNumber")}
        b["rows"] = len(j.get("data", []))
        b["areas"] = len({x.get("geoAreaCode") for x in j.get("data", [])})
        b["value_types"] = dict(Counter(str(x.get("valueType")) for x in j.get("data", [])))
        b["value_non_numeric"] = dict(Counter(str(x.get("value")) for x in j.get("data", [])
                                              if not re.fullmatch(r"-?[0-9.]+(E[-+]?[0-9]+)?", str(x.get("value")))))
        b["nature"] = dict(Counter(str((x.get("attributes") or {}).get("Nature")) for x in j.get("data", [])))
        b["time_detail"] = dict(Counter(str(x.get("time_detail"))[:12] for x in j.get("data", [])).most_common(5))
    except (KeyError, ValueError) as exc:
        b["error"] = str(exc)
    # POST DataCSV (Swagger names the form fields; this tries the documented names)
    csv = call(f"{API}/Series/DataCSV", post={"seriesCodes": "SH_DYN_MORT", "areaCodes": "616"}, accept="text/csv")
    annotate("notice", "sdg probe 5 EU page and DataCSV", {"eu_all_in_one_page": b, "datacsv": short(csv, 600)})


def part2(minutes: float) -> None:
    """Every series for Poland and the EU countries, paged, analysed in memory (nothing stored)."""
    deadline = time.monotonic() + minutes * 60
    items = json.loads(call(f"{API}/Series/List", {"allreleases": "false"})["text"])
    codes = sorted({i["code"] for i in items})
    totals: dict[str, int] = {}
    errors: dict[str, str] = {}
    c: dict[str, Counter] = {k: Counter() for k in (
        "value_non_numeric", "value_type", "nature", "reporting", "dims", "attrs", "time_detail_shape", "units",
        "pages", "bytes_bucket", "start_frac")}
    units_per_series: dict[str, set] = {}
    dup_keys = dup_keys_td = dup_keys_full = rows = 0
    dup_examples: list = []
    nbytes = 0
    start = time.monotonic()
    for code in codes:
        if time.monotonic() > deadline:
            break
        data: list = []
        page, pages = 1, 1
        while page <= pages:
            r = call(f"{API}/Series/Data", [("seriesCode", code), *[("areaCode", str(a)) for a in EU],
                                            ("pageSize", "50000"), ("page", str(page))])
            try:
                j = json.loads(r["text"])
                pages = int(j.get("totalPages") or 1)
                totals[code] = int(j["totalElements"])
                data += j.get("data") or []
                nbytes += r["n"]
            except (KeyError, ValueError, TypeError):
                errors[code] = f"{r.get('st', r.get('err'))} {str(r.get('body', ''))[:80]}"
                break
            page += 1
        c["pages"][pages] += 1
        keys, keys_td, keys_full = Counter(), Counter(), Counter()
        for x in data:
            rows += 1
            v = str(x.get("value"))
            if not re.fullmatch(r"-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?", v):
                c["value_non_numeric"][v[:20]] += 1
            c["value_type"][str(x.get("valueType"))] += 1
            a, d = x.get("attributes") or {}, x.get("dimensions") or {}
            c["nature"][str(a.get("Nature"))] += 1
            c["reporting"][str(d.get("Reporting Type"))] += 1
            c["units"][str(a.get("Units"))] += 1
            units_per_series.setdefault(code, set()).add(str(a.get("Units")))
            for k in d:
                c["dims"][k] += 1
            for k in a:
                c["attrs"][k] += 1
            td = x.get("time_detail")
            c["time_detail_shape"][re.sub(r"\d", "9", str(td))[:15]] += 1
            ts = x.get("timePeriodStart")
            c["start_frac"][str(isinstance(ts, float) and not float(ts).is_integer())] += 1
            dk = json.dumps(d, sort_keys=True)
            base = (x.get("geoAreaCode"), dk, ts)
            keys[base] += 1
            keys_td[(*base, td)] += 1
            keys_full[(*base, td, x.get("source"), json.dumps(a, sort_keys=True))] += 1
        for name, counter in (("base", keys), ("td", keys_td), ("full", keys_full)):
            n = sum(1 for v in counter.values() if v > 1)
            if name == "base":
                dup_keys += n
                if n and len(dup_examples) < 6:
                    k = next(k for k, v in counter.items() if v > 1)
                    dup_examples.append({"series": code, "key": str(k)[:200], "rows": [
                        {kk: str(vv)[:60] for kk, vv in x.items() if kk in ("value", "time_detail", "source",
                                                                           "attributes", "timeCoverage")}
                        for x in data if (x.get("geoAreaCode"), json.dumps(x.get("dimensions") or {}, sort_keys=True),
                                          x.get("timePeriodStart")) == k][:3]})
            elif name == "td":
                dup_keys_td += n
            else:
                dup_keys_full += n
    vals = sorted(totals.values(), reverse=True)
    top = sorted(totals.items(), key=lambda kv: -kv[1])[:12]
    annotate("notice", "sdg probe 6 EU rows", {
        "series_listed": len(codes), "series_read": len(totals), "errors": len(errors),
        "error_sample": dict(list(errors.items())[:8]), "rows_total": sum(vals), "rows_read": rows,
        "json_bytes": nbytes, "series_with_rows": sum(1 for v in vals if v), "series_empty": sum(1 for v in vals if not v),
        "over_10k": sum(1 for v in vals if v > 10_000), "max": vals[:1], "top": top, "pages": dict(c["pages"]),
        "minutes": round((time.monotonic() - start) / 60, 1)})
    annotate("notice", "sdg probe 7 EU profile", {"truncated": json.dumps({
        "dup_area_dims_start": dup_keys, "dup_plus_time_detail": dup_keys_td, "dup_plus_source_attrs": dup_keys_full,
        "dup_examples": dup_examples,
        "series_with_several_units": sum(1 for s in units_per_series.values() if len(s) > 1),
        "several_units_sample": dict(list({k: sorted(v) for k, v in units_per_series.items() if len(v) > 1}.items())[:6]),
        **{k: dict(v.most_common(25)) for k, v in c.items() if k not in ("pages", "bytes_bucket")}},
        default=str)[:3900]})


def main(argv: list[str]) -> int:
    if not argv or argv[0] != "count":
        part1()
    if argv and argv[0] in ("count", "all"):
        part2(float(argv[1]) if len(argv) > 1 else 100)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
