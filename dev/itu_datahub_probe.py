"""Read-only probe for GL-031 ITU DataHub through World Bank Data360 (#2419). GET/POST reads only, one request at a
time, >= 1 s apart, bounded retries; no writes to R2. Profiles the whole ITU_DH dataset and the catalogue resources."""

import collections
import gzip
import json
import os
import time
import urllib.error
import urllib.request

UA = "zohelo-data/1.0 (+https://zohelo.com)"
API = "https://data360api.worldbank.org/data360"
OUT = os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output")
LAST = [0.0]


def call(url, body=None, tries=4):
    for attempt in range(tries):
        wait = 1.2 - (time.time() - LAST[0])
        if wait > 0:
            time.sleep(wait)
        try:
            data = json.dumps(body).encode() if body is not None else None
            req = urllib.request.Request(url, data=data, headers={"User-Agent": UA, "Accept": "application/json",
                                                                   **({"Content-Type": "application/json"} if data else {})})
            with urllib.request.urlopen(req, timeout=120) as r:
                return r.status, dict(r.headers), r.read()
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504) and attempt < tries - 1:
                time.sleep(20 * (attempt + 1))
                continue
            return e.code, dict(e.headers or {}), e.read()[:2000]
        except Exception as e:  # noqa: BLE001
            if attempt < tries - 1:
                time.sleep(20 * (attempt + 1))
                continue
            return 0, {}, str(e).encode()
        finally:
            LAST[0] = time.time()
    return 0, {}, b""


def main():
    os.makedirs(OUT, exist_ok=True)
    notes = {}
    st, h, b = call(f"{API}/data?DATABASE_ID=ITU_DH&skip=0")
    first = json.loads(b)
    count = first["count"]
    notes["count"] = count
    notes["first_headers"] = {k: v for k, v in h.items() if k.lower() in ("content-type", "content-length", "etag", "last-modified", "cache-control")}
    rows = list(first["value"])
    pages = 1
    t0 = time.time()
    skip = len(rows)
    while skip < count and time.time() - t0 < 50 * 60:
        st, h, b = call(f"{API}/data?DATABASE_ID=ITU_DH&skip={skip}")
        if st != 200:
            notes.setdefault("page_errors", []).append([skip, st, b[:200].decode("utf-8", "replace")])
            break
        page = json.loads(b)
        if page.get("count") != count:
            notes.setdefault("count_changed", []).append([skip, page.get("count")])
        if not page["value"]:
            break
        rows += page["value"]
        skip += len(page["value"])
        pages += 1
    notes.update({"pages": pages, "rows_read": len(rows), "seconds": round(time.time() - t0)})
    with gzip.open(f"{OUT}/itu_dh_all.json.gz", "wt") as f:
        json.dump(rows, f)
    keys = collections.Counter(tuple(sorted(r)) for r in rows)
    notes["key_sets"] = [[list(k), n] for k, n in keys.most_common(3)]
    ind = collections.defaultdict(lambda: collections.defaultdict(collections.Counter))
    for r in rows:
        d = ind[r["INDICATOR"]]
        for c in ("UNIT_MEASURE", "UNIT_MULT", "FREQ", "OBS_STATUS", "OBS_CONF", "SEX", "AGE", "URBANISATION",
                  "COMP_BREAKDOWN_1", "COMP_BREAKDOWN_2", "COMP_BREAKDOWN_3", "DECIMALS", "TIME_FORMAT", "UNIT_TYPE",
                  "AGG_METHOD", "DATA_SOURCE", "SEGMENT", "EV_CRITERIA", "LATEST_DATA"):
            d[c][str(r.get(c))] += 1
        d["_n"]["n"] += 1
        d["_areas"][r["REF_AREA"]] += 1
        d["_years"][str(r["TIME_PERIOD"])] += 1
    for name, d in sorted(ind.items()):
        years = sorted(d["_years"])
        summary = {c: dict(v.most_common(12)) for c, v in d.items() if not c.startswith("_")}
        print(f"IND {name} n={d['_n']['n']} areas={len(d['_areas'])} years={years[0]}..{years[-1]} POL={d['_areas'].get('POL', 0)} "
              + json.dumps(summary), flush=True)
    other = collections.Counter()
    nonnum = collections.Counter()
    comments = collections.Counter()
    for r in rows:
        v = r.get("OBS_VALUE")
        try:
            float(v)
        except (TypeError, ValueError):
            nonnum[str(v)[:30]] += 1
        for c in ("COMMENT_OBS", "COMMENT_TS"):
            if r.get(c):
                comments[f"{c}:{str(r[c])[:80]}"] += 1
        other[str(r.get("DATABASE_ID"))] += 1
    print("NONNUM " + json.dumps(nonnum.most_common(20)))
    print("COMMENTS " + json.dumps(comments.most_common(40)))
    print("DATABASE_ID " + json.dumps(other.most_common()))
    key = lambda r: (r["INDICATOR"], r["REF_AREA"], r["SEX"], r["AGE"], r["URBANISATION"], r["COMP_BREAKDOWN_1"],
                     r["COMP_BREAKDOWN_2"], r["COMP_BREAKDOWN_3"], str(r["TIME_PERIOD"]), r["FREQ"])
    dup = collections.Counter(key(r) for r in rows)
    dups = [[list(k), n] for k, n in dup.items() if n > 1]
    notes["dup_keys"] = len(dups)
    print("DUPS " + json.dumps(dups[:20]))
    areas = collections.Counter(r["REF_AREA"] for r in rows)
    print("AREAS " + json.dumps(sorted(areas)))
    pol = [r for r in rows if r["REF_AREA"] == "POL" and r["INDICATOR"].endswith(("IT_NET_USER", "IT_NET_BBND", "IT_BB_MOB_PSB", "IT_BB_FIX_PSB", "IT_CEL_SETS"))]
    for r in sorted(pol, key=lambda r: (r["INDICATOR"], str(r["TIME_PERIOD"]))):
        if str(r["TIME_PERIOD"]) >= "2019":
            print("POL " + json.dumps({k: r[k] for k in ("INDICATOR", "TIME_PERIOD", "OBS_VALUE", "UNIT_MEASURE", "UNIT_MULT", "OBS_STATUS", "SEX", "COMP_BREAKDOWN_1")}))
    # metadata and disaggregation
    for body in ({"query": "&$filter=series_description/database_id eq 'ITU_DH'"},
                 {"query": "&$filter=series_description/idno eq 'ITU_DH_IT_NET_USER'"}):
        st, h, b = call(f"{API}/metadata", body)
        print(f"META {st} {len(b)} {json.dumps(body)}\n{b[:6000].decode('utf-8', 'replace')}\n=== END", flush=True)
        if st == 200:
            with open(f"{OUT}/metadata_{len(b)}.json", "wb") as f:
                f.write(b)
    for path in ("disaggregation?datasetId=ITU_DH&indicatorId=ITU_DH_IT_NET_USER",
                 "indicators?datasetId=ITU_DH"):
        st, h, b = call(f"{API}/{path}")
        print(f"GET {st} {path}\n{b[:3000].decode('utf-8', 'replace')}\n=== END", flush=True)
    for url in ("https://datacatalogapi.worldbank.org/ddhxext/DatasetView?dataset_unique_id=0067055",
                "https://datacatalogapi.worldbank.org/ddhxext/ResourceView?dataset_unique_id=0067055"):
        st, h, b = call(url, tries=1)
        print(f"CAT {st} {url}\n{b[:6000].decode('utf-8', 'replace')}\n=== END", flush=True)
    notes["indicators"] = len(ind)
    notes["areas"] = len(areas)
    print("NOTES " + json.dumps(notes))
    print("::notice title=itu_datahub probe::" + json.dumps(notes)[:3800])


if __name__ == "__main__":
    main()
