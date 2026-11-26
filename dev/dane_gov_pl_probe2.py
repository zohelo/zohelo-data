"""Second probe for #3263: field selection, scroll full pass, distribution layout. Read-only, no R2 writes."""

from __future__ import annotations

from collections import Counter
import json
import re
import time

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
BASE = "https://data.europa.eu/api/hub/search"
FACETS = json.dumps({"catalog": ["dane-gov-pl"]})
S = requests.Session()
S.headers.update({"User-Agent": UA, "Accept": "application/json"})
FIELDS = (
    "id,identifier,title.pl,title.en,publisher.name,publisher.type,publisher.resource,modified,issued,"
    "accrual_periodicity,categories.id,is_hvd,landing_page,catalog_record,country.id,keywords.label,"
    "distributions.id,distributions.format,distributions.media_type,distributions.byte_size,distributions.modified,"
    "distributions.issued,distributions.access_url,distributions.download_url,distributions.status,"
    "distributions.title.pl,distributions.license"
)


def get(path, **params):
    time.sleep(1.1)
    t = time.monotonic()
    r = S.get(f"{BASE}{path}", params=params, timeout=(20, 180))
    return r, round(time.monotonic() - t, 2)


def strip(v):
    """Drop multilingual dicts down to pl/en for printing."""
    if isinstance(v, dict):
        if len(v) > 5 and all(len(k) == 2 for k in v):
            return {k: v[k] for k in ("pl", "en") if k in v}
        return {k: strip(x) for k, x in v.items()}
    if isinstance(v, list):
        return [strip(x) for x in v]
    return v


def main():
    tests = {}
    base, s = get("/search", filter="dataset", facets=FACETS, limit=5, page=0, sort="id+asc")
    tests["plain"] = {"bytes": len(base.content), "s": s}
    for name in ("includes", "fields", "include"):
        r, s = get("/search", filter="dataset", facets=FACETS, limit=5, page=0, sort="id+asc", **{name: FIELDS})
        res = (r.json().get("result") or {}).get("results") or [] if r.ok else []
        tests[name] = {
            "http": r.status_code,
            "bytes": len(r.content),
            "s": s,
            "keys": sorted({k for i in res for k in i})[:30],
            "dkeys": sorted({k for i in res for d in (i.get("distributions") or []) for k in d})[:20],
            "err": r.text[:200] if not r.ok else None,
        }
    annotate("notice", "probe2 field selection", tests)
    print("SELECT", json.dumps(tests))
    chosen = next((n for n in ("includes", "fields", "include") if tests[n].get("http") == 200
                   and tests[n]["bytes"] < tests["plain"]["bytes"] / 3 and "distributions" in tests[n]["keys"]), None)
    sample = base.json()["result"]["results"]
    for i in sample[:2]:
        print("ITEM", json.dumps(strip({k: v for k, v in i.items() if k not in ("translation_meta", "description")}),
                                 ensure_ascii=False)[:12000])
    extra = {"includes": FIELDS} if chosen == "includes" else ({chosen: FIELDS} if chosen else {})

    t0 = time.monotonic()
    r, s = get("/search", filter="dataset", facets=FACETS, limit=1000, scroll="true", **extra)
    first = r.json()["result"]
    count, sid = first.get("count"), first.get("scrollId")
    pages, total_bytes, items = 1, len(r.content), list(first.get("results") or [])
    secs = [s]
    while sid:
        r, s = get("/scroll", scrollId=sid)
        secs.append(s)
        if not r.ok:
            print("SCROLL FAIL", r.status_code, r.text[:300])
            break
        body = r.json()["result"]
        res = body.get("results") or []
        pages += 1
        total_bytes += len(r.content)
        if not res:
            break
        items += res
        sid = body.get("scrollId")
        if pages > 60:
            break
    ids = [i.get("id") for i in items]
    nums = Counter()
    noid, pubnull, catnull, dists, maxd = [], [], [], 0, (0, None)
    dtypes, dfmt, dmedia, dstatus, dmod, dlic, dsize = Counter(), Counter(), Counter(), Counter(), Counter(), Counter(), Counter()
    ap, cr, kw = Counter(), [], Counter()
    for i in items:
        ident = i.get("identifier") or []
        m = re.search(r"dane\.gov\.pl/(?:[a-z]{2}/)?dataset/(\d+)", ident[0] if ident else "")
        m2 = re.search(r"^https-dane-gov-pl-(?:[a-z]{2}-)?dataset-(\d+)-", i.get("id") or "")
        n = m.group(1) if m else (m2.group(1) if m2 else None)
        if not m and len(noid) < 6:
            noid.append([i.get("id"), ident, strip(i.get("landing_page"))])
        if m and m2 and m.group(1) != m2.group(1):
            noid.append(["MISMATCH", i.get("id"), ident])
        nums[n] += 1
        p = i.get("publisher")
        if not (isinstance(p, dict) and p.get("name")) and len(pubnull) < 4:
            pubnull.append([i.get("id")[:60], strip(p)])
        c = i.get("categories")
        if (not c or any(not isinstance(x, dict) or not x.get("id") for x in c)) and len(catnull) < 4:
            catnull.append([i.get("id")[:60], strip(c)])
        a = i.get("accrual_periodicity")
        ap[(a or {}).get("resource", "").rsplit("/", 1)[-1] if isinstance(a, dict) else str(a)] += 1
        cr.append(((i.get("catalog_record") or {}).get("modified") or ""))
        for k in i.get("keywords") or []:
            kw["n"] += 1
        ds = i.get("distributions") or []
        dists += len(ds)
        if len(ds) > maxd[0]:
            maxd = (len(ds), i.get("id"))
        for d in ds:
            for k, v in d.items():
                dtypes[f"{k}:{type(v).__name__}"] += 1
            f = d.get("format")
            dfmt[(f.get("id") if isinstance(f, dict) else str(f))] += 1
            mt = d.get("media_type")
            dmedia[str(mt)[:40]] += 1
            st = d.get("status")
            dstatus[str(st)[:60]] += 1
            dmod[str(d.get("modified"))[:4]] += 1
            dlic[str(d.get("license"))[:80]] += 1
            bs = d.get("byte_size")
            dsize["null" if bs in (None, "") else ("num" if isinstance(bs, int | float) else type(bs).__name__)] += 1
    dup_nums = {k: v for k, v in nums.items() if v > 1}
    full = {
        "selection": chosen,
        "count": count,
        "pages": pages,
        "records": len(items),
        "distinct_ids": len(set(ids)),
        "distinct_numbers": len(nums),
        "dup_numbers": dict(list(dup_nums.items())[:8]),
        "dup_numbers_n": len(dup_nums),
        "bytes": total_bytes,
        "seconds": round(time.monotonic() - t0),
        "max_page_s": max(secs),
        "distributions": dists,
        "max_distributions": maxd,
        "catalog_record_modified_max": max(cr),
        "catalog_record_modified_min": min(c for c in cr if c) if any(cr) else None,
        "accrual": dict(ap),
    }
    annotate("notice", "probe2 scroll full pass", full)
    print("FULL", json.dumps(full, default=str))
    dist = {"types": dict(dtypes), "formats_n": len(dfmt), "media": dmedia.most_common(8),
            "status": dstatus.most_common(5), "modified_years": sorted(dmod.items()), "license": dlic.most_common(5),
            "byte_size": dict(dsize)}
    annotate("notice", "probe2 distributions", dist)
    print("DIST", json.dumps(dist, default=str))
    odd = {"no_identifier": noid, "publisher_missing": pubnull, "category_missing": catnull, "keywords": kw["n"]}
    annotate("notice", "probe2 odd records", odd)
    print("ODD", json.dumps(odd, ensure_ascii=False, default=str))
    for i in items[:2]:
        print("SEL_ITEM", json.dumps(strip(i), ensure_ascii=False)[:6000])
    ds = [d for i in items for d in (i.get("distributions") or [])][:3]
    print("SEL_DIST", json.dumps(strip(ds), ensure_ascii=False)[:5000])
    mon = Counter(str(i.get("modified"))[:7] for i in items)
    print("MONTHS", json.dumps(sorted(mon.items())))
    pubs = Counter((i.get("publisher") or {}).get("name") for i in items)
    print("PUBS", len(pubs), json.dumps(pubs.most_common(15), ensure_ascii=False))
    cats = Counter(c.get("id") for i in items for c in (i.get("categories") or []) if isinstance(c, dict))
    ncat = Counter(len(i.get("categories") or []) for i in items)
    annotate("notice", "probe2 shape", {"publishers": len(pubs), "categories": dict(cats), "categories_per_dataset": dict(ncat),
                                        "months": len(mon), "modified_max": max(str(i.get("modified")) for i in items)})


if __name__ == "__main__":
    main()
