"""Probe of the data.europa.eu search API for the dane-gov-pl catalogue (#3263). Read-only, no R2 writes."""

from __future__ import annotations

from collections import Counter
import json
import os
from pathlib import Path
import re
import time

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
BASE = "https://data.europa.eu/api/hub/search"
FACETS = json.dumps({"catalog": ["dane-gov-pl"]})
OUT = Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output"))
S = requests.Session()
S.headers.update({"User-Agent": UA, "Accept": "application/json"})


def get(path, **params):
    time.sleep(1.2)
    t = time.monotonic()
    r = S.get(f"{BASE}{path}", params=params, timeout=(20, 120))
    return r, round(time.monotonic() - t, 2)


def short(v, n=300):
    return json.dumps(v, ensure_ascii=False, default=str)[:n]


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    r, s = get("/catalogues/dane-gov-pl")
    cat = r.json() if r.ok else {}
    res = cat.get("result", cat) if isinstance(cat, dict) else {}
    info = {
        "http": r.status_code,
        "s": s,
        "keys": sorted(res)[:40] if isinstance(res, dict) else None,
        "modified": res.get("modified") if isinstance(res, dict) else None,
        "issued": res.get("issued") if isinstance(res, dict) else None,
        "count": res.get("count") if isinstance(res, dict) else None,
    }
    (OUT / "catalogue.json").write_bytes(r.content)
    print("CATALOGUE", r.status_code, r.text[:3000])
    annotate("notice", "probe catalogue", info)

    r, s = get("/search", filter="dataset", facets=FACETS, limit=100, page=0)
    body = r.json()
    (OUT / "page0.json").write_bytes(r.content)
    result = body.get("result", {})
    items = result.get("results", [])
    keys = Counter(k for i in items for k in i)
    print("TOP", sorted(body), "RESULT", sorted(result))
    print("ITEM0", json.dumps(items[0], ensure_ascii=False)[:20000] if items else None)
    print("ITEM1", json.dumps(items[1], ensure_ascii=False)[:20000] if len(items) > 1 else None)
    types = {k: sorted({type(i.get(k)).__name__ for i in items if k in i}) for k in keys}
    annotate(
        "notice",
        "probe page0",
        {
            "http": r.status_code,
            "s": s,
            "bytes": len(r.content),
            "top": sorted(body),
            "result": sorted(result),
            "count": result.get("count"),
            "n": len(items),
            "keys": dict(keys),
        },
    )
    annotate("notice", "probe page0 types", types)
    for k in sorted(keys):
        print("FIELD", k, short([i.get(k) for i in items[:3]], 1500))
    if items:
        annotate(
            "notice",
            "probe sample fields",
            {
                k: short(items[0].get(k), 250)
                for k in (
                    "id",
                    "identifier",
                    "publisher",
                    "categories",
                    "accrual_periodicity",
                    "is_hvd",
                    "modified",
                    "issued",
                    "catalog",
                    "country",
                )
            },
        )
        dkeys = Counter(k for i in items for x in (i.get("distributions") or []) for k in x)
        annotate(
            "notice",
            "probe distributions",
            {
                "keys": dict(dkeys),
                "first": short((items[0].get("distributions") or [])[:1], 1500),
                "per_dataset_max": max(len(i.get("distributions") or []) for i in items),
            },
        )
    annotate("notice", "probe facets", {"facets": short(result.get("facets"), 3000)})

    tests = {}
    for label, params in {
        "limit1000": {"limit": 1000, "page": 0},
        "page99": {"limit": 100, "page": 99},
        "page100": {"limit": 100, "page": 100},
        "sort_id": {"limit": 5, "page": 0, "sort": "id+asc"},
        "sort_id2": {"limit": 5, "page": 0, "sort": "id asc"},
        "scroll": {"limit": 500, "scroll": "true"},
    }.items():
        try:
            r, s = get("/search", filter="dataset", facets=FACETS, **params)
            b = r.json() if "json" in r.headers.get("Content-Type", "") else {}
            rs = b.get("result", {}) if isinstance(b, dict) else {}
            tests[label] = {
                "http": r.status_code,
                "s": s,
                "bytes": len(r.content),
                "n": len(rs.get("results", []) or []),
                "count": rs.get("count"),
                "rkeys": sorted(rs)[:12],
                "ids": [str(x.get("id", ""))[:60] for x in (rs.get("results") or [])[:3]],
                "err": r.text[:200] if not r.ok else None,
            }
            if label == "scroll" and rs.get("scrollId"):
                sid = rs["scrollId"]
                r2, s2 = get("/scroll", scrollId=sid)
                rs2 = (r2.json() if r2.ok else {}).get("result", {})
                tests["scroll_next"] = {
                    "http": r2.status_code,
                    "s": s2,
                    "n": len(rs2.get("results", []) or []),
                    "rkeys": sorted(rs2)[:12],
                    "same_id": rs2.get("scrollId") == sid,
                    "err": r2.text[:200] if not r2.ok else None,
                }
        except Exception as exc:  # probe: record and continue
            tests[label] = {"error": f"{type(exc).__name__}: {str(exc)[:200]}"}
    annotate("notice", "probe paging", tests)
    print("TESTS", json.dumps(tests))

    t0 = time.monotonic()
    ids, mods, total_bytes, pages, dist = [], [], 0, 0, 0
    publishers, cats, idforms, nonmatch, dfmt = Counter(), Counter(), Counter(), [], Counter()
    for page in range(0, 101):
        r, s = get("/search", filter="dataset", facets=FACETS, limit=100, page=page)
        if not r.ok:
            print("PAGE FAIL", page, r.status_code, r.text[:200])
            break
        total_bytes += len(r.content)
        pages += 1
        res = r.json()["result"].get("results") or []
        if not res:
            break
        for i in res:
            ids.append(i.get("id"))
            mods.append(str(i.get("modified")))
            p = i.get("publisher") or {}
            publishers[p.get("name") if isinstance(p, dict) else str(p)] += 1
            for c in i.get("categories") or []:
                cats[c.get("id") if isinstance(c, dict) else str(c)] += 1
            ident = i.get("identifier")
            idv = ident[0] if isinstance(ident, list) and ident else ident
            m = re.search(r"dane\.gov\.pl/(?:[a-z]{2}/)?dataset/(\d+)", str(idv))
            if m:
                idforms["identifier"] += 1
            else:
                m2 = re.search(r"dane-gov-pl-(?:[a-z]{2}-)?dataset-(\d+)", str(i.get("id")))
                idforms["id_only" if m2 else "none"] += 1
                if len(nonmatch) < 10:
                    nonmatch.append([str(i.get("id"))[:90], str(ident)[:120]])
            for x in i.get("distributions") or []:
                dist += 1
                f = x.get("format")
                dfmt[f.get("id") if isinstance(f, dict) else str(f)] += 1
    mods_sorted = sorted(m for m in mods if m and m != "None")
    full = {
        "pages": pages,
        "records": len(ids),
        "distinct": len(set(ids)),
        "bytes": total_bytes,
        "seconds": round(time.monotonic() - t0),
        "distributions": dist,
        "modified_min": mods_sorted[:3],
        "modified_max": mods_sorted[-3:],
        "modified_none": mods.count("None"),
        "publishers": len(publishers),
        "top_publishers": publishers.most_common(6),
        "categories": dict(cats),
        "idforms": dict(idforms),
        "nonmatch": nonmatch[:5],
        "formats": dfmt.most_common(12),
    }
    annotate("notice", "probe full pass", full)
    print("FULL", json.dumps(full, ensure_ascii=False, default=str))
    print("NONMATCH", json.dumps(nonmatch, ensure_ascii=False))
    mon = Counter(m[:7] for m in mods_sorted)
    print("MONTHS", json.dumps(sorted(mon.items())))
    annotate("notice", "probe months", {"months": len(mon), "first": sorted(mon.items())[:6], "last": sorted(mon.items())[-12:]})


if __name__ == "__main__":
    main()
