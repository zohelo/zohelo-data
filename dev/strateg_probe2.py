"""Read-only probe 2 of GUS STRATEG (PLS-003): the SOR strategy (id 78) indicator list, metadata of its indicators and
real data of a few indicator dimensions at level 0 (EU, countries) and 2 (voivodeships). No writes.

Sequential GET requests, one at a time, 1.2 s pause; at most 95 calls (anonymous quota 100 per 15 min, spec
strateg_en.json). Full bodies to the log, compact summary to annotations.
"""

from __future__ import annotations

import collections
import json
import sys
import time

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
S = requests.Session()
S.headers["User-Agent"] = UA
BASE = "https://strateg.stat.gov.pl/api/"
CALLS = {"n": 0}


def get(path: str):
    if CALLS["n"] >= 95:
        return None, None
    for attempt in range(3):
        time.sleep(1.2)
        CALLS["n"] += 1
        try:
            r = S.get(BASE + path, timeout=(15, 120), headers={"Accept": "application/json"})
        except requests.RequestException as exc:
            print("error", path, repr(exc)[:300], flush=True)
            time.sleep(5 * (attempt + 1))
            continue
        hdr = {k: v for k, v in r.headers.items() if k.lower() in ("last-modified", "etag", "cache-control")
               or "limit" in k.lower()}
        print(f"GET {path} -> {r.status_code} {len(r.content)}B {hdr}", flush=True)
        if r.status_code == 429:
            time.sleep(60)
            continue
        if r.status_code >= 500:
            time.sleep(5 * (attempt + 1))
            continue
        try:
            body = r.json()
        except ValueError:
            body = r.text[:500]
        print("BODY", path, json.dumps(body, ensure_ascii=False)[:400000], flush=True)
        return r.status_code, body
    return None, None


def main(args: list[str]) -> int:
    strategy = args[0] if args else "78"
    st, body = get(f"strategies-programmes/{strategy}")
    rows = []
    for obj in body or []:
        for ind in obj.get("indicators", []):
            rows.append({**ind, **{k: v for k, v in obj.items() if k != "indicators"}})
    inds = sorted({r["indicator_id"] for r in rows})
    dims = sorted({(r["indicator_id"], r["dimension_id"]) for r in rows})
    summary = {
        "rows": len(rows), "indicators": len(inds), "indicator_dimensions": len(dims),
        "territories": collections.Counter(r.get("territory_code") for r in rows).most_common(10),
        "units": collections.Counter(r.get("unit") for r in rows).most_common(15),
        "objective_keys": sorted({k for r in rows for k in r if k.startswith("objective")}),
        "keys": sorted({k for r in rows for k in r}),
    }
    annotate("notice", f"strateg strategy {strategy}", summary)
    levels = {}
    for ind in inds[:80]:
        _, d = get(f"indicators/{ind}")
        if isinstance(d, dict):
            ys = d.get("years") or []
            levels[ind] = [sorted((d.get("territory_levels") or {}).keys()), min(ys) if ys else None,
                           max(ys) if ys else None, len(d.get("genders") or {}), len(d.get("places") or {}),
                           len(d.get("sections") or {})]
    blob = json.dumps(levels, separators=(",", ":"))
    for i in range(0, min(len(blob), 3800 * 3), 3800):
        annotate("notice", f"strateg levels {i // 3800}", blob[i:i + 3800])
    data = {}
    for ind, dim in dims[:3]:
        get(f"metrics/{ind}")
        get(f"indicators/{ind}/dimensions")
        for lvl in ("0", "2"):
            st2, v = get(f"indicators/{ind}/dimensions/{dim}?level_id={lvl}")
            data[f"{ind}/{dim}/{lvl}"] = {"st": st2, "len": len(json.dumps(v)) if v is not None else None,
                                         "head": json.dumps(v, ensure_ascii=False)[:600]}
    blob = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    for i in range(0, min(len(blob), 3800 * 3), 3800):
        annotate("notice", f"strateg data {i // 3800}", blob[i:i + 3800])
    print("calls", CALLS["n"], flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
