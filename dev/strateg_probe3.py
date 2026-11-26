"""Read-only probe 3 of GUS STRATEG (PLS-003, #2430): the SOR strategy (id 78) indicator list and the real-data
answers of a few of its indicator dimensions at level 0 (EU, countries), level 2 (voivodeships) and Poland by
territory code; then the levels and years of every SOR indicator. No writes. Deleted before merge.

Sequential GET requests, one at a time, 1.5 s pause; at most 92 calls (anonymous quota 100 per 15 min, spec
strateg_en.json). Bodies to the log, compact summary to annotations.
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


def get(path: str, keep: int = 30000):
    if CALLS["n"] >= 92:
        return None, None
    for attempt in range(3):
        time.sleep(1.5)
        CALLS["n"] += 1
        try:
            r = S.get(BASE + path, timeout=(30, 120), headers={"Accept": "application/json"})
        except requests.RequestException as exc:
            print("error", path, repr(exc)[:300], flush=True)
            time.sleep(5 * (attempt + 1))
            continue
        print(f"GET {path} -> {r.status_code} {len(r.content)}B {dict(r.headers)}", flush=True)
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
        print("BODY", path, json.dumps(body, ensure_ascii=False)[:keep], flush=True)
        return r.status_code, body
    return None, None


def chunks(title: str, value, n: int = 3) -> None:
    blob = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    for i in range(0, min(len(blob), 3800 * n), 3800):
        annotate("notice", f"{title} {i // 3800}", blob[i:i + 3800])


def reach() -> dict:
    """DNS and TCP connect to each address of the hosts (connect timeouts of run 45745965411)."""
    import socket

    out = {}
    for host in ("strateg.stat.gov.pl", "api-strateg.stat.gov.pl", "api.stat.gov.pl", "bdl.stat.gov.pl"):
        try:
            infos = socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP)
        except OSError as exc:
            out[host] = f"dns {exc}"
            continue
        res = {}
        for fam, _, _, _, addr in infos:
            s = socket.socket(fam, socket.SOCK_STREAM)
            s.settimeout(20)
            t = time.monotonic()
            try:
                s.connect(addr)
                res[addr[0]] = f"ok {time.monotonic() - t:.2f}s"
            except OSError as exc:
                res[addr[0]] = f"{type(exc).__name__} {time.monotonic() - t:.1f}s"
            finally:
                s.close()
        out[host] = res
    annotate("notice", "strateg reach", out)
    return out


def main(args: list[str]) -> int:
    reach()
    _, body = get("strategies-programmes/78", keep=400000)
    rows = []
    for obj in body or []:
        for ind in obj.get("indicators", []):
            rows.append({**ind, **{k: v for k, v in obj.items() if k != "indicators"}})
    dims = sorted({(r["indicator_id"], r["dimension_id"]) for r in rows})
    inds = sorted({r["indicator_id"] for r in rows})
    annotate("notice", "strateg SOR list", {
        "rows": len(rows), "indicators": len(inds), "indicator_dimensions": len(dims),
        "territories": collections.Counter(r.get("territory_code") for r in rows).most_common(8),
        "units": collections.Counter(r.get("unit") for r in rows).most_common(12),
        "keys": sorted({k for r in rows for k in r}),
        "objectives": sorted({r.get("objective_1", "") for r in rows})[:12],
        "with_target": sum(1 for r in rows if r.get("target_value")),
        "with_base": sum(1 for r in rows if r.get("base_value")),
    })
    _, en = get("strategies-programmes/78?lang=en", keep=3000)
    data = {"en_head": json.dumps(en, ensure_ascii=False)[:400]}
    picks = [dims[0], dims[len(dims) // 2], dims[-1]] if dims else []
    for ind, dim in picks:
        _, d = get(f"indicators/{ind}/dimensions")
        data[f"{ind}/dims"] = json.dumps(d, ensure_ascii=False)[:500]
        for q in ("level_id=0", "level_id=2", "territory_code=100000000000", "level_id=0&lang=en"):
            st, v = get(f"indicators/{ind}/dimensions/{dim}?{q}")
            data[f"{ind}/{dim}?{q}"] = {"st": st, "len": len(json.dumps(v)) if v is not None else None,
                                        "head": json.dumps(v, ensure_ascii=False)[:700]}
    chunks("strateg data", data, 4)
    levels = {}
    for ind in inds:
        _, d = get(f"indicators/{ind}", keep=3000)
        if d is None:
            break
        if isinstance(d, dict):
            ys = d.get("years") or []
            levels[ind] = [sorted((d.get("territory_levels") or {}).keys()), min(ys) if ys else None,
                           max(ys) if ys else None, len(d.get("genders") or {}), len(d.get("places") or {}),
                           len(d.get("sections") or {})]
    annotate("notice", "strateg level counts", {
        "indicators_read": len(levels), "of": len(inds),
        "levels": collections.Counter(",".join(v[0]) for v in levels.values()).most_common(10),
        "multi_breakdown": sum(1 for v in levels.values() if max(v[3:]) > 1),
        "year_min": min((v[1] for v in levels.values() if v[1]), default=None),
        "year_max": max((v[2] for v in levels.values() if v[2]), default=None)})
    chunks("strateg levels", levels, 3)
    print("calls", CALLS["n"], flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
