"""Read-only probe of the ENTSOG Transparency Platform API for GAZ-SYSTEM (PL-TSO-0002). No R2 writes.

A handful of requests, one at a time, 6 s apart: two identical monthly answers (are the bytes stable?), the
oldest and the current month, and the point/operator reference endpoints. Output: annotations + files in
DEV_OUTPUT_DIR (uploaded as the run artifact).
"""

from __future__ import annotations

from collections import Counter
from hashlib import sha256
import json
import os
from pathlib import Path
import time

import requests

from ingestion.common.report import annotate

API = "https://transparency.entsog.eu/api/v1"
UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
OUT = Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output"))
S = requests.Session()
S.headers.update({"User-Agent": UA, "Accept": "application/json"})


def get(name: str, path: str):
    time.sleep(6)
    t = time.monotonic()
    r = S.get(f"{API}/{path}", timeout=90)
    took = round(time.monotonic() - t, 1)
    (OUT / f"{name}.json").write_bytes(r.content)
    info = {"name": name, "status": r.status_code, "bytes": len(r.content), "s": took,
            "sha": sha256(r.content).hexdigest()[:16], "ctype": r.headers.get("Content-Type"),
            "last_modified": r.headers.get("Last-Modified"), "etag": r.headers.get("ETag")}
    try:
        body = r.json()
    except ValueError:
        body = None
        info["text"] = r.text[:300]
    return info, body


def flows(body):
    rows = body.get("operationaldatas") or []
    keys = Counter(k for r in rows for k in r)
    out = {
        "meta": body.get("meta"),
        "rows": len(rows),
        "fields": sorted(keys),
        "flowStatus": Counter(str(r.get("flowStatus")) for r in rows),
        "isNA": Counter(str(r.get("isNA")) for r in rows),
        "unit": Counter(str(r.get("unit")) for r in rows),
        "periodType": Counter(str(r.get("periodType")) for r in rows),
        "indicator": Counter(str(r.get("indicator")) for r in rows),
        "value_types": Counter(type(r.get("value")).__name__ for r in rows),
        "dup_keys": sum(c - 1 for c in Counter((r.get("pointKey"), r.get("directionKey"), r.get("periodFrom")) for r in rows).values() if c > 1),
        "series": len({(r.get("pointKey"), r.get("directionKey")) for r in rows}),
        "periodFrom_min": min((r.get("periodFrom") or "" for r in rows), default=None),
        "periodFrom_max": max((r.get("periodFrom") or "" for r in rows), default=None),
        "lastUpdate_max": max((r.get("lastUpdateDateTime") or "" for r in rows), default=None),
        "from_hours": Counter((r.get("periodFrom") or "")[11:] for r in rows),
    }
    return out


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    q = "operationaldatas?operatorKey=PL-TSO-0002&indicator=Physical%20Flow&periodType=day&timezone=WET&limit=-1"
    a, oct1 = get("oct_a", f"{q}&from=2026-10-01&to=2026-10-31")
    b, oct2 = get("oct_b", f"{q}&from=2026-10-01&to=2026-10-31")
    annotate("notice", "probe oct twice", {"a": a, "b": b, "same_bytes": a["sha"] == b["sha"],
                                           "same_rows": oct1.get("operationaldatas") == oct2.get("operationaldatas")})
    annotate("notice", "probe oct profile", flows(oct1))
    rows = oct1["operationaldatas"]
    annotate("notice", "probe oct sample rows", rows[:2] + [r for r in rows if str(r.get("isNA")).lower() in ("1", "true")][:1])
    series = sorted({(r["pointKey"], r["directionKey"], r.get("pointLabel"), r.get("isNA") in (1, True, "1", "true")) for r in rows if r.get("periodFrom", "").startswith("2026-10-15")})
    annotate("notice", "probe oct series 15 Oct", [f"{p}|{d}|{l}|{'NA' if n else ''}" for p, d, l, n in series])
    c, jan21 = get("jan21", f"{q}&from=2021-01-01&to=2021-01-31")
    d, nov = get("nov26", f"{q}&from=2026-11-01&to=2026-11-30")
    e, dec20 = get("dec20", f"{q}&from=2020-12-01&to=2020-12-31")
    annotate("notice", "probe months", {"jan21": c, "jan21_p": flows(jan21) if jan21 else None, "nov26": d,
                                        "nov26_p": flows(nov) if nov else None, "dec20": e, "dec20_body": str(dec20)[:300]})
    f, opd = get("opd", "operatorpointdirections?operatorKey=PL-TSO-0002&limit=-1")
    recs = (opd or {}).get("operatorpointdirections") or []
    annotate("notice", "probe operatorpointdirections", {"info": f, "meta": (opd or {}).get("meta"), "rows": len(recs),
             "fields": sorted({k for r in recs for k in r}), "sample": recs[:1]})
    keep = ("pointKey", "directionKey", "pointLabel", "pointType", "tSOCountry", "adjacentCountry", "adjacentOperatorKey",
            "crossBorderPointType", "isInterconnection", "isImport", "isPlanned", "validFrom", "validTo", "hasData", "isCrossBorder", "euRelationship")
    lines = ["|".join(str(r.get(k)) for k in keep) for r in recs]
    for i in range(0, len(lines), 40):
        annotate("notice", f"probe opd lines {i}", {"cols": keep, "lines": lines[i:i + 40]})
    g, ic = get("ic", "interconnections?fromOperatorKey=PL-TSO-0002&limit=-1")
    recs = (ic or {}).get("interconnections") or []
    annotate("notice", "probe interconnections", {"info": g, "rows": len(recs), "fields": sorted({k for r in recs for k in r}), "sample": recs[:1]})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
