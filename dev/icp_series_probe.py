"""Read-only Dev probe for #2800: does ICP series 9100000 fail at per_page=50000 but work smaller?

No R2 access, no writes. One request at a time with a pause; each result is one annotation.
"""

from __future__ import annotations

import json
import time

import requests

from ingestion.common.report import annotate
from ingestion.to_landing.world_bank_icp.extract import API, SOURCE_ID, USER_AGENT

SERIES = "9100000"
HEALTHY = "1101150"
SIZES = (50000, 20000, 10000, 5000, 1000)
TIMEOUT = 600
MAX_FULL_PAGES = 40


def probe(session: requests.Session, series: str, per_page: int, page: int = 1) -> dict:
    url = f"{API}/sources/{SOURCE_ID}/country/all/series/{series}/time/all?format=json&per_page={per_page}&page={page}"
    out: dict = {"series": series, "per_page": per_page, "page": page}
    start = time.monotonic()
    try:
        r = session.get(url, timeout=TIMEOUT)
        body = r.content
        out.update(status=r.status_code, bytes=len(body), ctype=r.headers.get("Content-Type"))
        try:
            parsed = json.loads(body)
            if isinstance(parsed, dict):
                out.update(pages=parsed.get("pages"), total=parsed.get("total"), per_page_resp=parsed.get("per_page"))
                src = parsed.get("source")
                data = src.get("data") if isinstance(src, dict) else None
                out["rows"] = len(data) if isinstance(data, list) else None
            else:
                out["json_head"] = json.dumps(parsed)[:300]
        except ValueError:
            out["body_head"] = body[:300].decode("utf-8", "replace")
    except requests.RequestException as exc:
        out.update(status=None, error=f"{type(exc).__name__}: {str(exc)[:300]}")
    out["seconds"] = round(time.monotonic() - start, 1)
    time.sleep(2)
    return out


def main() -> int:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    results = []
    for size in SIZES:
        res = probe(session, SERIES, size)
        results.append(res)
        annotate("notice", f"icp probe {SERIES} per_page={size}", res)
    annotate("notice", f"icp probe healthy {HEALTHY} per_page=50000", probe(session, HEALTHY, 50000))
    # Full fetch at the largest size below 50000 whose page 1 worked: is the whole series reachable?
    ok = [r for r in results if r.get("status") == 200 and r.get("pages") and r["per_page"] < 50000]
    if ok:
        first = ok[0]
        pages = int(first["pages"])
        rows = []
        for page in range(2, min(pages, MAX_FULL_PAGES) + 1):
            rows.append(probe(session, SERIES, first["per_page"], page))
        good = [r for r in rows if r.get("status") == 200 and r.get("rows") is not None]
        annotate(
            "notice",
            f"icp probe {SERIES} all pages per_page={first['per_page']}",
            {
                "pages": pages,
                "fetched_pages_2_on": len(rows),
                "ok": len(good),
                "rows_total": (first.get("rows") or 0) + sum(r["rows"] for r in good),
                "max_seconds": max([r["seconds"] for r in rows] + [first["seconds"]]),
                "sum_seconds": round(sum(r["seconds"] for r in rows) + first["seconds"], 1),
                "bad": [r for r in rows if r not in good][:3],
            },
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
