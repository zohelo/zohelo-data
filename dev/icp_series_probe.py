"""Read-only Dev probe for #2800 (second run): how long do World Bank 502s last after the big ICP series?

Run 1 (43287012577): series 9100000 at per_page=50000 answered 200 in 182 s (8.4 MB, 24,633 rows, 1 page);
every request after it, including a healthy series, got an instant 502 HTML page. This run measures that:
healthy series first, then 9100000, then the healthy series once a minute until 200 (at most 20 tries),
then 9100000 at per_page=10000. No R2 access, no writes; one request at a time.
"""

from __future__ import annotations

import json
import time

import requests

from ingestion.common.report import annotate
from ingestion.to_landing.world_bank_icp.extract import API, SOURCE_ID, USER_AGENT

SERIES = "9100000"
HEALTHY = "1101150"
TIMEOUT = 600


def probe(session: requests.Session, series: str, per_page: int) -> dict:
    url = f"{API}/sources/{SOURCE_ID}/country/all/series/{series}/time/all?format=json&per_page={per_page}&page=1"
    out: dict = {"series": series, "per_page": per_page}
    start = time.monotonic()
    try:
        r = session.get(url, timeout=TIMEOUT)
        out.update(status=r.status_code, bytes=len(r.content))
        try:
            parsed = json.loads(r.content)
            if isinstance(parsed, dict):
                out.update(pages=parsed.get("pages"), total=parsed.get("total"))
        except ValueError:
            out["html"] = True
            out["server"] = r.headers.get("Server")
    except requests.RequestException as exc:
        out.update(status=None, error=f"{type(exc).__name__}: {str(exc)[:200]}")
    out["seconds"] = round(time.monotonic() - start, 1)
    return out


def main() -> int:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    t0 = time.monotonic()
    annotate("notice", "icp probe2 healthy before", probe(session, HEALTHY, 50000))
    time.sleep(5)
    annotate("notice", "icp probe2 9100000 per_page=50000", probe(session, SERIES, 50000))
    after = time.monotonic()
    tries = []
    for _ in range(20):
        time.sleep(60)
        res = probe(session, HEALTHY, 50000)
        tries.append({"t": round(time.monotonic() - after), "s": res["status"], "sec": res["seconds"]})
        if res["status"] == 200:
            break
    annotate("notice", "icp probe2 healthy after big series, once a minute", {"tries": tries})
    time.sleep(5)
    annotate("notice", "icp probe2 9100000 per_page=10000", probe(session, SERIES, 10000))
    annotate("notice", "icp probe2 total", {"minutes": round((time.monotonic() - t0) / 60, 1)})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
