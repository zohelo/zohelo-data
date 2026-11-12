"""Dev trial for #2870 (PSE Reports API v2, test branch only). Mode `probe`: read-only, about 14 requests, 1 s apart."""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime, timedelta
from hashlib import sha256
import sys
import time

import requests

from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file

API = "https://api.raporty.pse.pl/api"


def get(entity: str, day: str) -> tuple[requests.Response, float]:
    time.sleep(1.1)
    started = time.monotonic()
    r = requests.get(
        f"{API}/{entity}",
        params={"$filter": f"business_date eq '{day}'"},
        headers={"User-Agent": bulk_file.USER_AGENT, "Accept-Encoding": "identity"},
        timeout=(30, 120),
    )
    return r, round(time.monotonic() - started, 2)


def profile(entity: str, day: str) -> dict:
    r, secs = get(entity, day)
    out: dict = {"http": r.status_code, "secs": secs, "bytes": len(r.content)}
    out["sha"] = sha256(r.content).hexdigest()[:12]
    out["headers"] = {k: r.headers.get(k) for k in ("Content-Type", "Last-Modified", "ETag", "Cache-Control", "Date")}
    try:
        body = r.json()
    except ValueError:
        out["text"] = r.text[:300]
        return out
    rows = body.get("value") or []
    out["top_keys"] = sorted(body)
    out["rows"] = len(rows)
    if not rows:
        return out
    keys = sorted({k for row in rows for k in row})
    out["keys"] = keys
    out["types"] = {k: dict(Counter(type(row.get(k)).__name__ for row in rows)) for k in keys}
    out["distinct_period"] = len({row.get("period") for row in rows})
    out["distinct_dtime"] = len({row.get("dtime") for row in rows})
    out["distinct_dtime_utc"] = len({row.get("dtime_utc") for row in rows})
    pubs = sorted({str(row.get("publication_ts_utc") or row.get("publication_ts")) for row in rows})
    out["publication"] = [len(pubs), pubs[0], pubs[-1]]
    out["first"] = rows[0]
    out["last"] = rows[-1]
    if entity == "rce-pln" or day.endswith("10-26") or day.endswith("03-30"):
        out["periods_sample"] = [(row.get("dtime"), row.get("dtime_utc"), row.get("period")) for row in rows[6:14]]
    return out


def probe() -> int:
    today = datetime.now(UTC).date()
    days = ["2024-06-14", "2025-03-30", "2025-10-26", str(today - timedelta(days=1)), str(today)]
    for entity in ("his-wlk-cal", "rce-pln"):
        result = {day: profile(entity, day) for day in days}
        if entity == "his-wlk-cal":
            result["2024-06-13"] = profile(entity, "2024-06-13")
        again = profile(entity, "2025-10-26")
        result["repeat_same_sha"] = again.get("sha") == result["2025-10-26"].get("sha")
        for group in (days[:2], days[2:]):
            payload = {d: {k: v for k, v in result[d].items() if k not in ("first", "last")} for d in group}
            for d in group[1:]:  # keys and headers once per annotation
                for k in ("keys", "headers", "top_keys"):
                    if payload[d].get(k) == payload[group[0]].get(k):
                        payload[d].pop(k, None)
            annotate("notice", f"pse probe {entity} {group[0]}..", payload)
        annotate(
            "notice",
            f"pse probe {entity} rows",
            {
                "first_2025_10_26": result["2025-10-26"].get("first"),
                "last_yesterday": result[days[3]].get("last"),
                "today": {k: result[days[4]].get(k) for k in ("rows", "publication")},
                "repeat_same_sha": result["repeat_same_sha"],
                "before_start": {k: result.get("2024-06-13", {}).get(k) for k in ("rows", "bytes")},
            },
        )
    return 0


def main() -> int:
    mode = sys.argv[1] if len(sys.argv) > 1 else "probe"
    if mode == "probe":
        return probe()
    raise SystemExit(f"unknown mode {mode}")


if __name__ == "__main__":
    raise SystemExit(main())
