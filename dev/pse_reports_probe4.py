"""Read-only Dev probe 4 for #2870 (test branch only): on every DST day since 2024-06-14, the rows whose published
local end (dtime without 'a') differs from dtime_utc plus the Warsaw offset at the start of the period, for both
reports. Ten requests, 1.5 s apart, no writes."""

from __future__ import annotations

from datetime import datetime, timedelta
import json
import time
from zoneinfo import ZoneInfo

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
W = ZoneInfo("Europe/Warsaw")
UTC = ZoneInfo("UTC")


def main() -> int:
    session = requests.Session()
    session.headers.update({"User-Agent": UA})
    out = {}
    for report in ("rce-pln", "his-wlk-cal"):
        for day in ("2024-10-27", "2025-03-30", "2025-10-26", "2026-03-29", "2026-10-25"):
            time.sleep(1.5)
            body = session.get(f"https://api.raporty.pse.pl/api/{report}",
                               params={"$filter": f"business_date eq '{day}'"}, timeout=(20, 120)).content  # fmt: skip
            rows = json.loads(body, parse_float=str)["value"]
            bad = []
            for i, r in enumerate(rows):
                end = datetime.strptime(r["dtime_utc"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
                off = (end - timedelta(minutes=15)).astimezone(W).utcoffset()
                local = (end.replace(tzinfo=None) + off).strftime("%Y-%m-%d %H:%M:%S")
                if r["dtime"].replace("a", "") != local:
                    bad.append([i, r["period"], r["dtime"], r["dtime_utc"], local])
            out[f"{report} {day}"] = {"rows": len(rows), "mismatches": len(bad), "sample": bad[:4],
                                      "pub": sorted({r["publication_ts"] for r in rows})[:2]}  # fmt: skip
    annotate("notice", "pse probe4 · DST local labels", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
