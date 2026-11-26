"""Read-only Dev probe 3 for #2870 (test branch only): the time fields of every row of the autumn DST day (100 periods)
and of the spring one, raw text as the API returns it. Two requests, 1.5 s apart, no writes."""

from __future__ import annotations

import json
import time

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"


def main() -> int:
    session = requests.Session()
    session.headers.update({"User-Agent": UA})
    out = {}
    for day in ("2025-10-26", "2025-03-30"):
        time.sleep(1.5)
        body = session.get("https://api.raporty.pse.pl/api/rce-pln", params={"$filter": f"business_date eq '{day}'"},
                           timeout=(20, 120)).content  # fmt: skip
        rows = json.loads(body, parse_float=str)["value"]
        out[day] = [[r["period"], r["dtime"], r["period_utc"], r["dtime_utc"]] for r in rows[4:16]]
        out[day + " all distinct dtime / dtime text lengths"] = [len({r["dtime"] for r in rows}),
                                                                sorted({len(r["dtime"]) for r in rows})]  # fmt: skip
    annotate("notice", "pse probe3 · DST day time fields", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
