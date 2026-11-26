"""Read-only probe 4 of GUS STRATEG (#2430): after trial 45754068310 got HTTP 404 on its 101st request
(indicators/54/dimensions/31?level_id=0) and then on strategies-programmes/78, is the 404 a missing resource or
the quota? A few GETs, 12 s apart, statuses and bodies to annotations. Args: API paths (default: the two above and
two controls). Deleted before merge.
"""

from __future__ import annotations

import json
import sys
import time

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
BASE = "https://strateg.stat.gov.pl/api/"
DEFAULT = ["strategies-programmes/78", "indicators/54", "indicators/54/dimensions", "indicators/54/dimensions/31?level_id=0",
           "metrics/54", "indicators/14/dimensions/5?level_id=0", "strategies-programmes/78"]


def main(args: list[str]) -> int:
    out = []
    for path in args or DEFAULT:
        time.sleep(12)
        try:
            r = requests.get(BASE + path, headers={"User-Agent": UA, "Accept": "application/json"}, timeout=(30, 120))
            out.append([time.strftime("%H:%M:%S"), path, r.status_code, len(r.content), r.text[:300],
                        {k: v for k, v in r.headers.items() if k.lower() not in ("date", "content-length")}])
        except requests.RequestException as exc:
            out.append([time.strftime("%H:%M:%S"), path, type(exc).__name__])
        print(json.dumps(out[-1], ensure_ascii=False), flush=True)
    annotate("notice", "strateg probe 4", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
