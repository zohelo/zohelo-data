"""Read-only probe of GLEIF golden copy and MF flat-file endpoints. Deleted before merge."""

from datetime import UTC, datetime, timedelta
import json

import requests

from ingestion.common.report import annotate

UA = {"User-Agent": "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"}
out = {}
try:
    r = requests.get("https://goldencopy.gleif.org/api/v2/golden-copies/publishes/latest", headers=UA, timeout=60)
    out["gleif_status"] = r.status_code
    body = r.json()
    data = body.get("data", body)
    out["gleif_keys"] = list(data)[:20] if isinstance(data, dict) else type(data).__name__
    out["gleif_sample"] = json.dumps(data, default=str)[:2200]
except Exception as exc:
    out["gleif_error"] = repr(exc)[:300]
for days in range(0, 3):
    day = (datetime.now(UTC) - timedelta(days=days)).strftime("%Y%m%d")
    url = f"https://plikplaski.mf.gov.pl/pliki/{day}.7z"
    try:
        h = requests.head(url, headers=UA, timeout=60, allow_redirects=True)
        out[f"mf_{day}"] = [h.status_code, h.headers.get("Content-Length"), h.headers.get("Last-Modified")]
    except Exception as exc:
        out[f"mf_{day}"] = repr(exc)[:200]
annotate("notice", "next sources probe", out)
