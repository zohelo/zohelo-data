"""Dry run of NBP Landing + Bronze decode against the real API; writes nothing."""
from collections import Counter
from datetime import datetime, timezone
from hashlib import sha256
import time

import requests

from ingestion.common.report import annotate
from ingestion.to_bronze.nbp.load import decode
from ingestion.to_landing.nbp.extract import PAUSE_SECONDS, USER_AGENT, expected, fetch, version_of

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
stats, errors, started = Counter(), [], time.monotonic()
per_table = Counter()
for item in expected(datetime.now(timezone.utc).date()):
    time.sleep(PAUSE_SECONDS)
    try:
        status, body, _ = fetch(session, item["url"])
        if status == 404:
            stats["no_data"] += 1
            continue
        receipt = {"table": item["table"], "entity": item["entity"], "sha256": sha256(body).hexdigest(),
                   "version_token": version_of(item["table"], body), "ingested_at": "20261013T000000Z"}
        rows = decode(body, receipt).num_rows
        stats["files"] += 1
        stats["bytes"] += len(body)
        stats["rows"] += rows
        per_table[item["table"]] += rows
    except Exception as exc:
        errors.append(f"{item['entity']}: {type(exc).__name__}: {exc}"[:200])
annotate("notice" if not errors else "warning", "nbp dry run", {
    **stats, "rows_by_table": dict(per_table), "errors": len(errors), "error_sample": errors[:8],
    "seconds": round(time.monotonic() - started)})
raise SystemExit(1 if errors else 0)
