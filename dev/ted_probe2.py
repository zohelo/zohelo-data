"""Dev probe 2 for PLG-023 TED notices (test branch only). Read-only, no R2 writes.

1. data.europa.eu record of the TED CSV dataset: publisher, licence fields (dataset and distributions).
2. Each candidate field alone (limit 1, one Poland day): valid or not, the value's JSON shape.
3. One page of three notices with the chosen fields, printed whole.
"""

from __future__ import annotations

import json
import time

import requests

from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file

API = "https://api.ted.europa.eu/v3/notices/search"
HEAD = {"User-Agent": bulk_file.USER_AGENT, "Accept": "application/json"}
DAY = "buyer-country=POL AND publication-date=20261110"
CANDIDATES = [
    "publication-number", "notice-identifier", "notice-version", "notice-type", "form-type", "publication-date",
    "dispatch-date", "contract-nature", "procedure-type", "classification-cpv", "main-classification-proc",
    "notice-title", "title-proc", "buyer-country", "buyer-country-sub", "buyer-name", "buyer-legal-type",
    "buyer-category", "buyer-contracting-entity", "place-of-performance", "place-of-performance-country-proc",
    "place-of-performance-subdiv-proc", "place-of-performance-subdiv-lot", "estimated-value-proc",
    "estimated-value-cur-proc", "estimated-value-glo", "estimated-value-cur-glo", "total-value", "total-value-cur",
    "framework-value-notice", "framework-value-cur-notice", "result-value-notice", "result-value-cur-notice",
    "tender-value", "tender-value-cur", "winner-country", "ojs-number", "ojs-issue-number", "TED-identifier",
    "official-language", "deadline-receipt-tender-date-lot", "change-notice-version-identifier",
    "previous-notice-identifier", "modification-previous-notice-identifier", "BT-27-Procedure",
    "BT-27-Procedure-Currency", "BT-161-NoticeResult", "BT-161-NoticeResult-Currency", "notice-subtype",
]  # fmt: skip


def post(body: dict) -> tuple[int, object]:
    time.sleep(1.5)
    r = requests.post(API, json=body, headers=HEAD, timeout=(20, 120))
    try:
        return r.status_code, r.json()
    except ValueError:
        return r.status_code, r.text[:500]


def licence() -> None:
    time.sleep(1)
    r = requests.get("https://data.europa.eu/api/hub/search/datasets/ted-csv", headers=HEAD, timeout=(20, 60))
    res = r.json().get("result", {})
    dists = res.get("distributions") or []
    annotate(
        "notice",
        "ted probe2 data.europa.eu ted-csv",
        {
            "http": r.status_code,
            "title": (res.get("title") or {}).get("en"),
            "publisher": res.get("publisher"),
            "catalog": (res.get("catalog") or {}).get("id"),
            "licence_keys": {k: v for k, v in res.items() if "licen" in k.lower() or "rights" in k.lower()},
            "modified": res.get("modified"),
            "description": str((res.get("description") or {}).get("en"))[:600],
            "distributions": len(dists),
            "dist_licences": sorted({json.dumps(d.get("license"), sort_keys=True)[:200] for d in dists})[:5],
            "dist_titles": [str((d.get("title") or {}).get("en"))[:80] for d in dists[:8]],
        },
    )


def fields() -> list[str]:
    valid, shapes, invalid = [], {}, {}
    for f in CANDIDATES:
        status, res = post({"query": DAY, "fields": [f], "limit": 3, "page": 1})
        if status != 200:
            invalid[f] = str(res)[:80]
            continue
        valid.append(f)
        values = [n.get(f) for n in res.get("notices", [])]
        shapes[f] = json.dumps(values, ensure_ascii=False)[:160]
    annotate("notice", "ted probe2 fields valid", {"valid": valid, "invalid": invalid})
    annotate("notice", "ted probe2 field shapes", shapes)
    return valid


def page(valid: list[str]) -> None:
    status, res = post({"query": DAY, "fields": valid[:39], "limit": 2, "page": 2})
    out = {"http": status}
    if isinstance(res, dict):
        out["notices"] = [json.dumps(n, ensure_ascii=False)[:1800] for n in res.get("notices", [])]
    else:
        out["body"] = str(res)[:1000]
    annotate("notice", "ted probe2 page", out)
    status, res = post({"query": "buyer-country=POL AND publication-date=20261110 SORT BY publication-number DESC",
                        "fields": ["publication-number"], "limit": 3})  # fmt: skip
    annotate("notice", "ted probe2 sort", {"http": status, "body": str(res)[:300]})
    counts = {}
    for label, q in {
        "pol_2026_10": "buyer-country=POL AND publication-date>=20261001 AND publication-date<=20261031",
        "pol_buyer_not_place_2026_10": "buyer-country=POL AND NOT place-of-performance=POL AND "
        "publication-date>=20261001 AND publication-date<=20261031",
    }.items():
        status, res = post({"query": q, "fields": ["publication-number"], "limit": 1})
        counts[label] = res.get("totalNoticeCount") if isinstance(res, dict) else str(res)[:200]
    annotate("notice", "ted probe2 counts", counts)


def main() -> int:
    licence()
    valid = fields()
    page(valid)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
