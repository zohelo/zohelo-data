"""Read-only probe 3 (CD-006, CD-007) with the real reader (ingestion/common/report_lists.py): three whole KNF days
chosen for their report types (annual and half-year deadlines, a current day) and one GPW pass of 365 days. List
pages only, one request at a time, 5 seconds apart. Prints counts to the job log; nothing is written."""

from collections import Counter
from datetime import date, datetime, timedelta
import json

from ingestion.common import report_lists as lists


def main():
    reader = lists.Reader()
    for day in (date(2026, 4, 30), date(2026, 9, 30), date(2026, 11, 12)):
        try:
            listed, rows, _ = lists.read_knf_day(reader, day)
        except Exception as exc:  # noqa: BLE001
            print(json.dumps({"knf_day": str(day), "error": f"{type(exc).__name__}: {exc}"[:400]}), flush=True)
            continue
        prefix = Counter(r["title"].split(",")[0].strip()[:30] if "," in r["title"] else "<no comma>" for r in rows)
        print(json.dumps({"knf_day": str(day), "listed": listed, "rows": len(rows), "requests": reader.requests,
                          "issuers": len({r["issuer_name"] for r in rows}), "prefixes": prefix.most_common(60),
                          "max_title": max(len(r["title"]) for r in rows), "empty_issuer": sum(not r["issuer_name"] for r in rows),
                          "doc_bytes": len(lists.document("knf_espi", day, listed, rows))}, ensure_ascii=False), flush=True)
    reader = lists.Reader()
    today = datetime.now(lists.WARSAW).date()
    try:
        by_day, covered, other = lists.read_gpw_since(reader, today - timedelta(days=365))
        rows = [r for d in by_day.values() for r in d]
        print(json.dumps({"gpw": "365 days", "requests": reader.requests, "covered_from": str(covered), "other_dropped": other,
                          "days": len(by_day), "rows": len(rows), "first": str(min(by_day, default=None)), "last": str(max(by_day, default=None)),
                          "categories": Counter(r["category"] for r in rows).most_common(), "isin_empty": sum(not r["isin"] for r in rows),
                          "issuers": len({r["issuer_name"] for r in rows}), "isin_prefix": Counter(r["isin"][:2] for r in rows).most_common(8),
                          "per_month": sorted(Counter(str(d)[:7] for d in by_day for _ in by_day[d]).items()),
                          "numbers_sample": [r["report_number"] for r in rows[:8]],
                          "issuer_sample": sorted({r["issuer_name"] for r in rows})[:25]}, ensure_ascii=False), flush=True)
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"gpw": "error", "error": f"{type(exc).__name__}: {exc}"[:600], "requests": reader.requests}), flush=True)


if __name__ == "__main__":
    main()
