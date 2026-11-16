"""Third read-only probe of the BZP read API (PLG-022, #2981): what PublicationDateTo bounds, paging to the end of a
day, and the HTML of the value lines of result notices.

GET only, one request at a time, 2.5 s apart (published limit 30 requests a minute), at most 40 requests. No names,
cities, ids, e-mails or phones are printed: from htmlBody only the markup and text of the value lines (template label
and amount).
"""

from __future__ import annotations

from collections import Counter
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request

UA = "zohelo-data/1.0 (+https://zohelo.com; public statistics platform)"
V1 = "https://ezamowienia.gov.pl/mo-board/api/v1/notice"
BUDGET = [40]


def get(path: str, params: dict):
    if BUDGET[0] <= 0:
        return None, "budget used"
    BUDGET[0] -= 1
    time.sleep(2.5)
    url = path + "?" + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=180) as r:
            return json.loads(r.read()), str(r.status)
    except urllib.error.HTTPError as e:
        return e.read(600).decode("utf-8", "replace"), f"HTTP {e.code}"
    except Exception as e:  # noqa: BLE001
        return None, f"{type(e).__name__}: {str(e)[:200]}"


def walk(kind: str, start: str, end: str, max_pages: int, size: int = 500) -> list:
    out, after = [], None
    for page in range(1, max_pages + 1):
        params = {"NoticeType": kind, "PublicationDateFrom": start, "PublicationDateTo": end, "PageSize": size}
        if after:
            params["SearchAfter"] = after
        doc, st = get(V1, params)
        items = doc if isinstance(doc, list) else []
        dates = sorted(i["publicationDate"] for i in items)
        print(f"  {kind} {start}..{end} page {page}: {st} n={len(items)} dates={dates[:1]}..{dates[-1:]}", flush=True)
        if not isinstance(doc, list):
            print("  ", str(doc)[:400])
            break
        out += items
        if len(items) < size:
            break
        after = items[-1]["objectId"]
    days = Counter(i["publicationDate"][:10] for i in out)
    hours = Counter(i["publicationDate"][11:13] for i in out)
    ids = [i["objectId"] for i in out]
    print(f"  total {len(out)} distinct {len(set(ids))} sorted {ids == sorted(ids)} days {sorted(days.items())} "
          f"hours {sorted(hours.items())}", flush=True)
    return out


def stats(start: str, end: str) -> dict:
    doc, st = get(V1 + "/stats", {"PublicationDateFrom": start, "PublicationDateTo": end})
    keep = {d["noticeType"]: d["numberOfNotices"] for d in doc if d["numberOfNotices"]} if isinstance(doc, list) else doc
    print(f"  stats {start}..{end}: {st} {json.dumps(keep)}", flush=True)
    return keep


VALUE_LABELS = re.compile(r"(4\.3\.\)|4\.3\.1\)|4\.5\.5\.\)|6\.1\.\)|6\.2\.\)|6\.3\.\)|6\.4\.\)|8\.1\.\)|8\.2\.\)|5\.1\.\)|4\.1\.5\.\)|4\.1\.6\.\)|4\.2\.5\.\))")


def html_values(items: list, n: int) -> None:
    shown = 0
    shapes: Counter = Counter()
    for it in items:
        body = it.get("htmlBody") or ""
        for m in VALUE_LABELS.finditer(body):
            snippet = body[max(0, m.start() - 60) : m.start() + 260]
            snippet = re.sub(r"\s+", " ", snippet)
            text = re.sub(r"<[^>]+>", "|", snippet)
            value = re.sub(r"\d", "9", text.split(":", 1)[1][:60]) if ":" in text else "?"
            shapes[(m.group(1), value[:40])] += 1
            if shown < n and m.group(1) in ("8.2.)", "6.4.)", "4.3.)", "4.5.5.)", "5.1.)", "8.1.)"):
                print("  HTML", snippet[:330])
                shown += 1
    for (label, value), count in shapes.most_common(80):
        print(f"  SHAPE {count:4d} {label} {value!r}")


def main() -> None:
    d = "2026-11-03"
    print("A full day, To = 23:59:59")
    a = walk("ContractNotice", f"{d}T00:00:00", f"{d}T23:59:59", 5)
    stats(f"{d}T00:00:00", f"{d}T23:59:59")
    print("B To = 10:00:00")
    walk("ContractNotice", f"{d}T00:00:00", f"{d}T10:00:00", 3)
    stats(f"{d}T00:00:00", f"{d}T10:00:00")
    print("C From = 10:00:00")
    walk("ContractNotice", f"{d}T10:00:00", f"{d}T23:59:59", 3)
    stats(f"{d}T10:00:00", f"{d}T23:59:59")
    print("C2 To = next day 00:00:00")
    walk("ContractNotice", f"{d}T00:00:00", "2026-11-04T00:00:00", 4)
    stats(f"{d}T00:00:00", "2026-11-04T00:00:00")
    print("D Sunday")
    walk("ContractNotice", "2026-11-08T00:00:00", "2026-11-08T23:59:59", 3)
    stats("2026-11-08T00:00:00", "2026-11-08T23:59:59")
    print("E Saturday + Sunday stats per day")
    stats("2026-11-07T00:00:00", "2026-11-07T23:59:59")
    print("F result notices, whole day")
    f = walk("TenderResultNotice", f"{d}T00:00:00", f"{d}T23:59:59", 5)
    html_values(f, 25)
    print("G contract notices, value lines")
    html_values(a[:300], 6)
    print("requests left", BUDGET[0])


if __name__ == "__main__":
    main()
