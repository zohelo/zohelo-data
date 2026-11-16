"""Second read-only probe of the BZP read API (PLG-022, #2981): paging, day boundaries, volumes, value fields.

GET only, one request at a time, 2.5 s apart (published limit 30 requests a minute), at most 45 requests. Personal
data are never printed: no names, cities, national ids, e-mails or phones; from htmlBody only the template labels
of value lines and the numbers after them (lines with '@' or a phone label are skipped).
"""

from __future__ import annotations

from collections import Counter
import html as htmlmod
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request

UA = "zohelo-data/1.0 (+https://zohelo.com; public statistics platform)"
V1 = "https://ezamowienia.gov.pl/mo-board/api/v1/notice"
BUDGET = [45]


def get(path: str, params: dict) -> tuple[object, str, int, float]:
    if BUDGET[0] <= 0:
        return None, "budget used", 0, 0.0
    BUDGET[0] -= 1
    time.sleep(2.5)
    url = path + ("?" + urllib.parse.urlencode(params) if params else "")
    t = time.monotonic()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=180) as r:
            body = r.read()
            return json.loads(body), str(r.status), len(body), round(time.monotonic() - t, 1)
    except urllib.error.HTTPError as e:
        return e.read(600).decode("utf-8", "replace"), f"HTTP {e.code}", 0, round(time.monotonic() - t, 1)
    except Exception as e:  # noqa: BLE001
        return None, f"{type(e).__name__}: {str(e)[:200]}", 0, round(time.monotonic() - t, 1)


def notices(kind: str, start: str, end: str, size: int = 500, after: str | None = None):
    params = {"NoticeType": kind, "PublicationDateFrom": start, "PublicationDateTo": end, "PageSize": size}
    if after:
        params["SearchAfter"] = after
    doc, st, n, sec = get(V1, params)
    items = doc if isinstance(doc, list) else []
    print(f"\n--- {kind} {start}..{end} size={size} after={after}: {st} bytes={n} sec={sec} items={len(items)}")
    if not isinstance(doc, list):
        print(str(doc)[:600])
    return items


def summary(items: list) -> dict:
    if not items:
        return {}
    ids = [i["objectId"] for i in items]
    dates = [i["publicationDate"] for i in items]
    return {
        "n": len(items),
        "distinct_objectId": len(set(ids)),
        "distinct_noticeNumber": len({i["noticeNumber"] for i in items}),
        "distinct_bzpNumber": len({i["bzpNumber"] for i in items}),
        "objectId_sorted_asc": ids == sorted(ids),
        "pubdate_sorted_asc": dates == sorted(dates),
        "first_last_objectId": [ids[0], ids[-1]],
        "min_max_pubdate": [min(dates), max(dates)],
        "html_bytes": sum(len((i.get("htmlBody") or "").encode()) for i in items),
        "json_bytes_wo_html": sum(len(json.dumps({k: v for k, v in i.items() if k != "htmlBody"}).encode()) for i in items),
    }


def counts(items: list, key: str) -> list:
    return Counter(str(i.get(key)) for i in items).most_common(25)


LABEL = re.compile(r"^\s*(\d+(?:\.\d+)*\.\))\s*([^:]{0,140})")
VALUE = re.compile(r"(?i)(warto|cena|kwot|szacunk|budżet|budzet)")


def lines_of(body: str) -> list[str]:
    text = re.sub(r"(?i)<br\s*/?>|</(p|div|h\d|li|tr|td|span)>", "\n", body or "")
    text = htmlmod.unescape(re.sub(r"<[^>]+>", " ", text))
    return [re.sub(r"\s+", " ", t).strip() for t in text.split("\n") if t.strip()]


def html_report(items: list, label: str) -> None:
    labels: Counter = Counter()
    value_lines: Counter = Counter()
    samples = []
    for it in items:
        for line in lines_of(it.get("htmlBody") or ""):
            if "@" in line or re.search(r"(?i)telefon|e-mail|tel\.|faks|osob", line):
                continue
            m = LABEL.match(line)
            if m:
                labels[m.group(1) + " " + m.group(2).strip()[:90]] += 1
            if VALUE.search(line):
                lab = LABEL.match(line)
                key = (lab.group(1) + " " + lab.group(2).strip()[:110]) if lab else line[:110]
                value_lines[key] += 1
                if len(samples) < 40 and re.search(r"\d", line):
                    samples.append(re.sub(r"[A-Za-zÀ-ž]{3,}\s+[A-ZŁŚŻŹĆŃ][a-zà-ž]+\s+[A-ZŁŚŻŹĆŃ][a-zà-ž]+", "<x>", line)[:200])
    print(f"\n=== HTML {label}: section labels (count over {len(items)} notices)")
    for k, v in sorted(labels.items()):
        print(f"LABEL {v:4d} {k}")
    print(f"=== HTML {label}: value lines")
    for k, v in value_lines.most_common(60):
        print(f"VALUE {v:4d} {k}")
    for s in samples:
        print("SAMPLE", s)


def main() -> None:
    day = "2026-11-03"
    s = get(V1 + "/stats", {"PublicationDateFrom": f"{day}T00:00:00", "PublicationDateTo": f"{day}T23:59:59"})
    print("stats day", s[1], s[0])
    whole = {}
    for kind in ("ContractNotice", "TenderResultNotice"):
        items = notices(kind, f"{day}T00:00:00", f"{day}T23:59:59")
        whole[kind] = items
        print("summary", json.dumps(summary(items)))
        for key in ("clientType", "orderType", "tenderType", "isTenderAmountBelowEU", "organizationProvince",
                    "organizationCountry", "procedureResult", "submittingOffersDate"):
            if key == "submittingOffersDate":
                print("nonnull", key, sum(i.get(key) is not None for i in items))
            else:
                print("counts", key, counts(items, key))
        cont = [c for i in items for c in (i.get("contractors") or [])]
        print("contractors", len(cont), "keys", Counter(k for c in cont for k in c),
              "nonnull", {k: sum(c.get(k) not in (None, "") for c in cont) for k in ("contractorName", "contractorNationalId", "contractorProvince", "contractorCountry", "contractorCity")},
              "province", Counter(str(c.get("contractorProvince")) for c in cont).most_common(20),
              "per notice", Counter(len(i.get("contractors") or []) for i in items).most_common(8))
        print("noticeNumber suffix", Counter(i["noticeNumber"][-3:] for i in items).most_common(5),
              "objectId per bzpNumber max", max(Counter(i["bzpNumber"] for i in items).values()))
        print("cpv sample shape", [len((i.get("cpvCode") or "").split("),")) for i in items[:20]])
        html_report(items[:150], kind)
    # paging with SearchAfter and its order
    kind = "ContractNotice"
    p1 = notices(kind, f"{day}T00:00:00", f"{day}T23:59:59", size=100)
    p2 = notices(kind, f"{day}T00:00:00", f"{day}T23:59:59", size=100, after=p1[-1]["objectId"] if p1 else None)
    ids1, ids2 = [i["objectId"] for i in p1], [i["objectId"] for i in p2]
    full = [i["objectId"] for i in whole[kind]]
    print("paging: overlap", len(set(ids1) & set(ids2)), "p1+p2 == first 200 of full", ids1 + ids2 == full[:200],
          "p2 sorted", ids2 == sorted(ids2))
    # day boundary: last hour of the day in UTC and in Warsaw
    late = notices(kind, f"{day}T22:00:00", f"{day}T23:59:59")
    print("late summary", json.dumps({k: v for k, v in summary(late).items() if k.startswith(("n", "min"))}))
    nxt = notices(kind, "2026-11-04T00:00:00", "2026-11-04T00:59:59")
    print("next early summary", json.dumps({k: v for k, v in summary(nxt).items() if k.startswith(("n", "min"))}))
    # repeat request: identical answer?
    again = notices(kind, f"{day}T00:00:00", f"{day}T23:59:59")
    print("repeat identical", json.dumps(again, sort_keys=True) == json.dumps(whole[kind], sort_keys=True))
    # monthly volumes and history
    for frm, to in (("2021-01-01", "2021-12-31"), ("2024-01-01", "2024-12-31"), ("2025-01-01", "2025-12-31"),
                    ("2026-01-01", "2026-06-30"), ("2026-07-01", "2026-11-15"), ("2026-10-01", "2026-10-31")):
        doc, st, _, sec = get(V1 + "/stats", {"PublicationDateFrom": f"{frm}T00:00:00", "PublicationDateTo": f"{to}T23:59:59"})
        keep = {d["noticeType"]: d["numberOfNotices"] for d in doc} if isinstance(doc, list) else doc
        print("stats", frm, to, st, sec, json.dumps(keep))
    print("requests left", BUDGET[0])


if __name__ == "__main__":
    main()
