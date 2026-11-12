"""Read-only probe of the NFZ pharmacy-refund API (PLS-013, #2930) before the build.

GET only, one request at a time, 1.1 s apart, at most 70 requests; robots.txt (HTTP 404: none) honoured.
Settles: /terms text, field definitions, /available-periods, the page limit, dateFrom/dateTo, aggregationType
default, province and ATC filters (and whether a wrong filter is refused or silently ignored), the partition of the
national monthly rows by province and by ATC group, and the shape of a small province x ATC selection.
"""

from __future__ import annotations

import html
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
BASE = "https://api.nfz.gov.pl/app-stat-api-ra"
LEFT = [70]
MONTH = {"dateFrom": "2024-12-01", "dateTo": "2024-12-31"}
FIELDS = ("quantity", "refund", "donation", "contribution-of-patient", "value")


def get(path: str, params: dict | None = None) -> tuple[int, bytes]:
    if LEFT[0] <= 0:
        raise SystemExit("request budget used")
    LEFT[0] -= 1
    query = urllib.parse.urlencode(params or {}, quote_via=urllib.parse.quote)
    url = f"{BASE}/{path}" + (f"?{query}" if query else "")
    time.sleep(1.1)
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json, text/html"})
    try:
        with urllib.request.urlopen(req, timeout=90) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read(800)
    except Exception as e:  # noqa: BLE001
        return -1, f"{type(e).__name__}: {e}".encode()


def api(path: str, **params) -> tuple[int, dict | list | None, bytes]:
    status, body = get(path, {**params, "format": "json", "api-version": "1.0"})
    try:
        return status, json.loads(body), body
    except ValueError:
        return status, None, body


def text_of(body: bytes) -> str:
    t = re.sub(r"(?s)<(script|style)[^>]*>.*?</\1>", " ", body.decode("utf-8", "replace"))
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", t))).strip()


def rows(doc) -> list[dict]:
    return (doc or {}).get("data") or [] if isinstance(doc, dict) else []


def sums(data: list[dict]) -> dict:
    return {f: round(sum((r.get(f) or 0) for r in data), 5) for f in FIELDS}


def line(label: str, status: int, doc, body: bytes) -> None:
    meta = (doc or {}).get("meta") if isinstance(doc, dict) else None
    self_link = ((doc or {}).get("links") or {}).get("self") if isinstance(doc, dict) else None
    print(f"\n### {label}: HTTP {status} meta={json.dumps({k: (meta or {}).get(k) for k in ('count', 'page', 'limit')})}"
          f" self={self_link}\n{body[:700].decode('utf-8', 'replace')}", flush=True)


def main() -> None:
    status, body = get("terms")
    print(f"\n=== TERMS HTTP {status} ({len(body)} bytes)\n{text_of(body)[:9000]}", flush=True)
    status, body = get("")
    t = text_of(body)
    print(f"\n=== ROOT HTTP {status} ({len(body)} bytes)\n{t[:2500]}", flush=True)
    for word in ("donation", "dopłat", "refund", "contribution", "wartość", "aktualiz"):
        for m in re.finditer(word, t, re.I):
            print(f"ROOT[{word}] …{t[max(0, m.start() - 250): m.end() + 250]}…")
            break

    status, body = get("swagger/v1.0/swagger.json")
    doc = json.loads(body)
    print("\n=== SWAGGER descriptions")
    for name, d in (doc.get("definitions") or {}).items():
        if name in ("monthly-provision", "yearly-provision", "provisions-sums", "available-periods", "provision-attributes"):
            print(name, d.get("description"), json.dumps(d.get("properties"), ensure_ascii=False)[:1500])
    for p, ops in doc["paths"].items():
        if p in ("/monthly-provisions", "/available-periods"):
            for verb, op in ops.items():
                print(p, verb, (op.get("summary") or "")[:300], (op.get("description") or "")[:600])
                for q in op.get("parameters", []):
                    if q.get("name") in ("aggregationType", "province", "dateFrom", "dateTo", "limit", "atc"):
                        print("  ", q.get("name"), json.dumps({k: v for k, v in q.items() if k != "description"}),
                              (q.get("description") or "")[:900])

    for path in ("available-periods", "version"):
        s, d, b = api(path)
        line(path, s, d, b)

    for limit in (100, 50):
        s, d, b = api("monthly-provisions", page=1, limit=limit)
        line(f"limit={limit}", s, d, b[:300])
        print("rows returned", len(rows(d)))

    pages, keys = [], []
    for page in (1, 2, 3, 4, 5):
        s, d, b = api("monthly-provisions", page=page, limit=25)
        data = rows(d)
        pages.append(data)
        keys += [(r["year"], r["month"]) for r in data]
        print(f"page {page}: HTTP {s} rows {len(data)} first {data[:1]} last {data[-1:]} keys {sorted(set().union(*(r.keys() for r in data))) if data else []}")
    print("unfiltered rows", len(keys), "distinct", len(set(keys)), "min", min(keys), "max", max(keys),
          "sorted" if keys == sorted(keys) else "NOT sorted",
          "nulls", sum(1 for p in pages for r in p for f in FIELDS if r.get(f) is None),
          "value=refund+contribution", sum(1 for p in pages for r in p if abs(r["value"] - r["refund"] - r["contribution-of-patient"]) < 0.02))

    s, d, b = api("monthly-provisions", page=1, limit=25, **MONTH)
    line("national 2024-12", s, d, b)
    national = sums(rows(d))
    for agg in ("W", "G13", "G14"):
        s, d, b = api("monthly-provisions", page=1, limit=25, aggregationType=agg, **MONTH)
        print(f"aggregationType={agg}: HTTP {s} {sums(rows(d))} equal_to_default={sums(rows(d)) == national}")
    s, d, b = api("monthly-provisions", page=1, limit=25, dateFrom="2024-12", dateTo="2024-12")
    line("dateFrom=2024-12 (no day)", s, d, b[:400])

    prov = {}
    for code in [f"{i:02d}" for i in range(1, 17)]:
        s, d, b = api("monthly-provisions", page=1, limit=25, province=code, **MONTH)
        prov[code] = sums(rows(d))
        print(f"province {code}: HTTP {s} count={(d or {}).get('meta', {}).get('count') if isinstance(d, dict) else None} {prov[code]}")
    total = {f: round(sum(v[f] for v in prov.values()), 2) for f in FIELDS}
    print("PARTITION provinces 2024-12", {f: (total[f], round(national[f], 2), round(total[f] - national[f], 2)) for f in FIELDS})
    for code in ("00", "17", "99"):
        s, d, b = api("monthly-provisions", page=1, limit=25, province=code, **MONTH)
        line(f"province={code}", s, d, b[:500])

    s, d, b = api("atc", page=1, limit=25)
    groups = [(x["attributes"]["code"], x["attributes"]["name"]) for x in rows(d)]
    s, d, b = api("atc-provisions", **MONTH)
    by_code = {r["code"]: r for r in ((d or {}).get("data") or {}).get("data", [])}
    atc_sum = {f: 0.0 for f in FIELDS}
    for code, name in groups:
        s, d, b = api("monthly-provisions", page=1, limit=25, atc=name, **MONTH)
        got = sums(rows(d))
        ref = {f: (by_code.get(code) or {}).get(f) for f in FIELDS}
        for f in FIELDS:
            atc_sum[f] += got[f]
        print(f"atc {code}: HTTP {s} {got['refund']} atc-provisions {ref['refund']} equal={all(abs(got[f] - (ref[f] or 0)) < 0.01 for f in FIELDS)}")
    dash = by_code.get("-") or {}
    print("PARTITION atc 2024-12", {f: (round(atc_sum[f], 2), round(national[f], 2), dash.get(f),
                                        round(national[f] - atc_sum[f] - (dash.get(f) or 0), 2)) for f in FIELDS})
    print("atc-provisions codes", sorted(by_code))
    for label, value in (("code C", "C"), ("unknown name", "XYZ")):
        s, d, b = api("monthly-provisions", page=1, limit=25, atc=value, **MONTH)
        line(f"atc {label}", s, d, b[:500])

    names = dict(groups)
    for code, atc in (("08", "P"), ("08", "V"), ("10", "D")):
        s, d, b = api("monthly-provisions", page=1, limit=25, province=code, atc=names[atc])
        data = rows(d)
        print(f"province {code} atc {atc} all periods: HTTP {s} count={(d or {}).get('meta', {}).get('count')} "
              f"first {data[:1]} zeros {sum(1 for r in data for f in FIELDS if r.get(f) == 0)} nulls {sum(1 for r in data for f in FIELDS if r.get(f) is None)}")
    s, d, b = api("monthly-provisions", page=2, limit=25, province="07", atc=names["C"])
    line("province 07 atc C page 2", s, d, b[:600])
    s, d, b = api("yearly-provisions", page=1, limit=25, province="07")
    line("yearly province 07", s, d, b[:900])
    print("requests left", LEFT[0])


if __name__ == "__main__":
    main()
