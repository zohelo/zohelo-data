"""Read-only Dev probe for gios_air_quality (#2960): parameter codes of all sensors, which codes answer getData,
terms wording. No storage access. One request at a time, 1 s apart (lists 31 s for metadata). Deleted before merge.
"""

from __future__ import annotations

from collections import Counter, defaultdict
import json
import re
import time

import requests

from ingestion.common.report import annotate

API = "https://api.gios.gov.pl/pjp-api/v1/rest"
UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
S = requests.Session()
S.headers.update({"User-Agent": UA, "Accept": "application/ld+json, application/json;q=0.9"})


def get(url, params=None, accept=None):
    time.sleep(1.0)
    h = {"Accept": accept} if accept else {}
    r = S.get(url, params=params, timeout=(30, 120), headers=h)
    return r


def note(title, data):
    text = json.dumps(data, ensure_ascii=False, default=str)
    print(f"### {title}: {text}", flush=True)
    annotate("notice", title, json.loads(text) if len(text) < 3800 else {"cut": text[:3700]})


def main() -> int:
    started = time.monotonic()
    r = get(f"{API}/station/findAll", {"page": 0, "size": 500})
    stations = r.json()["Lista stacji pomiarowych"]
    codes, by_code, odd = Counter(), defaultdict(list), []
    timing = []
    for st in stations:
        sid = st["Identyfikator stacji"]
        t0 = time.monotonic()
        r = get(f"{API}/station/sensors/{sid}", {"page": 0, "size": 500})
        timing.append(round(time.monotonic() - t0, 2))
        if r.status_code != 200 or not r.content:
            odd.append({"station": sid, "status": r.status_code, "bytes": len(r.content), "text": r.text[:120]})
            continue
        for s in r.json().get("Lista stanowisk pomiarowych dla podanej stacji", []):
            key = (s.get("Wskaźnik - kod"), s.get("Wskaźnik - wzór"), s.get("Wskaźnik"), s.get("Id wskaźnika"))
            codes[key] += 1
            by_code[s.get("Wskaźnik - kod")].append(s["Identyfikator stanowiska"])
    note("probe3 sensor lists", {"stations": len(stations), "sensors": sum(codes.values()), "odd": odd[:10],
                                 "odd_count": len(odd), "max_s": max(timing), "codes": [[*k, n] for k, n in codes.most_common()]})
    result = {}
    for code, ids in sorted(by_code.items(), key=lambda kv: str(kv[0])):
        out = []
        for sensor in ids[:3]:
            r = get(f"{API}/data/getData/{sensor}", {"page": 0, "size": 500})
            row = {"sensor": sensor, "status": r.status_code}
            if r.status_code == 200:
                items = r.json().get("Lista danych pomiarowych", [])
                row.update({"n": len(items), "nulls": sum(i.get("Wartość") is None for i in items),
                            "first": items[:1], "pages": r.json().get("totalPages")})
            else:
                row["text"] = re.sub(r"\s+", " ", r.text)[:240]
            out.append(row)
        result[str(code)] = out
    note("probe3 getData per code", result)
    # 400 rate across one code that failed and one that worked: are 400s stable on repeat?
    terms = get("https://powietrze.gios.gov.pl/pjp/content/terms_of_service", accept="text/html")
    plain = re.sub(r"<[^>]+>", " ", terms.text)
    plain = re.sub(r"\s+", " ", plain)
    hits = {k: [plain[max(0, m.start() - 200): m.start() + 260] for m in re.finditer(k, plain)][:2]
            for k in ("EKOINFONET", "Creative Commons", "usługi API", "przetworzeni")}
    api = get("https://powietrze.gios.gov.pl/pjp/content/api", accept="text/html")
    plain_api = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", api.text))
    hits["api_page_regulamin"] = [plain_api[max(0, m.start() - 200): m.start() + 200]
                                  for m in re.finditer("Regulamin", plain_api)][:3]
    hits["api_page_weryfik"] = [plain_api[max(0, m.start() - 200): m.start() + 200]
                                for m in re.finditer("weryfik", plain_api)][:2]
    note("probe3 terms", {"terms_status": terms.status_code, "api_status": api.status_code, **hits})
    time.sleep(31)
    meta = get(f"{API}/metadata/stations", {"page": 0, "size": 2})
    try:
        body = meta.json()
        lists = {k: v[:2] for k, v in body.items() if isinstance(v, list)}
    except ValueError:
        lists = {"text": meta.text[:300]}
    note("probe3 metadata/stations", {"status": meta.status_code, "lists": lists,
                                      "minutes": round((time.monotonic() - started) / 60, 1)})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
