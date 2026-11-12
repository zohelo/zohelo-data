"""Read-only probe 2 of RAD-on (PLS-023): page size, cursor paging of institutions and courses in full, profiles
of the non-person fields, and the help-centre pages on the API (parameters, limits, terms).

One request at a time, 2 s pause, identifying user agent, bounded retries. Writes nothing to R2.
"""

from __future__ import annotations

from collections import Counter
import json
import re
import time

import requests

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
BASE = "https://radon.nauka.gov.pl"
S = requests.Session()
S.headers.update({"User-Agent": UA, "Accept": "application/json"})


def get(url, params=None):
    for attempt in range(4):
        time.sleep(2)
        try:
            r = S.get(url, params=params, timeout=(15, 120))
            if r.status_code in (429, 500, 502, 503, 504):
                print(f"  retry {r.status_code} {r.headers.get('Retry-After')}")
                time.sleep(15 * (attempt + 1))
                continue
            return r
        except requests.RequestException as exc:
            print(f"  error {type(exc).__name__}: {exc}"[:300])
            time.sleep(15 * (attempt + 1))
    raise RuntimeError(f"no answer: {url}")


def page_all(resource, size):
    url = f"{BASE}/opendata/polon/{resource}"
    token, rows, pages, seen_tokens, started = None, [], 0, set(), time.monotonic()
    max_count = None
    while True:
        params = {"resultNumbers": size}
        if token:
            params["token"] = token
        r = get(url, params)
        if r.status_code != 200:
            print(f"  page {pages}: {r.status_code} {r.text[:200]}")
            break
        body = r.json()
        res = body.get("results") or []
        rows.extend(res)
        pages += 1
        max_count = body["pagination"].get("maxCount")
        token = body["pagination"].get("token")
        if not res or not token or token in seen_tokens:
            print(f"  stop: results={len(res)} token={token} repeated={token in seen_tokens}")
            break
        seen_tokens.add(token)
        if pages > 400:
            break
    print(f"== {resource}: pages={pages} rows={len(rows)} maxCount={max_count} seconds={time.monotonic()-started:.0f} bytes~{len(json.dumps(rows))}")
    return rows


def prof(rows, key, n=40):
    c = Counter(str(r.get(key)) for r in rows)
    return f"{key}: distinct={len(c)} " + json.dumps(c.most_common(n), ensure_ascii=False)


def main():
    for size in (100, 500, 1000, 5000):
        r = get(f"{BASE}/opendata/polon/institutions", {"resultNumbers": size})
        b = r.json() if r.status_code == 200 else {}
        print(f"size {size}: status {r.status_code} results={len(b.get('results') or [])} {r.text[:150] if r.status_code != 200 else ''}")
    # filters accepted?
    for params in ({"resultNumbers": 2, "lastRefreshDateFrom": "2026-11-01"}, {"resultNumbers": 2, "status": "1"}):
        r = get(f"{BASE}/opendata/polon/institutions", params)
        b = r.json() if r.status_code == 200 else {}
        print(f"params {params}: {r.status_code} maxCount={b.get('pagination', {}).get('maxCount')} {r.text[:150] if r.status_code != 200 else ''}")
    inst = page_all("institutions", 1000)
    uu = Counter(r["institutionUuid"] for r in inst)
    print("institutions dup uuid:", sum(1 for v in uu.values() if v > 1), "null uuid:", sum(1 for r in inst if not r.get("institutionUuid")))
    print("keys:", sorted(set().union(*(r.keys() for r in inst))))
    for k in ("iKindCd", "iKindName", "uTypeCd", "uTypeName", "siTypeCd", "siTypeName", "statusCode", "status",
              "voivodeshipCode", "voivodeship", "country", "dataSource", "pib", "yearPib"):
        print(" ", prof(inst, k))
    print("  null regon:", sum(1 for r in inst if not r.get("regon")), "dup regon:", sum(1 for v in Counter(r.get("regon") for r in inst if r.get("regon")).values() if v > 1))
    print("  regon lengths:", Counter(len(r.get("regon") or "") for r in inst))
    print("  iStartDT range:", min(str(r.get("iStartDT")) for r in inst), max(str(r.get("iStartDT")) for r in inst))
    print("  lastRefresh sample:", [r.get("lastRefresh") for r in inst[:3]])
    print("  managers non-null:", sum(1 for r in inst if r.get("managerSurname")))
    print("  statuses lens:", Counter(len(r.get("statuses") or []) for r in inst))
    print("  kind x status:", json.dumps(Counter((r.get("iKindName"), r.get("status")) for r in inst).most_common(60), ensure_ascii=False))
    courses = page_all("courses", 1000)
    cu = Counter(r["courseUuid"] for r in courses)
    print("courses dup uuid:", sum(1 for v in cu.values() if v > 1), "null uuid:", sum(1 for r in courses if not r.get("courseUuid")))
    print("keys:", sorted(set().union(*(r.keys() for r in courses))))
    for k in ("levelCode", "levelName", "profileCode", "profileName", "currentStatusCode", "currentStatusName",
              "mainInstitutionKindCode", "mainInstitutionKind", "leadingInstitutionVoivodeshipCode",
              "leadingInstitutionVoivodeship", "leadingInstitutionIsForeign", "coLed", "teacherTraining", "dataSource",
              "legalBasisTypeCode", "iscedCode"):
        print(" ", prof(courses, k, 25))
    inst_keys = sorted(set().union(*(i.keys() for r in courses for i in r.get("courseInstances") or [{}])))
    print("  instance keys:", inst_keys)
    ins = [i for r in courses for i in r.get("courseInstances") or []]
    print("  instances:", len(ins), "per course:", Counter(min(len(r.get("courseInstances") or []), 9) for r in courses))
    for k in ("formCode", "formName", "titleCode", "titleName", "statusCode", "statusName", "languageCode", "dual"):
        print("   inst", prof(ins, k, 20))
    print("   inst start years:", sorted(Counter(str(i.get("educationStartDate"))[:4] for i in ins).items()))
    print("   dup instance uuid:", sum(1 for v in Counter(i.get("courseInstanceUuid") for i in ins).values() if v > 1))
    print("  creationDate years:", sorted(Counter(str(r.get("creationDate"))[:4] for r in courses).items()))
    print("  course main inst not in institutions:", sum(1 for r in courses if r.get("mainInstitutionUuid") not in uu))
    print("  leading inst not in institutions:", sum(1 for r in courses if r.get("leadingInstitutionUuid") not in uu))
    print("  status x level:", json.dumps(Counter((r.get("currentStatusName"), r.get("levelName")) for r in courses).most_common(30), ensure_ascii=False))
    # help centre pages
    for slug in ("od-czego-zaczac-prace-z-api", "katalog-udostepnianych-danych-api", "katalog-udostepnianych-danych-api-2",
                 "jakie-uslugi-api-sa-dostepne-w-rad-on", "scenariusze-pobierania-danych-przy-uzyciu-api-rad-onu",
                 "co-to-jest-api", "katalog-edycji-danych-api", "jakie-dane-sa-na-rad-on"):
        url = f"{BASE}/pomoc/knowledge-base/{slug}/"
        r = get(url)
        text = re.sub(r"(?s)<script.*?</script>|<style.*?</style>", " ", r.text)
        text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", text))
        start = text.find("Strona główna")
        body = text[start:start + 3500] if start >= 0 else text[:3500]
        print(f"=== KB {slug}: {r.status_code}\n{body}")
        for m in re.finditer(r"[^.]{0,200}(licenc|CC BY|Creative|ponown|warunk|regulamin|limit|resultNumbers|token|maksym|ogranicz)[^.]{0,250}", text, re.I):
            print("  >>", m.group(0)[:450])


if __name__ == "__main__":
    main()
