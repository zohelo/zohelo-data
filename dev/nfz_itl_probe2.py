"""Second read-only probe of the NFZ waiting-times API v1.4 (PLS-015, Task #2956).

GET only, one request at a time, 0.6 s pause (terms: at most 10 requests/second per IP), at most 200 requests.
Questions: where the v1.4 site's "Regulamin" lives; whether case=2 differs from case=1; whether paging is stable
(one province read in full twice); how long a polite full read takes; the /version answer. Writes nothing to R2.
"""

import json
import re
import time
import urllib.error
import urllib.request
from collections import Counter

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
BASE = "https://apinfz.nfz.gov.pl/app-itl-api-pcus"
Q = "format=json&api-version=1.4"
BUDGET = [200]
TIMES = []


def get(url):
    if BUDGET[0] <= 0:
        return None, "budget used"
    BUDGET[0] -= 1
    time.sleep(0.6)
    t = time.monotonic()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json, text/html;q=0.8"})
        with urllib.request.urlopen(req, timeout=90) as r:
            body = r.read()
            TIMES.append(time.monotonic() - t)
            return body, f"{r.status}"
    except urllib.error.HTTPError as e:
        return e.read(800), f"HTTP {e.code}"
    except Exception as e:  # noqa: BLE001
        return None, f"{type(e).__name__}: {str(e)[:160]}"


def page(case, prov, n):
    body, st = get(f"{BASE}/queues?case={case}&province={prov}&page={n}&limit=25&{Q}")
    if body is None or st != "200":
        print("FAIL", case, prov, n, st, (body or b"")[:300])
        return None, 0
    return json.loads(body), len(body)


def main():
    body, st = get(BASE + "/")
    print("=== ROOT HTML", st)
    print((body or b"").decode("utf-8", "replace"))
    scripts = re.findall(r'src="([^"]+)"', (body or b"").decode("utf-8", "replace"))
    print("SCRIPTS", scripts)
    for path in ("/regulamin/", "/terms/", "/Regulamin", "/home/terms", "/docs/regulamin", "/errors", "/changelog", "/schemas"):
        body, st = get(BASE + path)
        text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", (body or b"").decode("utf-8", "replace")))
        print(f"=== {path} {st} {len(body or b'')}: {text[:400]}")
    for path in ("/version", "/version/sources"):
        body, st = get(f"{BASE}{path}?{Q}")
        print(f"=== {path} {st}: {(body or b'')[:1500].decode('utf-8', 'replace')}")
    # case 1 vs case 2, first page of mazowieckie
    a, _ = page(1, "07", 1)
    b, _ = page(2, "07", 1)
    if a and b:
        ia = [d["id"] for d in a["data"]]
        ib = [d["id"] for d in b["data"]]
        print("CASE ids equal", ia == ib, "overlap", len(set(ia) & set(ib)))
        print("CASE1 attrs", [(d["attributes"]["case"], d["attributes"]["benefit"][:30], d["attributes"]["dates"]) for d in a["data"][:5]])
        print("CASE2 attrs", [(d["attributes"]["case"], d["attributes"]["benefit"][:30], d["attributes"]["dates"]) for d in b["data"][:5]])
        print("CASE2 meta", json.dumps(b["meta"].get("count")), json.dumps(b["links"]))
    # full read of lubuskie (04), case 1, twice
    reads = []
    for attempt in (1, 2):
        t0 = time.monotonic()
        first, size = page(1, "04", 1)
        if not first:
            return
        count = first["meta"]["count"]
        pages = -(-count // 25)
        ids, total_bytes, records = [], size, list(first["data"])
        for n in range(2, pages + 1):
            d, size = page(1, "04", n)
            if not d:
                break
            total_bytes += size
            records += d["data"]
        ids = [r["id"] for r in records]
        dup = [k for k, v in Counter(ids).items() if v > 1]
        reads.append(ids)
        print(f"READ {attempt}: count {count} pages {pages} records {len(ids)} distinct {len(set(ids))} dup {len(dup)} bytes {total_bytes} seconds {time.monotonic() - t0:.0f}")
        if attempt == 1:
            at = [r["attributes"] for r in records]
            keys = Counter(k for a in at for k in a)
            print("ATTR KEYS", dict(keys))
            for k in ("case", "many-places", "anesthesia", "benefits-for-children", "age-range", "covid-19", "type-building", "queue-in-cer"):
                print("VALUES", k, Counter(str(a.get(k)) for a in at).most_common(12))
            print("STATS update", Counter(str((a.get("statistics") or {}).get("provider-data", {}).get("update") if (a.get("statistics") or {}).get("provider-data") else None) for a in at).most_common(15))
            print("STATS null provider-data", sum(1 for a in at if not (a.get("statistics") or {}).get("provider-data")))
            print("STATS null awaiting", sum(1 for a in at if (a.get("statistics") or {}).get("provider-data") and a["statistics"]["provider-data"].get("awaiting") is None))
            print("COMPUTED", Counter(json.dumps((a.get("statistics") or {}).get("computed-data"))[:60] for a in at).most_common(5))
            print("DATES applicable", Counter(str((a.get("dates") or {}).get("applicable")) for a in at).most_common(5))
            print("DATES pcus", Counter(str((a.get("dates") or {}).get("pcus")) for a in at).most_common(25))
            print("DATES asat", Counter(str((a.get("dates") or {}).get("date-situation-as-at")) for a in at).most_common(10))
            print("DATES keys", Counter(k for a in at for k in (a.get("dates") or {})).most_common())
            print("BENEFITS-PROVIDED", Counter(json.dumps(a.get("benefits-provided"))[:80] for a in at).most_common(5))
            print("TERYT place len", Counter(len(a.get("teryt-place") or "") for a in at), "prefix", Counter((a.get("teryt-place") or "")[:2] for a in at).most_common(5))
            print("TERYT provider prefix", Counter((a.get("teryt-provider") or "")[:2] for a in at).most_common(5))
            mp = [r for r in records if r["attributes"].get("many-places") not in ("N", None)]
            print("MANY-PLACES sample", json.dumps(mp[:2], ensure_ascii=False)[:3000])
            key = Counter((a.get("provider-code"), a.get("benefit"), a.get("place"), a.get("address"), a.get("case")) for a in at)
            print("DUP provider/benefit/place/address/case", sum(1 for v in key.values() if v > 1))
            key2 = Counter((a.get("provider-code"), a.get("benefit"), a.get("case")) for a in at)
            print("DUP provider/benefit/case", sum(1 for v in key2.values() if v > 1))
            grp = {}
            for a in at:
                pd = (a.get("statistics") or {}).get("provider-data") or {}
                grp.setdefault((a.get("provider-code"), a.get("benefit")), []).append((a.get("place"), pd.get("awaiting"), pd.get("update"), a.get("many-places")))
            multi = [(k, v) for k, v in grp.items() if len(v) > 1]
            same = sum(1 for k, v in multi if len({x[1] for x in v}) == 1)
            print("MULTI provider/benefit groups", len(multi), "same awaiting", same, "sample", json.dumps(multi[:4], ensure_ascii=False)[:2000])
            print("NONASCII newline in page bodies", any("\n" in json.dumps(r) for r in records[:5]))
    if len(reads) == 2:
        print("STABLE order", reads[0] == reads[1], "same set", set(reads[0]) == set(reads[1]),
              "only1", len(set(reads[0]) - set(reads[1])), "only2", len(set(reads[1]) - set(reads[0])))
    TIMES.sort()
    print("LATENCY median", TIMES[len(TIMES) // 2] if TIMES else None, "max", TIMES[-1] if TIMES else None, "n", len(TIMES))
    print("BUDGET_LEFT", BUDGET[0])


if __name__ == "__main__":
    main()
