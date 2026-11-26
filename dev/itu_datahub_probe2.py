"""Read-only probe 2 for GL-031 ITU DataHub (#2419): the Data360 bulk CSV per indicator named by the metadata
(csv_link https://data360files.worldbank.org/data360-data/data/ITU_DH/<INDICATOR>.csv). GET/HEAD only, one request
at a time, >= 1 s apart, robots.txt read first; no writes to R2."""

import collections
import csv
import io
import json
import time
import urllib.error
import urllib.request
import urllib.robotparser

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
HOST = "https://data360files.worldbank.org"
BASE = f"{HOST}/data360-data/data/ITU_DH"
API = "https://data360api.worldbank.org/data360"
LAST = [0.0]


def call(url, method="GET", tries=3):
    for attempt in range(tries):
        wait = 1.2 - (time.time() - LAST[0])
        if wait > 0:
            time.sleep(wait)
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA}, method=method)
            with urllib.request.urlopen(req, timeout=120) as r:
                return r.status, dict(r.headers), r.read() if method == "GET" else b""
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504) and attempt < tries - 1:
                time.sleep(20 * (attempt + 1))
                continue
            return e.code, dict(e.headers or {}), e.read()[:500]
        except Exception as e:  # noqa: BLE001
            if attempt < tries - 1:
                time.sleep(20)
                continue
            return 0, {}, str(e).encode()
        finally:
            LAST[0] = time.time()


def main():
    st, h, b = call(f"{HOST}/robots.txt")
    print(f"ROBOTS {st} {b[:800]!r}")
    if st == 200:
        rp = urllib.robotparser.RobotFileParser()
        rp.parse(b.decode("utf-8", "replace").splitlines())
        print("ROBOTS can_fetch", rp.can_fetch(UA, f"{BASE}/IT_NET_USER.csv"))
    st, h, b = call(f"{API}/indicators?datasetId=ITU_DH")
    indicators = json.loads(b)
    for extra in ("", ".csv", ".zip", "/ITU_DH.csv", "/ITU_DH.zip"):
        url = BASE + extra if extra.startswith("/") or extra == "" else BASE + extra
        st, h, _ = call(url, "HEAD")
        print(f"EXTRA {st} {url} {h.get('Content-Length')} {h.get('Last-Modified')}")
    totals = collections.Counter()
    summary = {}
    for ind in indicators:
        url = f"{BASE}/{ind}.csv"
        st, h, b = call(url)
        meta = {k: h.get(k) for k in ("Content-Type", "Content-Length", "Last-Modified", "ETag", "Cache-Control")}
        if st != 200:
            print(f"CSV {ind} {st} {meta} {b[:200]!r}")
            summary[ind] = {"st": st}
            continue
        text = b.decode("utf-8-sig")
        rows = list(csv.reader(io.StringIO(text, newline="")))
        header, body = rows[0], [r for r in rows[1:] if r]
        if not totals:
            print(f"HEADER {ind} {header}")
            for r in body[:3]:
                print(f"ROW {ind} {r}")
        idx = {c: i for i, c in enumerate(header)}
        widths = collections.Counter(len(r) for r in body)
        col = lambda r, c: r[idx[c]] if c in idx and idx[c] < len(r) else None  # noqa: E731
        keycols = [c for c in ("INDICATOR", "REF_AREA", "SEX", "AGE", "URBANISATION", "COMP_BREAKDOWN_1",
                               "COMP_BREAKDOWN_2", "COMP_BREAKDOWN_3", "UNIT_MEASURE", "TIME_PERIOD", "FREQ") if c in idx]
        dup = collections.Counter(tuple(col(r, c) for c in keycols) for r in body)
        dups = [k for k, n in dup.items() if n > 1]
        nonnum = collections.Counter()
        for r in body:
            v = col(r, "OBS_VALUE")
            try:
                float(v)
            except (TypeError, ValueError):
                nonnum[str(v)[:40]] += 1
        prof = {c: dict(collections.Counter(col(r, c) for r in body).most_common(6))
                for c in ("UNIT_MEASURE", "UNIT_MULT", "OBS_STATUS", "OBS_CONF", "COMMENT_OBS", "DECIMALS", "INDICATOR", "FREQ") if c in idx}
        summary[ind] = {"rows": len(body), "bytes": len(b), "lm": meta["Last-Modified"], "dups": len(dups)}
        print(f"CSV {ind} rows={len(body)} bytes={len(b)} meta={meta} widths={dict(widths)} header_same={header == rows[0]} "
              f"dups={len(dups)} dup_sample={dups[:2]} nonnum={dict(nonnum.most_common(6))} prof={json.dumps(prof)[:900]}", flush=True)
        if ind in ("IT_NET_USER", "IT_NET_BBND"):
            print(f"HEADER {ind} {header}")
            for r in body:
                if col(r, "REF_AREA") == "POL" and (col(r, "TIME_PERIOD") or "") >= "2022":
                    print(f"POLROW {ind} {r}")
        totals["rows"] += len(body)
        totals["bytes"] += len(b)
    st, h, b = call(f"{HOST}/data360-data/metadata/ITU_DH/IT_NET_USER.json")
    print(f"METAJSON {st} {len(b)} {h.get('Last-Modified')} {b[:1500]!r}")
    note = {"totals": dict(totals), "files": len(summary), "per_file": summary}
    print("::notice title=itu_datahub probe2::" + json.dumps(note)[:3900])


if __name__ == "__main__":
    main()
