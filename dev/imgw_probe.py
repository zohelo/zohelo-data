"""Read-only probe 2 of the IMGW archive and API (no writes anywhere): synop daily listing, station list,
sample files with status statistics, API endpoints, Regulamin. Sequential requests with pauses."""

from __future__ import annotations

import collections
import csv
import html
import io
import json
import re
import sys
import time
import zipfile
from datetime import UTC, datetime
from urllib.parse import unquote, urljoin

import requests

from ingestion.common.report import annotate

BASE = "https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/"
API = "https://danepubliczne.imgw.pl/api/data/"
S = requests.Session()
S.headers["User-Agent"] = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"


def get(url):
    for attempt in range(3):
        time.sleep(0.5)
        try:
            r = S.get(url, timeout=(20, 120))
            if r.status_code == 200 or 400 <= r.status_code < 500:
                return r
        except requests.RequestException as exc:
            print("retry", url, exc, flush=True)
        time.sleep(5 * (attempt + 1))
    raise RuntimeError(url)


ROW = re.compile(r'<a href="([^"?/][^"]*)">[^<]*</a>\s*</td>\s*<td[^>]*>([^<]*)</td>\s*<td[^>]*>\s*([^<]*)</td>', re.I)


def listing(url):
    body = get(url).text
    rows = ROW.findall(body)
    if not rows:
        print("UNPARSED", url, body[:1500], flush=True)
    return [(urljoin(url, h), m.strip(), s.strip()) for h, m, s in rows]


def crawl(start, cap):
    files, queue, pages = [], [start], 0
    while queue and pages < cap:
        url = queue.pop(0)
        pages += 1
        for child, m, s in listing(url):
            (queue if child.endswith("/") else files).append(child if child.endswith("/") else (child, m, s))
    return files, pages, queue


def main():
    out = {}
    # 1. raw listing HTML of one directory (to see the exact index format)
    r = get(BASE + "dane_meteorologiczne/dobowe/synop/")
    print("===== RAW LISTING\n" + r.text[:4000], flush=True)
    # 2. synop daily: full crawl
    files, pages, left = crawl(BASE + "dane_meteorologiczne/dobowe/synop/", 200)
    print(f"===== SYNOP DAILY pages={pages} left={len(left)} files={len(files)}", flush=True)
    for f in files:
        print("F", f[0][len(BASE):], f[1], f[2], flush=True)
    per_dir = collections.Counter(f[0][len(BASE):].rsplit("/", 1)[0] for f in files)
    out["synop_daily_dirs"] = dict(sorted(per_dir.items()))
    annotate("notice", f"synop daily: {len(files)} files, {pages} pages", out["synop_daily_dirs"])
    # 3. other families: year directories only (no file listing below the year), for scope statements
    fam = {}
    for sub in ["dane_meteorologiczne/dobowe/klimat/", "dane_meteorologiczne/dobowe/opad/", "dane_meteorologiczne/miesieczne/synop/",
                "dane_meteorologiczne/terminowe/synop/", "dane_hydrologiczne/dobowe/", "dane_hydrologiczne/miesieczne/"]:
        items = listing(BASE + sub)
        fam[sub] = [i[0][len(BASE + sub):] for i in items][:80]
        print(f"===== {sub}", json.dumps(fam[sub]), flush=True)
    sample_dirs = {
        "klimat2023": BASE + "dane_meteorologiczne/dobowe/klimat/2023/",
        "opad2023": BASE + "dane_meteorologiczne/dobowe/opad/2023/",
        "hydro2025": BASE + "dane_hydrologiczne/dobowe/2025/",
        "hydro2022": BASE + "dane_hydrologiczne/dobowe/2022/",
    }
    for k, u in sample_dirs.items():
        items = listing(u)
        print(f"===== DIR {k}", json.dumps([(i[0][len(BASE):], i[1], i[2]) for i in items]), flush=True)
    # 4. station list
    raw = get(BASE + "dane_meteorologiczne/wykaz_stacji.csv").content
    text = raw.decode("cp1250")
    rows = list(csv.reader(io.StringIO(text)))
    codes = collections.Counter(r[0] for r in rows)
    code5 = collections.Counter(r[2].strip() for r in rows if len(r) > 2)
    syn = [r for r in rows if len(r) > 2 and r[2].strip().startswith("12")]
    st = {"rows": len(rows), "distinct_code": len(codes), "dup_code": [c for c, n in codes.items() if n > 1][:20],
          "distinct_code5": len(code5), "dup_code5": [c for c, n in code5.items() if n > 1][:20], "widths": collections.Counter(len(r) for r in rows),
          "synop_like": syn[:80]}
    print("===== WYKAZ", json.dumps(st, ensure_ascii=False), flush=True)
    annotate("notice", "wykaz_stacji", json.dumps(st, ensure_ascii=False)[:3800])
    # 5. synop daily samples: newest monthly (current year), one per-station-year, one 5-year package
    def pick(pred):
        c = [f for f in files if pred(f[0][len(BASE):])]
        return c[-1][0] if c else None
    now = datetime.now(UTC).year
    picks = {
        "current_month": pick(lambda p: f"/{now}/" in p),
        "prev_year_station": pick(lambda p: f"/{now - 1}/" in p),
        "y2010_station": pick(lambda p: "/2010/" in p),
        "five_year": pick(lambda p: re.search(r"/19\d\d_19\d\d/", p) is not None),
    }
    samples = {}
    for k, url in picks.items():
        if not url:
            continue
        z = zipfile.ZipFile(io.BytesIO(get(url).content))
        for m in z.infolist():
            data = z.read(m).decode("cp1250")
            recs = list(csv.reader(io.StringIO(data)))
            widths = collections.Counter(len(x) for x in recs)
            info = {"url": url[len(BASE):], "member": m.filename, "rows": len(recs), "widths": dict(widths), "head": data.splitlines()[:2]}
            if recs and len(recs[0]) == 65:
                stats = {}
                # value/status pairs of s_d: TMAX(5,6) TMIN(7,8) STD(9,10) TMNG(11,12) SMDB(13,14) PKSN(16,17) USL(20,21)
                for name, vi, si in [("TMAX", 5, 6), ("TMIN", 7, 8), ("STD", 9, 10), ("SMDB", 13, 14), ("PKSN", 16, 17), ("USL", 20, 21)]:
                    c = collections.Counter((("empty" if x[vi].strip() == "" else "zero" if re.fullmatch(r"-?0+(\.0+)?", x[vi].strip()) else "num" if re.fullmatch(r"-?\d+(\.\d+)?", x[vi].strip()) else "other:" + x[vi][:6]), x[si]) for x in recs)
                    stats[name] = {f"{a}|{b}": n for (a, b), n in c.items()}
                info["stats"] = stats
                info["stations"] = len({x[0] for x in recs})
                info["years"] = sorted({x[2] for x in recs})[:3] + sorted({x[2] for x in recs})[-2:]
                info["roop"] = dict(collections.Counter(x[15] for x in recs))
                info["sgr"] = dict(collections.Counter(x[58] for x in recs))
            samples[f"{k}:{m.filename}"] = info
            print("===== SAMPLE", json.dumps(info, ensure_ascii=False), flush=True)
    blob = json.dumps(samples, ensure_ascii=False)
    for i in range(0, min(len(blob), 3700 * 4), 3700):
        annotate("notice", f"synop samples {i // 3700}", blob[i:i + 3700])
    # 6. API
    api = {}
    for ep in ["synop", "meteo", "hydro", "hydro2", "warningsmeteo", "warningshydro", "product"]:
        r = get(API + ep)
        try:
            data = r.json()
        except ValueError:
            api[ep] = {"http": r.status_code, "bytes": len(r.content), "text": r.text[:300]}
            print(f"===== API {ep} not JSON {r.status_code} {r.text[:500]}", flush=True)
            continue
        recs = data if isinstance(data, list) else [data]
        keys = collections.Counter(k for x in recs if isinstance(x, dict) for k in x)
        api[ep] = {"http": r.status_code, "bytes": len(r.content), "n": len(recs), "keys": dict(keys),
                   "headers": {k: r.headers.get(k) for k in ("Last-Modified", "ETag", "Cache-Control", "Content-Type", "Date")}}
        print(f"===== API {ep} fetched {datetime.now(UTC).isoformat()} {json.dumps(api[ep], ensure_ascii=False)}\n{json.dumps(data, ensure_ascii=False)[:12000]}", flush=True)
    annotate("notice", "imgw api", json.dumps(api, ensure_ascii=False)[:3800])
    # 7. apiinfo
    r = get("https://danepubliczne.imgw.pl/apiinfo")
    text = re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", r.text)))
    print("===== APIINFO " + text, flush=True)
    i = text.find("Regulamin")
    annotate("notice", "apiinfo Regulamin", text[max(0, i): i + 3800])
    for page in ["https://danepubliczne.imgw.pl/", "https://danepubliczne.imgw.pl/regulamin", "https://danepubliczne.imgw.pl/datastore"]:
        r = get(page)
        t = re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", r.text)))
        print(f"===== PAGE {page} {r.status_code} {t[:6000]}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
