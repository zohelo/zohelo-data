"""Read-only probe for the source review of 13 Nov 2026 (Task #2997): PLG-030, PLG-004, PLG-005, GL-036.

GET/HEAD only; robots.txt honoured (generic agent and Claude agents); a host that refuses (403, TLS error,
robots, bot wall, timeout) is reported and never retried another way. One request at a time per host, a
second apart. Full results go to DEV_OUTPUT_DIR as JSON, compact results to at most ten notices.
"""

import concurrent.futures as cf
import html
import io
import json
import os
import re
import socket
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import urllib.robotparser
import zipfile

UA = "zohelo-data-research/1.0 (+https://zohelo.com)"
CTX = ssl.create_default_context()
ROBOTS: dict = {}
ROBOTS_TXT: dict = {}
LOCKS: dict = {}
LAST: dict = {}
GLOBAL = threading.Lock()
LIMIT = 3900
FULL: dict = {}


def host_lock(netloc):
    with GLOBAL:
        return LOCKS.setdefault(netloc, threading.Lock())


def raw_get(url, n=400000, method="GET", accept="*/*", timeout=40):
    netloc = urllib.parse.urlsplit(url).netloc
    with host_lock(netloc):
        wait = 1.0 - (time.time() - LAST.get(netloc, 0))
        if wait > 0:
            time.sleep(wait)
        try:
            req = urllib.request.Request(url, method=method, headers={"User-Agent": UA, "Accept": accept, "Accept-Language": "pl,en"})
            t0 = time.time()
            with urllib.request.urlopen(req, timeout=timeout, context=CTX) as r:
                body = b"" if method == "HEAD" else r.read(n)
                h = r.headers
                return {
                    "st": r.status,
                    "ct": (h.get("content-type") or "")[:40],
                    "len": h.get("content-length"),
                    "lm": h.get("last-modified"),
                    "etag": (h.get("etag") or "")[:24] or None,
                    "rl": {k: v for k, v in h.items() if "ratelimit" in k.lower() or k.lower() == "retry-after"} or None,
                    "final": r.geturl() if r.geturl() != url else None,
                    "s": round(time.time() - t0, 1),
                    "body": body,
                }
        finally:
            LAST[netloc] = time.time()


def robots_ok(url):
    p = urllib.parse.urlsplit(url)
    host = f"{p.scheme}://{p.netloc}"
    if host not in ROBOTS:
        rp = urllib.robotparser.RobotFileParser()
        try:
            r = raw_get(host + "/robots.txt", 200000, timeout=30)
            txt = r["body"].decode("utf-8", "replace")
            ROBOTS_TXT[host] = txt[:1500]
            rp.parse(txt.splitlines() if "<html" not in txt[:300].lower() else [])
            ROBOTS[host] = rp
        except urllib.error.HTTPError as e:
            ROBOTS_TXT[host] = f"HTTP {e.code}"
            if e.code in (401, 403):
                ROBOTS[host] = f"robots.txt {e.code}"
            else:
                rp.parse([])
                ROBOTS[host] = rp
        except Exception as e:  # noqa: BLE001
            ROBOTS[host] = f"robots.txt {type(e).__name__}: {str(e)[:80]}"
            ROBOTS_TXT[host] = ROBOTS[host]
    rp = ROBOTS[host]
    if isinstance(rp, str):
        return rp
    for agent in ("zohelo-data-research", "ClaudeBot", "Claude-User"):
        if not rp.can_fetch(agent, url):
            return f"robots disallow ({agent})"
    return None


def get(url, n=400000, method="GET", accept="*/*", timeout=40):
    try:
        no = robots_ok(url)
        if no:
            return {"err": no}
        return raw_get(url, n, method, accept, timeout)
    except urllib.error.HTTPError as e:
        return {"err": f"HTTP {e.code}"}
    except Exception as e:  # noqa: BLE001
        return {"err": f"{type(e).__name__}: {str(e)[:90]}"}


def meta(r):
    if "err" in r:
        return r["err"]
    out = {k: r[k] for k in ("st", "ct", "len", "lm", "etag", "rl", "final", "s") if r.get(k)}
    if r.get("body"):
        out["read"] = len(r["body"])
    return out


def text(r):
    return r["body"].decode("utf-8", "replace") if "body" in r else ""


def plain(t):
    t = re.sub(r"(?is)<(script|style|noscript).*?</\1>", " ", t)
    t = re.sub(r"(?s)<[^>]+>", " ", t)
    return re.sub(r"\s+", " ", html.unescape(t)).strip()


def around(t, pattern, before=80, after=320, limit=3):
    p = plain(t) if "<" in t[:3000] else re.sub(r"\s+", " ", t)
    out, last = [], -9999
    for m in re.finditer(pattern, p, re.I):
        if m.start() - last < after:
            continue
        last = m.start()
        out.append(p[max(0, m.start() - before) : m.start() + after])
        if len(out) >= limit:
            break
    return out or "no match"


def jget(url, n=3000000, timeout=60):
    r = get(url, n=n, accept="application/json", timeout=timeout)
    if "err" in r:
        return None, r["err"]
    try:
        return json.loads(text(r)), meta(r)
    except Exception as e:  # noqa: BLE001
        return None, {"not_json": str(e)[:60], **meta(r), "start": text(r)[:200]}


def shape(x, depth=0, maxd=3):
    """Structure of a JSON value: keys and sample scalars."""
    if isinstance(x, dict):
        if depth >= maxd:
            return f"dict({len(x)}): {list(x)[:6]}"
        ks = list(x)
        return {k: shape(x[k], depth + 1, maxd) for k in ks[:8]} | ({"…": len(ks)} if len(ks) > 8 else {})
    if isinstance(x, list):
        return [f"list({len(x)})", shape(x[0], depth + 1, maxd)] if x else []
    return str(x)[:60]


def step(o, key, fn, *a, **k):
    try:
        o[key] = fn(*a, **k)
    except Exception as e:  # noqa: BLE001
        o[key] = f"probe error {type(e).__name__}: {str(e)[:120]}"


# ---------------------------------------------------------------- dane.gov.pl
def dane_gov():
    o = {}
    for host in ("api.dane.gov.pl", "dane.gov.pl", "www.dane.gov.pl"):
        d = {}
        try:
            infos = socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP)
            addrs = []
            for fam, _, _, _, sa in infos:
                if sa[0] not in [a[1] for a in addrs]:
                    addrs.append((fam, sa[0]))
            d["dns"] = [a[1] for a in addrs]
        except Exception as e:  # noqa: BLE001
            d["dns"] = f"{type(e).__name__}: {e}"
            o[host] = d
            continue
        # Where one plain HTTPS request stops: address, TCP connection, TLS handshake.
        fam, ip = next(((f, a) for f, a in addrs if f == socket.AF_INET), addrs[0])
        t0 = time.time()
        try:
            s = socket.socket(fam, socket.SOCK_STREAM)
            s.settimeout(20)
            s.connect((ip, 443))
            d["tcp"] = f"connected to {ip} in {round(time.time() - t0, 1)} s"
            try:
                t1 = time.time()
                ss = CTX.wrap_socket(s, server_hostname=host)
                d["tls"] = f"{ss.version()} in {round(time.time() - t1, 1)} s"
                ss.close()
            except Exception as e:  # noqa: BLE001
                d["tls"] = f"{type(e).__name__}: {str(e)[:90]} after {round(time.time() - t1, 1)} s"
        except Exception as e:  # noqa: BLE001
            d["tcp"] = f"{type(e).__name__}: {str(e)[:60]} to {ip} after {round(time.time() - t0, 1)} s"
        o[host] = d
    reach = [h for h in ("api.dane.gov.pl", "dane.gov.pl") if "tls" in o[h] and "in " in o[h]["tls"] and "Error" not in o[h]["tls"]]
    for host in reach:
        no = robots_ok(f"https://{host}/robots.txt")
        o[f"robots {host}"] = no or ROBOTS_TXT.get(f"https://{host}", "")[:400]
    if "api.dane.gov.pl" in reach and not isinstance(ROBOTS.get("https://api.dane.gov.pl"), str):
        for path in ("datasets", "resources", "institutions"):
            j, m = jget(f"https://api.dane.gov.pl/1.4/{path}?per_page=1")
            o[path] = m if j is None else {"count": (j.get("meta") or {}).get("count"), "meta": shape(j.get("meta"), 0, 1), "attrs": sorted(((j.get("data") or [{}])[0].get("attributes") or {}))[:60], "http": m}
        r = get("https://api.dane.gov.pl/doc")
        o["doc"] = meta(r) if "err" in r else {"http": meta(r), "kw": around(text(r), r"limit|throttl|per_page|rate", limit=3)}
    if "dane.gov.pl" in reach and not isinstance(ROBOTS.get("https://dane.gov.pl"), str):
        for u in ("https://dane.gov.pl/pl/regulamin", "https://dane.gov.pl/pl"):
            r = get(u)
            o[u[20:] or "/"] = meta(r) if "err" in r else {"http": meta(r), "chars": len(plain(text(r))), "kw": around(text(r), r"ponown\w* wykorzyst|licencj|CC0|CC BY|API", limit=4)}
    return o


# ------------------------------------------------------------- data.europa.eu
EU = "https://data.europa.eu"


def eu_search(q, extra=""):
    facets = urllib.parse.quote(json.dumps({"catalog": ["dane-gov-pl"]}, separators=(",", ":")))
    u = f"{EU}/api/hub/search/search?filter=dataset&facets={facets}&limit=4&q={urllib.parse.quote(q)}{extra}"
    j, m = jget(u)
    if j is None:
        return m
    res = j.get("result") or {}
    hits = []
    for d in (res.get("results") or [])[:4]:
        t = d.get("title") or {}
        dist = d.get("distributions") or []
        hosts = sorted({urllib.parse.urlsplit(str((x.get("download_url") or x.get("access_url") or [""])[0] if isinstance(x.get("download_url") or x.get("access_url"), list) else (x.get("download_url") or x.get("access_url") or ""))).netloc for x in dist})
        fmts = sorted({str((x.get("format") or {}).get("label") or (x.get("format") or {}).get("id")) for x in dist})
        lic = sorted({str((x.get("license") or {}).get("label") or (x.get("license") or {}).get("id") or (x.get("license") or {}).get("resource")) for x in dist})
        hits.append({"id": d.get("id"), "t": str(t.get("pl") or t.get("en") or t)[:70], "pub": str((d.get("publisher") or {}).get("name"))[:40], "mod": str(d.get("modified"))[:10], "n": len(dist), "fmt": fmts[:6], "lic": [x[:40] for x in lic[:3]], "hosts": hosts[:4]})
    return {"count": res.get("count"), "hits": hits}


def europa():
    o = {}
    no = robots_ok(EU + "/api/hub/search/")
    o["robots"] = no or ROBOTS_TXT.get(EU, "")[:500]
    if no:
        return o, {}
    j, m = jget(f"{EU}/api/hub/search/catalogues/dane-gov-pl")
    if j is None:
        o["catalogue"] = m
    else:
        res = j.get("result") or j
        FULL["eu_catalogue"] = res
        o["catalogue"] = {k: (str(v)[:160]) for k, v in res.items() if k in ("id", "title", "description", "count", "modified", "issued", "homepage", "publisher", "license", "spatial", "country", "language")}
        o["catalogue_keys"] = sorted(res)[:40]
    step(o, "all", eu_search, "")
    j, m = jget(f"{EU}/api/hub/search/search?filter=dataset&limit=1&facets=" + urllib.parse.quote(json.dumps({"catalog": ["dane-gov-pl"]}, separators=(",", ":"))))
    if j:
        r0 = ((j.get("result") or {}).get("results") or [{}])[0]
        FULL["eu_first_dataset"] = r0
        o["dataset_keys"] = sorted(r0)[:60]
        o["dist_keys"] = sorted((r0.get("distributions") or [{}])[0])[:40]
        o["catalog_of_hit"] = shape(r0.get("catalog"), 0, 2)
    for u in (f"{EU}/en/legal-notice", f"{EU}/api/hub/search/", f"{EU}/en/about/api", "https://dataeuropa.gitlab.io/data-provider-manual/legal-notice/"):
        r = get(u)
        o["page " + u[8:60]] = meta(r) if "err" in r else {"http": meta(r), "chars": len(plain(text(r))), "kw": around(text(r), r"CC0|CC BY|Creative Commons|metadata (is|are)|reuse of|re-use|rate limit", limit=3)}
    s = {}
    for q in ("Urząd Transportu Kolejowego", "przewozy pasażerskie kolej", "Służba Więzienna", "osadzeni", "Najwyższa Izba Kontroli", "Warszawa rozkład jazdy", "ZTM Warszawa", "Urząd Patentowy"):
        step(s, q, eu_search, q)
    return o, s


# ------------------------------------------------------------------------ NBP
NBP_EN = "https://nbp.pl/en/statistic-and-financial-reporting/monetary-and-financial-statistics/"
NBP_PL = "https://nbp.pl/statystyka-i-sprawozdawczosc/statystyka-monetarna-i-finansowa/"
FILE_RE = r"https?:(?:\\?/){2}static\.nbp\.pl(?:\\?/)[^\"'\s<>)]+?\.(?:xlsx?|zip|csv)"


def file_urls(t):
    out = []
    for m in re.finditer(FILE_RE, t, re.I):
        u = m.group(0).replace("\\/", "/")
        if u not in out:
            out.append(u)
    for m in re.finditer(r'(?i)href=["\']([^"\']+\.(?:xlsx?|zip|csv))["\']', t):
        u = html.unescape(m.group(1))
        if u not in out:
            out.append(u)
    return out


def xlsx_peek(url, max_bytes=20000000):
    r = get(url, n=max_bytes, timeout=90)
    if "err" in r:
        return r["err"]
    b = r["body"]
    out = {"bytes": len(b), "lm": r.get("lm"), "etag": r.get("etag")}
    try:
        z = zipfile.ZipFile(io.BytesIO(b))
        wb = z.read("xl/workbook.xml").decode("utf-8", "replace")
        names = re.findall(r'<sheet\b[^>]*\bname="([^"]+)"', wb)
        out["sheets"] = len(names)
        out["names"] = [html.unescape(x)[:28] for x in names[:14]]
        sheets = sorted(x for x in z.namelist() if re.match(r"xl/worksheets/sheet\d+\.xml$", x))
        dims = []
        for sname in sheets[:14]:
            head = z.open(sname).read(3000).decode("utf-8", "replace")
            m = re.search(r'<dimension ref="([^"]+)"', head)
            dims.append(m.group(1) if m else "?")
        out["dims"] = dims
        try:
            from ingestion.common import xlsx_cells  # noqa: PLC0415

            per, total = {}, 0
            for c in xlsx_cells.cells(b):
                total += 1
                d = per.setdefault(c["sheet_name"], {"cells": 0, "max_row": 0, "head": [], "tail": []})
                d["cells"] += 1
                row = c["row_number"]
                d["max_row"] = max(d["max_row"], row)
                item = f"{c['cell_ref']}={str(c['value_text'])[:20]}"
                if len(d["head"]) < 28 and len(per) <= 2:
                    d["head"].append(item)
                if len(per) <= 1:
                    d["tail"] = (d["tail"] + [item])[-8:]
            out["cells"] = total
            out["per_sheet"] = {str(k)[:24]: (v if i < 2 else {"cells": v["cells"], "max_row": v["max_row"]}) for i, (k, v) in enumerate(per.items()) if i < 10}
        except Exception as e:  # noqa: BLE001
            out["rows"] = f"{type(e).__name__}: {str(e)[:80]}"
    except Exception as e:  # noqa: BLE001
        out["zip"] = f"{type(e).__name__}: {str(e)[:60]}; start {b[:40]!r}"
    return out


def nbp():
    o, files = {}, {}
    for h in ("https://nbp.pl", "https://static.nbp.pl"):
        no = robots_ok(h + "/robots.txt")
        o["robots " + h[8:]] = no or re.sub(r"\s+", " ", ROBOTS_TXT.get(h, ""))[:300]
    pages = []
    for idx in (NBP_EN, NBP_PL):
        r = get(idx, n=1500000)
        if "err" in r:
            o["index " + idx[15:40]] = r["err"]
            continue
        t = text(r)
        subs = []
        for m in re.finditer(r'(?i)href=["\']([^"\'#]+)["\']', t):
            u = urllib.parse.urljoin(idx, html.unescape(m.group(1)))
            if u.startswith(idx) and u != idx and u not in subs:
                subs.append(u)
        o["index " + idx[15:22]] = {"http": meta(r), "subpages": len(subs), "files": file_urls(t)[:6]}
        FULL.setdefault("nbp_subpages", []).extend(subs)
        pages += [(idx, t)] + [(u, None) for u in subs[:16]]
    per_page = {}
    for u, t in pages:
        if t is None:
            r = get(u, n=1500000)
            if "err" in r:
                per_page[u] = r["err"]
                continue
            t = text(r)
        fs = file_urls(t)
        per_page[u] = fs
        for f in fs:
            files.setdefault(f, u)
        if not fs and "/en/" in u:
            # Say how the page offers its file when no link is in the HTML.
            per_page[u] = {"none": around(t, r"Download|xlsx|\.xls", 60, 160, 2), "scripts": len(re.findall(r"(?i)<script", t)), "chars": len(plain(t))}
    FULL["nbp_pages"] = per_page
    o["pages"] = {re.sub(r"https://nbp.pl/(en/)?[^/]+/[^/]+/", lambda m: "en:" if m.group(1) else "pl:", k)[:70]: ([x.rsplit("/", 1)[-1] for x in v][:8] if isinstance(v, list) else v) for k, v in per_page.items()}
    heads = {}
    mon = [f for f in files if "static.nbp.pl" in f]
    for f in mon[:40]:
        r = get(f, method="HEAD")
        heads[f.replace("https://static.nbp.pl/", "")] = r["err"] if "err" in r else f"{r.get('len')} b; {r.get('lm')}; {r.get('etag')}"
    FULL["nbp_heads"] = heads
    FULL["nbp_files_from"] = files
    peeks = {}
    for f in mon[:40]:
        if f.lower().endswith((".xlsx",)) and len(peeks) < 7 and "monetarno" in f:
            step(peeks, f.rsplit("/", 1)[-1], xlsx_peek, f)
    FULL["nbp_peeks"] = peeks
    return o, {"n_files": len(files), "heads": heads}, peeks


# ----------------------------------------------------------------------- CCKP
def cckp():
    o = {}
    for h in ("https://climateknowledgeportal.worldbank.org", "https://cckpapi.worldbank.org", "https://worldbank.github.io", "https://datacatalogapi.worldbank.org", "https://wbg-cckp.s3.us-west-2.amazonaws.com"):
        no = robots_ok(h + "/robots.txt")
        o["robots " + h[8:30]] = no or re.sub(r"\s+", " ", ROBOTS_TXT.get(h, ""))[:160]
    r = get("https://worldbank.github.io/climateknowledgeportal/_sources/README.md", n=600000)
    if "err" in r:
        o["readme"] = r["err"]
    else:
        t = text(r)
        FULL["cckp_readme"] = t[:60000]
        o["readme"] = {"chars": len(t), "licence": around(t, r"licen[sc]e|CC[- ]BY|terms of use|attribution|citation", 60, 380, 3), "s3": around(t, r"s3://|arn:aws|amazonaws", 60, 260, 2), "era5": around(t, r"era5", 80, 300, 2), "subnational": around(t, r"sub-?national|admin|country", 60, 240, 2)}
    return o


def cckp_api():
    o = {}
    base = "https://cckpapi.worldbank.org/cckp/v1/"
    found = None
    for years in ("1950-2024", "1950-2023", "1950-2022"):
        u = f"{base}era5-x0.25_timeseries_tas_timeseries_monthly_{years}_mean_historical_era5_x0.25_mean/POL?_format=json"
        j, m = jget(u, timeout=90)
        if j is None:
            o[years] = m
            continue
        FULL["cckp_api_" + years] = j
        o[years] = {"http": m, "shape": shape(j, 0, 3)}
        found = years
        data = j.get("data") if isinstance(j, dict) else None
        try:
            series = data["POL"] if "POL" in data else next(iter(data.values()))
            if isinstance(series, dict) and "POL" in series:
                series = series["POL"]
            ks = sorted(series)
            o[years]["n"] = len(ks)
            o[years]["first"] = {k: series[k] for k in ks[:2]}
            o[years]["last"] = {k: series[k] for k in ks[-3:]}
        except Exception as e:  # noqa: BLE001
            o[years]["series"] = f"{type(e).__name__}: {str(e)[:60]}"
        break
    if found:
        for label, var, geo in (("pr", "pr", "POL"), ("annual", "tas", "POL"), ("sub", "tas", "POL.2593"), ("all", "tas", "all_countries")):
            agg = "annual" if label == "annual" else "monthly"
            u = f"{base}era5-x0.25_timeseries_{var}_timeseries_{agg}_{found}_mean_historical_era5_x0.25_mean/{geo}?_format=json"
            j, m = jget(u, n=8000000, timeout=90)
            if j is None:
                o[label] = m
            else:
                d = j.get("data") if isinstance(j, dict) else None
                o[label] = {"http": m, "geos": (len(d), list(d)[:5]) if isinstance(d, dict) else shape(j, 0, 2), "meta": shape(j.get("metadata"), 0, 2) if isinstance(j, dict) else None}
    for u in ("https://climateknowledgeportal.worldbank.org/download-data", "https://climateknowledgeportal.worldbank.org/"):
        r = get(u, n=900000)
        o["page " + u[45:] or "/"] = meta(r) if "err" in r else {"http": meta(r), "chars": len(plain(text(r))), "kw": around(text(r), r"licen[sc]e|CC[- ]BY|terms of use|cite|citation|cckpapi|sub-?national|\bS3\b", 60, 300, 4)}
    j, m = jget("https://datacatalogapi.worldbank.org/ddhxext/DatasetView?dataset_unique_id=0065626")
    if j is None:
        o["datacatalog"] = m
    else:
        FULL["datacatalog"] = j
        flat = json.dumps(j, ensure_ascii=False)
        o["datacatalog"] = {"keys": sorted(j)[:30], "licence": around(flat, r"licen[sc]e", 20, 220, 3), "updated": around(flat, r"last_updated|release_date|temporal|end_date", 10, 120, 3)}
    for pre in ("", "data/", "data/era5-x0.25/"):
        r = get("https://wbg-cckp.s3.us-west-2.amazonaws.com/?list-type=2&delimiter=/&max-keys=40&prefix=" + urllib.parse.quote(pre))
        o["s3 " + (pre or "/")] = r["err"] if "err" in r else {"prefixes": re.findall(r"<Prefix>([^<]+)</Prefix>", text(r))[:30], "keys": re.findall(r"<Key>([^<]+)</Key>", text(r))[:8], "truncated": re.findall(r"<IsTruncated>(\w+)", text(r))}
    return o


def notice(title, data):
    b = json.dumps(data, default=str, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    if len(b) > LIMIT:
        b = b[: LIMIT - 4] + "…".encode()
    msg = b.decode("utf-8", "ignore").replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    print(f"::notice title={title.replace(',', ' ')}::{msg}", flush=True)


def safe(fn):
    try:
        return fn()
    except Exception as e:  # noqa: BLE001
        return {"probe_error": f"{type(e).__name__}: {str(e)[:200]}"}


def main():
    with cf.ThreadPoolExecutor(5) as ex:
        f1, f2, f3, f4, f5 = (ex.submit(safe, f) for f in (dane_gov, europa, nbp, cckp, cckp_api))
        r1, r2, r3, r4, r5 = (f.result() for f in (f1, f2, f3, f4, f5))
    out = {"dane_gov": r1, "europa": r2, "nbp": r3, "cckp": r4, "cckp_api": r5, "robots": ROBOTS_TXT, "full": FULL}
    d = os.environ.get("DEV_OUTPUT_DIR")
    if d:
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, "src_r13_probe.json"), "w", encoding="utf-8") as fh:
            json.dump(out, fh, ensure_ascii=False, default=str, indent=1)
    notice("PLG-030 dane.gov.pl from the runner", r1)
    if isinstance(r2, tuple):
        notice("PLG-030 data.europa.eu catalogue dane-gov-pl", r2[0])
        notice("data.europa.eu searches (PL-ENV-025 PLG-032 PLG-028 PL-ENV-028 EX-002)", r2[1])
    else:
        notice("PLG-030 data.europa.eu", r2)
    if isinstance(r3, tuple):
        notice("PLG-004 PLG-005 NBP pages", r3[0])
        notice("PLG-004 PLG-005 NBP files (HEAD)", r3[1])
        notice("PLG-004 PLG-005 NBP workbooks", r3[2])
    else:
        notice("PLG-004 PLG-005 NBP", r3)
    notice("GL-036 CCKP documentation", r4)
    notice("GL-036 CCKP API and files", r5)


if __name__ == "__main__":
    main()
