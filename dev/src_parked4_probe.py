"""Read-only probe for the re-review of parked entries, batch 4 (Task #2916), 26 Nov 2026.

GL-002, GL-010, GL-012, GL-013, GL-014, GL-018, GL-019, GL-020, GL-024, GL-027, GL-030: terms pages and routes.
GET and HEAD only, one request at a time, at least a second apart per host, robots.txt honoured.
"""
import html
import json
import re
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
import urllib.robotparser

UA = "zohelo-data-research/1.0 (+https://zohelo.com)"
CTX = ssl.create_default_context()
ROBOTS: dict[str, object] = {}
ROBOTS_TXT: dict[str, str] = {}
LAST: dict[str, float] = {}
DEAD: dict[str, str] = {}
COUNT = [0]
LIMIT = 3800
N: dict[str, object] = {}
ERR: list[str] = []
FILE_RE = r"\.(xlsx?|csv|zip|json|xml|ods|pdf|docx?)(\?|$)"
WALL = r"(?i)incapsula|_Incapsula_Resource|cf-challenge|captcha|access denied"


def raw(url, n=3000000, timeout=40, method="GET"):
    netloc = urllib.parse.urlsplit(url).netloc
    wait = 1.0 - (time.time() - LAST.get(netloc, 0))
    if wait > 0:
        time.sleep(wait)
    COUNT[0] += 1
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "*/*", "Accept-Language": "pl,en"}, method=method)
        with urllib.request.urlopen(req, timeout=timeout, context=CTX) as r:
            h = r.headers
            return {
                "st": r.status,
                "ct": (h.get("content-type") or "")[:60],
                "len": h.get("content-length"),
                "lm": h.get("last-modified"),
                "etag": (h.get("etag") or "")[:30] or None,
                "cd": (h.get("content-disposition") or "")[:90] or None,
                "final": r.geturl() if r.geturl() != url else None,
                "body": r.read(n) if method == "GET" else b"",
            }
    finally:
        LAST[netloc] = time.time()


def robots_ok(url, timeout=25):
    p = urllib.parse.urlsplit(url)
    host = f"{p.scheme}://{p.netloc}"
    if host not in ROBOTS:
        rp = urllib.robotparser.RobotFileParser()
        try:
            txt = raw(host + "/robots.txt", 100000, timeout)["body"].decode("utf-8", "replace")
            ROBOTS_TXT[host] = txt[:1500]
            rp.parse(txt.splitlines() if "<html" not in txt[:300].lower() and "<!doctype" not in txt[:300].lower() else [])
            if re.search(WALL, txt[:5000]):
                ROBOTS_TXT[host] = "bot-protection page instead of robots.txt"
                ROBOTS[host] = "bot protection on robots.txt"
            else:
                ROBOTS[host] = rp
        except urllib.error.HTTPError as e:
            ROBOTS_TXT[host] = f"HTTP {e.code}"
            if e.code in (401, 403):
                ROBOTS[host] = f"robots.txt {e.code}"
            else:
                rp.parse([])
                ROBOTS[host] = rp
        except Exception as e:
            ROBOTS_TXT[host] = ROBOTS[host] = f"robots.txt {type(e).__name__}: {str(e)[:80]}"
        print(f"=== ROBOTS {host}\n{ROBOTS_TXT[host]}\n=== END", flush=True)
    rp = ROBOTS[host]
    if isinstance(rp, str):
        return rp
    for agent in ("zohelo-data-research", "ClaudeBot", "Claude-User"):
        if not rp.can_fetch(agent, url):
            return f"robots disallow ({agent})"
    return None


def get(url, n=3000000, timeout=40, method="GET", robots_timeout=25):
    host = urllib.parse.urlsplit(url).netloc
    if host in DEAD:
        return {"err": f"host skipped after: {DEAD[host]}"}
    try:
        no = robots_ok(url, robots_timeout)
        if no:
            if "timed out" in no or "bot protection" in no or "URLError" in no:
                DEAD[host] = no
            return {"err": no}
        return raw(url, n, timeout, method)
    except urllib.error.HTTPError as e:
        return {"err": f"HTTP {e.code}"}
    except Exception as e:
        msg = f"{type(e).__name__}: {str(e)[:90]}"
        if "timed out" in msg or "Name or service" in msg or "CERTIFICATE" in msg:
            DEAD[host] = msg
        return {"err": msg}


def plain(t):
    t = re.sub(r"(?is)<(script|style|noscript).*?</\1>", " ", t)
    t = re.sub(r"(?s)<[^>]+>", " ", t)
    return re.sub(r"\s+", " ", html.unescape(t)).strip()


def links(t, base):
    out = []
    for m in re.finditer(r'(?is)<a\b[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', t):
        h, a = html.unescape(m.group(1)).strip(), plain(m.group(2))[:80]
        if h.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue
        u = urllib.parse.urljoin(base, h).split("#")[0]
        if (a, u) not in out:
            out.append((a, u))
    return out


def around(p, pattern, before=150, after=500, limit=4):
    out, last = [], -9999
    for m in re.finditer(pattern, p, re.I):
        if m.start() - last < after:
            continue
        last = m.start()
        out.append(p[max(0, m.start() - before) : m.start() + after])
        if len(out) >= limit:
            break
    return out


def tag(url):
    return re.sub(r"https?://(www\.)?", "", url)[:100]


def fail(url, r):
    print(f"=== {url}\nERR {r['err']}\n=== END", flush=True)
    ERR.append(f"{tag(url)}: {r['err']}")


def page(url, n=3000000, timeout=40, text=6000, robots_timeout=25):
    """Fetch an HTML page, print its text and links; return (text, links, html) or None."""
    r = get(url, n, timeout, robots_timeout=robots_timeout)
    if "err" in r:
        fail(url, r)
        return None
    t = r.pop("body").decode("utf-8", "replace")
    p = plain(t)
    wall = re.findall(WALL, t[:30000])[:2]
    title = plain("".join(re.findall(r"(?is)<title>.*?</title>", t)))[:90]
    print(f"=== PAGE {url}\nMETA {json.dumps({k: v for k, v in r.items() if v}, ensure_ascii=False)} read={len(t)}", flush=True)
    print(f"TITLE {title}\nCHARS {len(p)} scripts={len(re.findall('(?i)<script', t))} tables={len(re.findall('(?i)<table', t))} wall={wall}", flush=True)
    print(f"TEXT {p[:text]}", flush=True)
    ll = links(t, r.get("final") or url)
    if wall and len(p) < 600:
        DEAD[urllib.parse.urlsplit(url).netloc] = "bot-protection page"
        ERR.append(f"{tag(url)}: bot-protection page ({len(p)} chars)")
    return p, ll, t


def end():
    print("=== END", flush=True)


def jget(url, n=8000000, timeout=60):
    r = get(url, n, timeout)
    if "err" in r:
        fail(url, r)
        return None
    try:
        return json.loads(r["body"].decode("utf-8", "replace"))
    except ValueError:
        print(f"=== {url}\nNOT JSON ct={r['ct']} head={r['body'][:300]!r}\n=== END", flush=True)
        ERR.append(f"{tag(url)}: not JSON ({r['ct']})")
        return None



def emit(title, data):
    msg = json.dumps(data, default=str, separators=(",", ":"), ensure_ascii=False)
    b = msg.encode("utf-8")
    if len(b) > LIMIT:
        msg = b[: LIMIT - 4].decode("utf-8", "ignore") + "…"
    msg = msg.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    print(f"::notice title={title}::{msg}", flush=True)


LIC = r"(?i)licencj|licenc|license|prawa autorskie|wszelkie prawa|all rights|copyright|ponowne wykorzyst|creative commons|cc by|warunki korzystania|regulamin|kopiowanie|rozpowszechnian"
DATA = r"(?i)pobierz|do pobrania|download|eksport|export|api|dane\.gov\.pl|otwarte dane|xlsx|csv"


SERVICE = r"(?i)wfs|wms|ogc|getcapabilities|atom|arcgis/rest|/api/|uslugi|services?\b"


def follow(ll, limit=4):
    out = {}
    for a, u in [(a, u) for a, u in ll if re.search(SERVICE, a + " " + u)][:limit]:
        r = get(u, 200000, 60)
        if "err" in r:
            out[u[-90:]] = r["err"][:60]
            continue
        b = r.pop("body").decode("utf-8", "replace")
        print(f"=== FOLLOW {a} | {u}\nMETA {json.dumps({k: v for k, v in r.items() if v}, ensure_ascii=False)}\nBODY {plain(b)[:3000]}\n=== END", flush=True)
        out[u[-90:]] = {"st": r.get("st"), "ct": r.get("ct"), "types": re.findall(r"<(?:wfs:)?Name>([^<]+)</", b)[:10], "head": plain(b)[:160]}
    return out


def probe_page(url, text=2500, follow_files=3, svc=False):
    got = page(url, text=text)
    if not got:
        return {"err": ERR[-1].split(": ", 1)[-1][:90] if ERR else "failed"}
    p, ll, t = got
    files = [(a, u) for a, u in ll if re.search(FILE_RE, u, re.I)]
    dl = [(a, u) for a, u in ll if re.search(DATA, a + " " + u) and (a, u) not in files]
    for a, u in files[:80]:
        print(f"  FILE {a} | {u}", flush=True)
    for a, u in dl[:40]:
        print(f"  DATA {a} | {u}", flush=True)
    for a, u in ll[:150]:
        print(f"  LINK {a} | {u}", flush=True)
    lic = around(p, LIC, 120, 300, 4)
    for s in lic:
        print(f"  LIC {s}", flush=True)
    end()
    ext: dict[str, int] = {}
    for _, u in files:
        e = re.search(FILE_RE, u, re.I).group(1).lower()
        ext[e] = ext.get(e, 0) + 1
    tab = [(a, u) for a, u in files if re.search(r"(?i)\.(xlsx?|csv|zip|ods|json|xml)(\?|$)", u)]
    heads = []
    for a, u in tab[:follow_files]:
        h = get(u, method="HEAD")
        heads.append({"u": u[-80:], "st": h.get("st") or h.get("err"), "ct": h.get("ct"), "len": h.get("len"), "lm": h.get("lm")})
    fol = follow(ll) if svc else None
    return {"follow": fol, "chars": len(p), "scripts": len(re.findall("(?i)<script", t)), "ext": ext, "tab_files": [u[-90:] for _, u in tab[:6]],
            "dl_links": [f"{a[:30]}|{u[-70:]}" for a, u in dl[:5]], "lic": [s[:240] for s in lic[:2]], "head": heads}


def dane_ds(did):
    """One dataset: its resources and licence fields."""
    api = "https://api.dane.gov.pl/1.4"
    out = {}
    d = jget(f"{api}/datasets/{did}")
    if d:
        at = d.get("data", {}).get("attributes", {})
        print(f"=== DANE dataset {did}\n{json.dumps(at, ensure_ascii=False)[:3000]}\n=== END", flush=True)
        out = {k: at.get(k) for k in ("title", "license_name", "license_condition_default_cc40", "license_condition_responsibilities", "license_condition_source", "license_condition_modification", "license_condition_cc40_responsibilities", "update_frequency", "modified")}
    r = jget(f"{api}/datasets/{did}/resources?per_page=30")
    if r:
        print(f"=== DANE resources raw {did} {json.dumps(r, ensure_ascii=False)[:2500]}\n=== END", flush=True)
        res = []
        for x in r.get("data", [])[:30]:
            at = x.get("attributes", {})
            res.append({"t": (at.get("title") or "")[:60], "fmt": at.get("format"), "url": (at.get("link") or at.get("download_url") or "")[-80:], "date": (at.get("data_date") or at.get("modified") or "")[:10]})
        print(f"=== DANE resources {did}", flush=True)
        for x in res:
            print("  " + json.dumps(x, ensure_ascii=False), flush=True)
        end()
        out["n_res"] = (r.get("meta") or {}).get("count")
        out["res"] = res[:6]
    return out



def rawget(u, n=400000, show=4000):
    r = get(u, n, 90)
    if "err" in r:
        fail(u, r)
        return {"err": r["err"][:80]}
    b = r.pop("body")
    body = b.decode("utf-8", "replace")
    print(f"=== RAW {u}\nMETA {json.dumps({k: v for k, v in r.items() if v}, ensure_ascii=False)} read={len(b)}\nBODY {body[:show]}\n=== END", flush=True)
    return {"st": r.get("st"), "ct": r.get("ct"), "len": len(b), "hits": re.findall(r'number(?:Matched|OfFeatures)="([^"]+)"', body)[:2],
            "types": re.findall(r"<(?:wfs:)?Name>([^<]+)</", body)[:25], "head": re.sub(r"\s+", " ", body[:240])}





TERMS = r"(?i)licen[cs]|terms|conditions|copyright|reuse|re-use|redistribut|commercial|attribution|creative commons|cc by|permission|automated|bulk"

ENTRIES = {
    "GL-010": ["https://www.enterprisesurveys.org/en/citing-the-data", "https://www.enterprisesurveys.org/en/terms-of-use",
               "https://data360.worldbank.org/en/dataset/WB_ES", "https://www.worldbank.org/en/about/legal/terms-of-use-for-datasets"],
    "GL-012": ["https://www.imf.org/en/about/copyright-and-terms", "https://data.imf.org/en/Resource-Pages/IMF-API",
               "https://www.imf.org/en/Publications/WEO/weo-database/2025/october"],
    "GL-018": ["https://unstats.un.org/sdgs/dataportal", "https://unstats.un.org/sdgs/indicators/database/",
               "https://unstats.un.org/UNSDWebsite/about/terms-of-use", "https://data.un.org/Host.aspx?Content=UNdataUse"],
    "GL-019": ["https://comtradeplus.un.org/", "https://uncomtrade.org/docs/", "https://comtrade.un.org/licenseagreement.html"],
    "GL-020": ["https://stats.wto.org/", "https://www.wto.org/english/info_e/cprght_e.htm", "https://apiportal.wto.org/",
               "https://www.wto.org/english/res_e/statis_e/statis_e.htm"],
    "GL-024": ["https://www.who.int/about/policies/publishing/data-policy/terms-and-conditions", "https://data.who.int/about/data/terms-and-conditions",
               "https://www.who.int/data/gho/info/gho-odata-api"],
    "GL-027": ["https://data.unicef.org/open-data/", "https://data.unicef.org/terms-of-use/", "https://www.unicef.org/legal",
               "https://data.unicef.org/sdmx-api-documentation/"],
    "GL-030": ["https://www.migrationdataportal.org/terms-use", "https://www.migrationdataportal.org/"],
}

APIS = {
    "GL-010": ["https://data360api.worldbank.org/data360/data?DATABASE_ID=WB_ES&top=3&skip=0"],
    "GL-012": ["https://api.imf.org/external/sdmx/2.1/dataflow/all/all/latest", "https://www.imf.org/external/datamapper/api/v1/indicators"],
    "GL-018": ["https://unstats.un.org/SDGAPI/v1/sdg/Goal/List?includechildren=false"],
    "GL-020": ["https://api.wto.org/timeseries/v1/indicators?lang=1"],
    "GL-027": ["https://sdmx.data.unicef.org/ws/public/sdmxapi/rest/dataflow/all/all/latest"],
}


def terms_page(url):
    got = page(url, text=1500)
    if not got:
        return {"err": ERR[-1].split(": ", 1)[-1][:80] if ERR else "failed"}
    p, ll, t = got
    snips = around(p, TERMS, 200, 700, 8)
    for s in snips:
        print(f"  TERMS {s}", flush=True)
    tl = [(a, u) for a, u in ll if re.search(r"(?i)licen|terms|copyright|legal|conditions|download|bulk|api", a + " " + u)]
    for a, u in tl[:60]:
        print(f"  TLINK {a} | {u}", flush=True)
    end()
    return {"chars": len(p), "title": plain("".join(re.findall(r"(?is)<title>.*?</title>", t)))[:60],
            "snip": [s[150:420] for s in snips[:2]], "links": [f"{a[:25]}|{u[-60:]}" for a, u in tl[:5]]}


def main():
    res = {}
    for k, urls in ENTRIES.items():
        res[k] = {tag(u)[:70]: terms_page(u) for u in urls}
        for u in APIS.get(k, []):
            r = rawget(u, 2000000, 2500)
            res[k]["api " + tag(u)[:60]] = {x: r.get(x) for x in ("st", "ct", "len", "err") if r.get(x)}
    keys = list(res)
    for i in range(0, len(keys), 1):
        emit(f"parked4 {keys[i]}", res[keys[i]])
    emit("parked4 errors", {"requests": COUNT[0], "dead": DEAD, "errors": ERR[:25]})


if __name__ == "__main__":
    main()
