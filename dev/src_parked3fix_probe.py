"""Read-only probe for the fix-first items of PR #3268 (re-review batch 3, Task #2915), 26 Nov 2026.

PL-ENV-011 GIOŚ portal download requests, PLS-024 ELA relative API paths, PL-ENV-017 dane.gov.pl 175/485,
PL-ENV-013 ZIP size, PL-ENV-006 EGiB county attribute, PLS-019 PDF text layer, PL-ENV-014 WFS attributes.
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




def bundle(url):
    r = get(url, 20000000, 90)
    if "err" in r:
        fail(url, r)
        return ""
    b = r["body"].decode("utf-8", "replace")
    print(f"=== BUNDLE {url} bytes={len(b)}\n=== END", flush=True)
    return b


def contexts(b, label, patterns, before=300, after=500, limit=5):
    out = {}
    for pat in patterns:
        found = around(b, re.escape(pat), before, after, limit)
        out[pat] = len(found)
        for s in found:
            print(f"=== CTX {label} [{pat}]\n{s}\n=== END", flush=True)
    return out


PATH_RE = r"""["'`]((?:\.{0,2}/)?[A-Za-z0-9_\-]*(?:api|rest|file|files|download|publication|publications|report|reports|export|data|assets|backend|service)[A-Za-z0-9_\-/\.{}$?=&:]*)["'`]"""


def literals(b, label):
    out = []
    for m in re.findall(PATH_RE, b, re.I):
        if 2 < len(m) < 140 and "/" in m and m not in out and not re.search(r"(?i)\.(css|svg|png|woff2?|ttf|ico)$|^data:|text/|application/|image/", m):
            out.append(m)
    print(f"=== LITERALS {label} n={len(out)}\n" + "\n".join(out[:400]) + "\n=== END", flush=True)
    return out


def auto(base_list, paths, limit):
    res, n = {}, 0
    for p in paths:
        if n >= limit:
            break
        if any(c in p for c in "{}$:") or not re.match(r"^/?[A-Za-z]", p.lstrip("./")):
            continue
        for base in base_list:
            if n >= limit:
                break
            u = urllib.parse.urljoin(base, p)
            if u in res:
                continue
            n += 1
            r = rawget(u, 400000, 1500)
            res[u] = {k: r.get(k) for k in ("st", "ct", "len", "err") if r.get(k)}
    return res


def gios():
    out = {}
    got = page("https://wody.gios.gov.pl/pjwp/", text=300)
    if not got:
        return {"err": "index failed"}
    srcs = [urllib.parse.urljoin("https://wody.gios.gov.pl/pjwp/", s) for s in re.findall(r'(?is)<script\b[^>]*src=["\']([^"\']+)', got[2])]
    print(f"=== GIOS scripts {srcs}\nHTML {got[2][:1200]}\n=== END", flush=True)
    for s in srcs:
        b = bundle(s)
        if not b:
            continue
        out["bundle"] = {"url": s[-50:], "bytes": len(b)}
        out["ctx"] = contexts(b, "gios", ["files/download", "downloadFileWithInterval", "fileLink", "fileId", "dataFormatSelected",
                                           "dataSetSelected", "/api/proxy", "/api/maps", "publication", "baseURL", "fetch(", "axios"], 400, 700, 4)
        lit = literals(b, "gios")
        out["literals"] = lit[:60]
        out["auto"] = auto(["https://wody.gios.gov.pl/", "https://wody.gios.gov.pl/pjwp/"],
                           [p for p in lit if re.search(r"(?i)api|publication|files", p)], 14)
    return out


def ela():
    out = {}
    for name, url in (("ela", "https://ela.nauka.gov.pl/pl"), ("uczen", "https://ela-uczen.nauka.gov.pl/")):
        got = page(url, text=300)
        if not got:
            out[name] = "failed"
            continue
        srcs = [urllib.parse.urljoin(url, s) for s in re.findall(r'(?is)<script\b[^>]*src=["\']([^"\']+)', got[2])]
        mains = [s for s in srcs if re.search(r"main", s) and "es5" not in s] or srcs
        print(f"=== ELA {name} scripts {srcs}\n=== END", flush=True)
        o = {}
        for s in mains[:1]:
            b = bundle(s)
            if not b:
                continue
            o["bundle"] = {"url": s[-45:], "bytes": len(b)}
            o["ctx"] = contexts(b, name, ["apiUrl", "baseUrl", "apiURL", "backendUrl", "environment", "http.get(", ".get(", "api/", "/api",
                                           "HttpClient", "assets/", ".json"], 300, 500, 4)
            lit = literals(b, name)
            o["literals"] = lit[:60]
            base = url if url.endswith("/") else url.rsplit("/", 1)[0] + "/"
            o["auto"] = auto([base], [p for p in lit if re.search(r"(?i)api|report|data|assets/.*json", p)], 10)
        out[name] = o
    return out


def gdos():
    out = {"175": dane_ds("175"), "485": dane_ds("485")}
    links = []
    for did in ("175", "485"):
        d = jget(f"https://api.dane.gov.pl/1.4/datasets/{did}/resources?per_page=30")
        links += [x.get("attributes", {}).get("link") for x in (d or {}).get("data", []) if x.get("attributes", {}).get("link")]
    print(f"=== GDOS links {links}\n=== END", flush=True)
    out["links"] = links[:20]
    seen = {}
    for u in ["http://natura2000.gdos.gov.pl/datafiles", "https://natura2000.gdos.gov.pl/datafiles/"] + links[:8]:
        if u in seen:
            continue
        if re.search(r"(?i)\.(zip|gml|shp|xml|mdb|gpkg)(\?|$)", u):
            h = get(u, method="HEAD", timeout=60)
            seen[u] = {k: h.get(k) for k in ("st", "ct", "len", "lm", "err", "final") if h.get(k)}
            print(f"=== HEAD {u}\n{seen[u]}\n=== END", flush=True)
            continue
        got = page(u, text=3000)
        if not got:
            seen[u] = ERR[-1][-80:] if ERR else "failed"
            continue
        p, ll, t = got
        files = [(a, x) for a, x in ll if re.search(r"(?i)\.(zip|gml|shp|xml|mdb|gpkg|xlsx?|csv)(\?|$)|wfs|wms|getcapabilities", a + x)]
        for a, x in files[:80]:
            print(f"  FILE {a} | {x}", flush=True)
        for a, x in ll[:80]:
            print(f"  LINK {a} | {x}", flush=True)
        end()
        heads = []
        for a, x in [f for f in files if re.search(r"(?i)\.(zip|gml|mdb|gpkg)(\?|$)", f[1])][:3]:
            h = get(x, method="HEAD", timeout=60)
            heads.append({"u": x[-80:], "st": h.get("st") or h.get("err"), "len": h.get("len"), "lm": h.get("lm")})
        seen[u] = {"chars": len(p), "files": [f"{a[:25]}|{x[-80:]}" for a, x in files[:10]], "heads": heads,
                   "lic": around(p, LIC, 80, 240, 2)}
    out["pages"] = seen
    return out


EGIB = "https://mapy.geoportal.gov.pl/wss/service/PZGIK/EGIB/WFS/UslugaZbiorcza"
CBDG = "https://cbdgmapa.pgi.gov.pl/arcgis/services/cbdg_otwory/MapServer/WFSServer"


def wfs():
    out = {}
    for t in ("ms:dzialki", "ms:budynki"):
        out["egib_dft " + t] = rawget(f"{EGIB}?service=WFS&version=2.0.0&request=DescribeFeatureType&typeNames={t}", 300000, 6000)
        out["egib_one " + t] = rawget(f"{EGIB}?service=WFS&version=2.0.0&request=GetFeature&typeNames={t}&count=1", 300000, 5000)
    caps = rawget(f"{CBDG}?service=WFS&version=2.0.0&request=GetCapabilities", 300000, 300)
    types = caps.get("types", []) if isinstance(caps, dict) else []
    out["cbdg_types"] = types
    for t in types[:3]:
        out["cbdg_dft " + t] = rawget(f"{CBDG}?service=WFS&version=2.0.0&request=DescribeFeatureType&typeNames={urllib.parse.quote(t)}", 300000, 8000)
        out["cbdg_hits " + t] = rawget(f"{CBDG}?service=WFS&version=2.0.0&request=GetFeature&typeNames={urllib.parse.quote(t)}&resultType=hits", 300000, 800)
    return out


def attrs(body):
    return re.findall(r'<(?:xsd?:)?element[^>]*name="([^"]+)"', body)[:60]


ZIP599 = "https://api.dane.gov.pl/media/resources/20231220/Geobaza_2aPGW_ver_20230915.gdb.zip"
PZH = "https://wwwold.pzh.gov.pl/oldpage/epimeld/index_p.html"


def pzh():
    out = {}
    got = page(PZH, text=500)
    if not got:
        return {"err": "index failed"}
    ch = [(a, u) for a, u in got[1] if re.search(r"/Ch_?\d{4}", u)]
    for a, u in ch:
        print(f"  CH {a} | {u}", flush=True)
    end()
    out["ch_links"] = sorted({re.search(r"Ch_?\d{4}\w*", u).group(0) for _, u in ch})
    import io
    try:
        from pypdf import PdfReader
    except ImportError as e:
        return {**out, "err": f"pypdf {e}"}
    for want in ("Ch_2024", "Ch_2005"):
        cand = [u for _, u in ch if re.search(want + r"\.pdf$", u, re.I)]
        if not cand:
            out[want] = "no link"
            continue
        r = get(cand[0], 60000000, 120)
        if "err" in r:
            out[want] = r["err"]
            continue
        b = r["body"]
        try:
            rd = PdfReader(io.BytesIO(b))
            n = len(rd.pages)
            texts = []
            woj_page = None
            for i in range(min(n, 60)):
                tx = rd.pages[i].extract_text() or ""
                texts.append(len(tx))
                if woj_page is None and re.search(r"(?i)dolnośląskie.*kujawsko", tx.replace("\n", " ")):
                    woj_page = i + 1
                    print(f"=== PZH {want} page {i + 1} voivodeship table\n{tx[:2500]}\n=== END", flush=True)
            first = (rd.pages[min(5, n - 1)].extract_text() or "")[:1500]
            print(f"=== PZH {want} url={cand[0]} bytes={len(b)} pages={n} chars/page={texts[:60]}\nPAGE6 {first}\n=== END", flush=True)
            out[want] = {"bytes": len(b), "pages": n, "text_pages": sum(1 for x in texts if x > 200), "scanned": len(texts),
                         "woj_table_page": woj_page, "lm": r.get("lm")}
        except Exception as e:
            out[want] = f"pdf {type(e).__name__}: {str(e)[:80]}"
    return out


def one():
    res = {}
    res["gios"] = gios()
    emit("fix3268 PL-ENV-011 gios", res["gios"])
    res["ela"] = ela()
    emit("fix3268 PLS-024 ela", res["ela"])
    res["gdos"] = gdos()
    emit("fix3268 PL-ENV-017 gdos", res["gdos"])
    h = get(ZIP599, method="HEAD", timeout=60)
    res["zip599"] = {k: h.get(k) for k in ("st", "ct", "len", "lm", "etag", "err", "final") if h.get(k)}
    w = wfs()
    small = {}
    for k, v in w.items():
        if isinstance(v, dict):
            small[k] = {"st": v.get("st") or v.get("err"), "len": v.get("len"), "hits": v.get("hits")}
        else:
            small[k] = v
    emit("fix3268 PL-ENV-006/014/013 wfs zip", {"zip599": res["zip599"], "wfs": small})
    res["pzh"] = pzh()
    emit("fix3268 PLS-019 pzh", res["pzh"])
    emit("fix3268 errors", {"requests": COUNT[0], "dead": DEAD, "errors": ERR[:25]})


def chunks_classic(base, runtime):
    """Angular CLI runtime: map of chunk id -> hash, names optional."""
    print(f"=== RUNTIME {base}\n{runtime[:4000]}\n=== END", flush=True)
    out = []
    for m in re.finditer(r'"-es2015\."\s*\+\s*\{([^}]*)\}', runtime):
        hashes = dict(re.findall(r'"?([\w-]+)"?:"([0-9a-f]{16,24})"', m.group(1)))
        pre = runtime[max(0, m.start() - 3000) : m.start()]
        names = dict(re.findall(r'"?(\d+)"?:"([A-Za-z][\w-]*)"', pre))
        for k, h in hashes.items():
            out.append(urllib.parse.urljoin(base, f"{names.get(k, k)}-es2015.{h}.js"))
    return out


def api_strings(text, label):
    pats = [r'apiUrl\s*\+\s*["\'`]([^"\'`]{1,120})', r'\$\{[\w.$]*(?:apiUrl|Url|url|URL)\}([^`$\s]{1,120})',
            r'apiService\.(\w+)\(\s*["\']([^"\']{1,120})["\']', r'(\w+Url)\s*[:=]\s*["\'](/[^"\']{1,120})["\']',
            r'\.get\(\s*["\'`]([^"\'`]{2,120})["\'`]', r'\.post\(\s*["\'`]([^"\'`]{2,120})["\'`]']
    out = []
    for p in pats:
        for m in re.findall(p, text):
            s = "|".join(m) if isinstance(m, tuple) else m
            if s not in out:
                out.append(s)
    print(f"=== API {label} n={len(out)}\n" + "\n".join(out[:300]) + "\n=== END", flush=True)
    return out


def ela2():
    out = {}
    base = "https://ela.nauka.gov.pl/"
    got = page(base + "pl", text=200)
    if got:
        srcs = [urllib.parse.urljoin(base, s) for s in re.findall(r'(?is)<script\b[^>]*src=["\']([^"\']+)', got[2])]
        rt = [s for s in srcs if "runtime-es2015" in s]
        texts = []
        if rt:
            urls = chunks_classic(base, bundle(rt[0]))
            out["chunks"] = len(urls)
            for u in urls[:40]:
                texts.append(bundle(u))
        texts.append(bundle([s for s in srcs if "main-es2015" in s][0]))
        allt = "\n".join(texts)
        contexts(allt, "ela2", ["apiUrl", "sla-server", "/.cdn/", "config.json", "boardUrl"], 250, 450, 6)
        found = api_strings(allt, "ela2")
        out["api"] = found[:50]
        tried = {}
        for p in ["", "config", "reports", "dictionaries"] + [f.split("|")[-1] for f in found if not re.search(r"[{}$]", f)][:10]:
            u = urllib.parse.urljoin(base + "sla-server/v6.0/", p.lstrip("/"))
            if u in tried:
                continue
            r = rawget(u, 300000, 1500)
            tried[u] = {k: r.get(k) for k in ("st", "ct", "len", "err") if r.get(k)}
        out["tried"] = tried
    ub = "https://ela-uczen.nauka.gov.pl/"
    got = page(ub, text=200)
    if got:
        srcs = [urllib.parse.urljoin(ub, s) for s in re.findall(r'(?is)<script\b[^>]*src=["\']([^"\']+)', got[2])]
        main = bundle([s for s in srcs if "main-" in s][0])
        names = sorted(set(re.findall(r'(chunk-[A-Z0-9]{6,}\.js)', main)))
        texts = [main] + [bundle(ub + n) for n in names[:40]]
        more = sorted(set(re.findall(r'(chunk-[A-Z0-9]{6,}\.js)', "\n".join(texts))) - set(names))
        texts += [bundle(ub + n) for n in more[:30]]
        allt = "\n".join(texts)
        out["uczen_chunks"] = len(names) + len(more)
        contexts(allt, "uczen2", ["apiUrl", "boardUrl", "http.get(", "sla-server", "environment"], 250, 450, 6)
        found = api_strings(allt, "uczen2")
        out["uczen_api"] = found[:50]
    return out


def gios2():
    out = {}
    b = bundle("https://wody.gios.gov.pl/pjwp/bundle-505ffd16142c797f5674.min.js")
    contexts(b, "gios2", ["ARCGIS_SERVICES_PATH", "getFinalUrl:", "fetchIndicatorsUrl", "geoFileWmsTypesDict", "downloadFile=", "/geo-files-export/download/",
                          "/publications/", "getRequest:"], 300, 600, 4)
    found = api_strings(b, "gios2")
    out["api"] = found[:80]
    tried = {}
    plain_get = [f.split("|")[1] for f in found if re.match(r"(getRequest|getPageRequest)\|", f)]
    for p in ["/publications?page=0&size=5", "/publications/367", "/publication/367"] + plain_get[:10]:
        if re.search(r"[{}$]", p):
            continue
        u = "https://wody.gios.gov.pl/pjwp" + p
        if u in tried:
            continue
        r = rawget(u, 400000, 2500)
        tried[u] = {k: r.get(k) for k in ("st", "ct", "len", "err") if r.get(k)}
    out["tried"] = tried
    svc = jget("https://wody.gios.gov.pl/arcgis/rest/services?f=json")
    out["arcgis_root"] = svc if not svc else {"folders": svc.get("folders"), "services": [s.get("name") for s in svc.get("services", [])][:30]}
    print(f"=== ARCGIS root {json.dumps(svc)[:3000]}\n=== END", flush=True)
    layer = None
    for f in (svc or {}).get("folders", [])[:6]:
        d = jget(f"https://wody.gios.gov.pl/arcgis/rest/services/{f}?f=json")
        names = [s.get("name") for s in (d or {}).get("services", [])]
        print(f"=== ARCGIS folder {f} {names}\n=== END", flush=True)
        out.setdefault("folders", {})[f] = names[:40]
        for nm in names:
            if nm.endswith("PWW_M_PPK_KLASYFIKACJA_WSK") and not layer:
                layer = f"https://wody.gios.gov.pl/arcgis/rest/services/{nm}/MapServer/0"
    if layer:
        meta = jget(layer + "?f=json")
        out["layer"] = {"url": layer, "name": (meta or {}).get("name"), "fields": [x.get("name") for x in (meta or {}).get("fields", [])][:40],
                        "maxRecordCount": (meta or {}).get("maxRecordCount")}
        c = jget(layer + "/query?where=1%3D1&returnCountOnly=true&f=json")
        out["layer"]["count"] = c
        one = rawget(layer + "/query?where=1%3D1&outFields=*&returnGeometry=false&resultRecordCount=2&f=json", 300000, 3000)
        out["layer"]["one"] = {k: one.get(k) for k in ("st", "len", "err")}
    return out


def two():
    res = {"gios": gios2()}
    emit("fix3268b PL-ENV-011 gios", res["gios"])
    res["ela"] = ela2()
    emit("fix3268b PLS-024 ela", res["ela"])
    emit("fix3268b errors", {"requests": COUNT[0], "dead": DEAD, "errors": ERR[:25]})


def gios3():
    out = {}
    b = bundle("https://wody.gios.gov.pl/pjwp/bundle-505ffd16142c797f5674.min.js")
    contexts(b, "gios3", ["root:", ".root=", "API_CONTEXT+", "getRequestWithProxy", "downloadFile=function", "/geo-files-export/download/",
                          "/terms_and_conditions", "/publications/publication/"], 400, 700, 3)
    api = "https://wody.gios.gov.pl/pjwp/api"
    tried = {}
    for p in ["/terms_and_conditions", "/publications/publication/367", "/publications?page=0&size=10", "/publications/?page=0&size=10",
              "/dictionaries/data-set-accessible", "/dictionaries/matrixes-accessible", "/dictionaries/water-area-categories-accessible",
              "/dictionaries/voivodeships", "/methodologies?page=0&size=10", "/methodologies/water-categories", "/site_map"]:
        r = rawget(api + p, 2000000, 3000)
        tried[p] = {k: r.get(k) for k in ("st", "ct", "len", "err") if r.get(k)}
    out["api"] = tried
    m = jget(api + "/methodologies?page=0&size=10")
    uu = re.findall(r'"uuid"\s*:\s*"([0-9a-f-]{20,})"', json.dumps(m)) if m else []
    if uu:
        h = get(api + "/files/download/" + uu[0], method="HEAD", timeout=60)
        out["file_head"] = {k: h.get(k) for k in ("st", "ct", "len", "cd", "err") if h.get(k)}
    for f in ("PJWP", "PJP2_ExtractDataTasks"):
        d = jget(f"https://wody.gios.gov.pl/arcgis/rest/services/{f}?f=json")
        names = [f"{s.get('name')}:{s.get('type')}" for s in (d or {}).get("services", [])]
        print(f"=== ARCGIS folder {f} {names}\n=== END", flush=True)
        out[f] = names[:30]
    lay = "https://wody.gios.gov.pl/arcgis/rest/services/PJWP/PWW_M_PPK_KLASYFIKACJA_WSK/MapServer/0"
    meta = jget(lay + "?f=json")
    if meta:
        out["ppk_layer"] = {"name": meta.get("name"), "fields": [x.get("name") for x in meta.get("fields", [])][:40], "max": meta.get("maxRecordCount")}
        out["ppk_count"] = jget(lay + "/query?where=1%3D1&returnCountOnly=true&f=json")
        rawget(lay + "/query?where=1%3D1&outFields=*&returnGeometry=false&resultRecordCount=2&f=json", 300000, 3000)
    return out


def ela3():
    out = {}
    base = "https://ela.nauka.gov.pl/"
    got = page(base + "pl", text=200)
    if not got:
        return {"err": "failed"}
    print(f"=== ELA html\n{got[2][:5000]}\n=== END", flush=True)
    srcs = [urllib.parse.urljoin(base, s) for s in re.findall(r'(?is)<script\b[^>]*src=["\']([^"\']+)', got[2])]
    rt = [s for s in srcs if "runtime-es2015" in s]
    texts = [bundle(u) for u in chunks_classic(base, bundle(rt[0]))[:40]] if rt else []
    texts.append(bundle([s for s in srcs if "main-es2015" in s][0]))
    allt = "\n".join(texts)
    contexts(allt, "ela3", ['"8ggo"', "8ggo:", "APP_INITIALIZER", "appConfig.apiUrl", "generatePdf", "TransferState", "makeStateKey", "window.__"], 500, 900, 4)
    for u in [base + ".cdn/ngrc.json", base + "sla-server/", base + "assets/config.json", base + "config.json"]:
        rawget(u, 200000, 2000)
    ub = "https://ela-uczen.nauka.gov.pl/"
    got = page(ub, text=200)
    if got:
        print(f"=== UCZEN html\n{got[2][:4000]}\n=== END", flush=True)
        srcs = [urllib.parse.urljoin(ub, s) for s in re.findall(r'(?is)<script\b[^>]*src=["\']([^"\']+)', got[2])]
        main = bundle([s for s in srcs if "main-" in s][0])
        names = sorted(set(re.findall(r'(chunk-[A-Z0-9]{6,}\.js)', main)))
        texts = [main] + [bundle(ub + n) for n in names[:40]]
        allt = "\n".join(texts)
        contexts(allt, "uczen3", ['"/filter', '"/details', "boardUrl", "apiUrl", "baseUrl", "rodzaj_rankingu"], 500, 900, 4)
    return out


def three():
    res = {"gios": gios3()}
    emit("fix3268c PL-ENV-011 gios", res["gios"])
    res["ela"] = ela3()
    emit("fix3268c errors", {"requests": COUNT[0], "dead": DEAD, "errors": ERR[:25]})


if __name__ == "__main__":
    import sys

    {"one": one, "two": two, "three": three}.get((sys.argv[1:] or ["one"])[0], one)()
