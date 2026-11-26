"""Read-only probe for the re-review of parked entries, batch 1 (Task #2913), 26 Nov 2026.

CD-001, CD-002, CD-003, CD-005, CD-008, CD-010, CD-011, CD-016, CD-032, EX-004: terms pages and routes.
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






TERMS = r"(?i)licen[cs]|terms|conditions|copyright|reuse|re-use|redistribut|commercial|attribution|creative commons|cc by|permission|automated|bulk|scrap|crawl|regulamin|prawa autorskie|wykorzyst|rozpowszechn|komercyjn|dane rynkowe|opłat|odpłatn|zgod"
FOLLOW = r"(?i)terms|licen|legal|regulamin|polityk|policy|warunki|copyright|prawa|rules|zasady|fair.?use|pricing|cennik|opłat"

ENTRIES = {
    "CD-001": ["https://marketstack.com/terms", "https://marketstack.com/pricing", "https://www.ideracorp.com/legal/APILayer"],
    "CD-002": ["https://www.alphavantage.co/terms_of_service/", "https://www.alphavantage.co/support/", "https://www.alphavantage.co/premium/"],
    "CD-003": ["https://site.financialmodelingprep.com/terms-of-service", "https://site.financialmodelingprep.com/pricing-plans",
               "https://site.financialmodelingprep.com/insights/platform/can-you-use-fmp-data-in-a-public-app-website-or-client-dashboard"],
    "CD-005": ["https://www.gpw.pl/market-data", "https://www.gpw.pl/archiwum-notowan", "https://www.gpw.pl/regulamin-korzystania-z-serwisu"],
    "CD-008": ["https://biznes.pap.pl/espi", "https://biznes.pap.pl/regulamin"],
    "CD-010": ["https://tge.pl/pub/TGE/polityka%20danych/2022_01_01_EN_Polityka_dotyczaca_danych_rynkowych.pdf", "https://tge.pl/dane-rynkowe",
               "https://tge.pl/energia-elektryczna-rdn"],
    "CD-011": ["https://gpwbenchmark.pl/dane_historyczne", "https://www.gpwbenchmark.pl/en-dokumentacja", "https://gpwbenchmark.pl/licencje"],
    "CD-016": ["https://opencorporates.com/terms-of-use/", "https://opencorporates.com/info/licence", "https://opencorporates.com/legal-information/enterprise-api-terms-of-service/",
               "https://api.opencorporates.com/documentation/API-Reference"],
    "CD-032": ["https://developer.allegro.pl/documentation", "https://developer.allegro.pl/rules", "https://developer.allegro.pl/tutorials/uwierzytelnianie-i-autoryzacja-zlq9e75GdIR"],
    "EX-004": ["https://www.epo.org/en/searching-for-patents/data/web-services/ops", "https://www.epo.org/en/service-support/ordering/terms-and-conditions/ops-terms-and-conditions",
               "https://www.epo.org/en/searching-for-patents/data/bulk-data-sets"],
}

APIS = {
    "CD-001": ["https://api.marketstack.com/v2/eod?symbols=AAPL"],
    "CD-002": ["https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey=demo"],
    "CD-016": ["https://api.opencorporates.com/v0.4/jurisdictions"],
    "CD-032": ["https://api.allegro.pl/sale/categories"],
    "EX-004": ["https://ops.epo.org/3.2/rest-services/published-data/publication/epodoc/EP1000000/biblio"],
}


def pdf_text(b):
    import io
    from pypdf import PdfReader
    rd = PdfReader(io.BytesIO(b))
    return re.sub(r"\s+", " ", " ".join((pg.extract_text() or "") for pg in rd.pages)), len(rd.pages)


def terms_page(url, depth=0, seen=None):
    seen = seen if seen is not None else set()
    seen.add(url)
    r = get(url, 6000000, 60)
    if "err" in r:
        fail(url, r)
        return {"err": r["err"][:80]}
    b = r.pop("body")
    meta = {k: v for k, v in r.items() if v}
    if b[:5] == b"%PDF-" or "pdf" in (r.get("ct") or ""):
        try:
            p, n = pdf_text(b)
        except Exception as e:
            print(f"=== PDF {url} ERR {e}\n=== END", flush=True)
            return {"st": r.get("st"), "pdf_err": str(e)[:80]}
        print(f"=== PDF {url}\nMETA {json.dumps(meta, ensure_ascii=False)} pages={n} chars={len(p)}\nTEXT {p[:30000]}\n=== END", flush=True)
        return {"st": r.get("st"), "pdf_pages": n, "chars": len(p), "lm": r.get("lm"), "snip": [s[100:360] for s in around(p, TERMS, 100, 600, 2)]}
    t = b.decode("utf-8", "replace")
    p = plain(t)
    wall = re.findall(WALL, t[:30000])[:2]
    title = plain("".join(re.findall(r"(?is)<title>.*?</title>", t)))[:90]
    print(f"=== PAGE {url}\nMETA {json.dumps(meta, ensure_ascii=False)} read={len(t)}\nTITLE {title}\nCHARS {len(p)} scripts={len(re.findall('(?i)<script', t))} wall={wall}", flush=True)
    print(f"TEXT {p[:20000]}", flush=True)
    ll = links(t, r.get("final") or url)
    tl = [(a, u) for a, u in ll if re.search(FOLLOW + r"|download|pobierz|archiw|bulk|api|\.pdf|\.xlsx?|\.csv|\.zip", a + " " + u)]
    for a, u in tl[:80]:
        print(f"  TLINK {a} | {u}", flush=True)
    end()
    out = {"st": r.get("st"), "chars": len(p), "title": title[:60], "wall": wall or None,
           "snip": [s[100:360] for s in around(p, TERMS, 100, 600, 2)]}
    if depth == 0:
        host = ".".join(urllib.parse.urlsplit(url).netloc.split(".")[-2:])
        fol = [(a, u) for a, u in ll if re.search(FOLLOW, a + " " + u) and host in urllib.parse.urlsplit(u).netloc and u not in seen]
        sub = {}
        for a, u in fol[:3]:
            sub[tag(u)[:60]] = {k: v for k, v in terms_page(u, 1, seen).items() if k in ("st", "chars", "err", "pdf_pages")}
        out["follow"] = sub
    return out


def main():
    res = {}
    seen = set()
    for k, urls in ENTRIES.items():
        res[k] = {}
        for u in urls:
            if u in seen:
                continue
            res[k][tag(u)[:60]] = terms_page(u, 0, seen)
        for u in APIS.get(k, []):
            r = rawget(u, 300000, 1500)
            res[k]["api " + tag(u)[:50]] = {x: r.get(x) for x in ("st", "ct", "len", "err", "head") if r.get(x)}
    res["EX-004"]["_errors"] = {"requests": COUNT[0], "dead": DEAD, "errors": ERR[:12]}
    for k in res:
        emit(f"parked1 {k}", res[k])




# Second pass: pages whose terms text sat behind long navigation, and links found by their anchor text.
DEEP = [
    ("EX-004", "https://www.epo.org/en/service-support/ordering/terms-and-conditions/ops-terms-and-conditions", r"(?i)linked open|bulk data|fair use", r"(?i)as such|fair use|4 GB|free|registr|charge|redistribut|own products|commercial|licen"),
    ("EX-004", "https://www.epo.org/en/searching-for-patents/data/bulk-data-sets", r"(?i)EP bibliographic data|linked open|terms", r"(?i)free of charge|charge|licen|CC BY|Creative Commons|terms|subscription|price|fee"),
    ("EX-004", "https://www.epo.org/en/searching-for-patents/data/linked-open-ep-data", r"(?i)sparql|licen|terms|download", r"(?i)CC BY|Creative Commons|licen|free|sparql|download|update|weekly"),
    ("CD-005", "https://www.gpw.pl/pub/GPW/files/PDF/dane_rynkowe/WSE_Market_Data_Policy.pdf", None, r"(?i)end of (trading|day)|historical|delayed|website|internet|private use|free of charge|non-display|statistic"),
    ("CD-005", "https://www.gpw.pl/market-data", r"(?i)^GPW statistics$|^Price archive$|legal|disclaimer", r"(?i)licen|free|statistic"),
    ("CD-011", "https://gpwbenchmark.pl/dane_historyczne", r"(?i)korzystanie z danych|stosowanie lub korzystanie|zastrzeżenia prawne|dane opóźnione", r"(?i)licencj|umow|opłat|nieodpłat|korzystan|rozpowszechn|komercyj|zgod"),
    ("CD-001", "https://www.ideracorp.com/legal/APILayer", r"(?i)master software as a service|general website terms|marketplace terms", r"(?i)redistribut|resell|sublicens|display|commercial|personal|non-commercial|data"),
]


def deep(entry, url, anchor, kw):
    r = get(url, 8000000, 60, robots_timeout=40)
    if "err" in r:
        fail(url, r)
        return {"err": r["err"][:80]}
    b = r.pop("body")
    if b[:5] == b"%PDF-":
        p, n = pdf_text(b)
        ll = []
        kind = f"pdf pages={n}"
    else:
        t = b.decode("utf-8", "replace")
        p = plain(t)
        ll = links(t, r.get("final") or url)
        kind = "html"
    print(f"=== DEEP {entry} {url}\nMETA {json.dumps({k: v for k, v in r.items() if v}, ensure_ascii=False)} {kind} chars={len(p)}", flush=True)
    for s in around(p, kw, 300, 1500, 14):
        print(f"  KW {s}", flush=True)
    if kind.startswith("pdf"):
        print(f"FULL {p[:40000]}", flush=True)
    end()
    out = {"st": r.get("st"), "chars": len(p), "kind": kind}
    if anchor:
        fol = [(a, u) for a, u in ll if re.search(anchor, a)][:4]
        out["follow"] = {}
        for a, u in fol:
            g = get(u, 8000000, 60)
            if "err" in g:
                fail(u, g)
                out["follow"][a[:30]] = g["err"][:60]
                continue
            gb = g.pop("body")
            if gb[:5] == b"%PDF-":
                q, n = pdf_text(gb)
            else:
                gt = gb.decode("utf-8", "replace")
                q = plain(gt)
                for a2, u2 in links(gt, g.get("final") or u):
                    if re.search(r"(?i)\.(pdf|xlsx?|csv|zip|xml|json)(\?|$)|sparql|download|pobierz|licen|terms", a2 + " " + u2):
                        print(f"  L2 {a2} | {u2}", flush=True)
            print(f"=== DEEP2 {entry} {a} | {u}\nMETA {json.dumps({k: v for k, v in g.items() if v}, ensure_ascii=False)} chars={len(q)}", flush=True)
            for s in around(q, kw, 300, 1500, 12):
                print(f"  KW {s}", flush=True)
            end()
            out["follow"][a[:30]] = {"u": u[-70:], "st": g.get("st"), "chars": len(q)}
    return out


def two():
    res = {}
    for entry, url, anchor, kw in DEEP:
        res.setdefault(entry, {})[tag(url)[:55]] = deep(entry, url, anchor, kw)
    res["errors"] = {"requests": COUNT[0], "dead": DEAD, "errors": ERR[:12]}
    for k in res:
        emit(f"parked1b {k}", res[k])


if __name__ == "__main__":
    import sys

    two() if sys.argv[1:] == ["two"] else main()
