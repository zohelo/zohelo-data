"""Read-only probe for the re-review of parked register entries, batch 3, Task #2915 (26 Nov 2026).

PL-ENV-006, -011, -013, -014, -016, -017, -018, -035, PLS-016, PLS-019, PLS-024, PLS-025, PLS-030.
GET and HEAD only, one request at a time, at least a second apart per host, robots.txt honoured; a refused,
walled or timed-out host is reported and not asked again. Page text and links go to the job log; at most ten
notices carry the summary.
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


def dane(q, kind="datasets"):
    api = "https://api.dane.gov.pl/1.4"
    url = f"{api}/{kind}?q={urllib.parse.quote(q)}&per_page=15"
    r = get(url, 4000000, 75, robots_timeout=75)
    if "err" in r:
        fail(url, r)
        return {"err": r["err"][:70]}
    try:
        d = json.loads(r["body"].decode("utf-8", "replace"))
    except ValueError:
        return {"err": "not JSON"}
    rows = []
    for x in d.get("data", [])[:15]:
        at = x.get("attributes", {})
        inst = ((x.get("relationships", {}).get("institution", {}) or {}).get("data") or {}).get("id")
        row = {"id": x.get("id"), "t": (at.get("title") or "")[:70], "fmt": at.get("formats"), "lic": at.get("license_name") or at.get("license_condition_default_cc40"),
               "inst": inst, "mod": (at.get("modified") or at.get("data_date") or "")[:10], "freq": at.get("update_frequency")}
        if kind == "institutions":
            row = {"id": x.get("id"), "t": (at.get("title") or "")[:70], "n_ds": at.get("datasets_count") or at.get("published_datasets_count")}
        rows.append(row)
    print(f"=== DANE {kind} q={q} count={(d.get('meta') or {}).get('count')}", flush=True)
    for x in rows:
        print("  " + json.dumps(x, ensure_ascii=False), flush=True)
    end()
    return {"n": (d.get("meta") or {}).get("count"), "rows": rows[:5]}


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


ENTRIES = {
    "PL-ENV-006": {"pages": ["https://www.geoportal.gov.pl/pl/dane/ewidencja-gruntow-i-budynkow-egib/"],
                   "raw": ["https://mapy.geoportal.gov.pl/wss/service/PZGIK/EGIB/WFS/UslugaZbiorcza?service=WFS&request=GetCapabilities",
                           "https://mapy.geoportal.gov.pl/wss/service/PZGIK/EGIB/WFS/UslugaZbiorcza?service=WFS&version=2.0.0&request=GetFeature&typeNames=ms:dzialki&resultType=hits",
                           "https://mapy.geoportal.gov.pl/wss/service/PZGIK/EGIB/WFS/UslugaZbiorcza?service=WFS&version=2.0.0&request=GetFeature&typeNames=ms:budynki&resultType=hits"]},
    "PL-ENV-011": {"pages": ["https://wody.gios.gov.pl/pjwp/publication/367", "https://www.gov.pl/web/gios/monitoring-jakosci-wod"], "app": "https://wody.gios.gov.pl/pjwp/",
                   "dane": ["wód powierzchniowych", "monitoring wód"], "inst": ["Ochrony Środowiska"]},
    "PL-ENV-013": {"pages": ["https://isok.gov.pl/inspire.html", "https://www.gov.pl/web/wody-polskie/udostepnianie-danych-z-systemu-informacyjnego-gospodarowania-wodami"],
                   "dane": ["zagrożenia powodziowego", "Wody Polskie"], "inst": ["Wody Polskie"]},
    "PL-ENV-014": {"pages": ["https://baza.pgi.gov.pl/podsystemy/otwory"], "app": "https://dm.pgi.gov.pl/", "dane": ["otwory wiertnicze", "Państwowy Instytut Geologiczny"]},
    "PL-ENV-016": {"pages": ["https://www.pgi.gov.pl/psh/dane-hydrogeologiczne-psh/947-bazy-danych-hydrogeologiczne/8888-dane-hydrogeologiczne-mhp.html"], "dane": ["hydrogeologiczna"]},
    "PL-ENV-017": {"pages": ["https://www.gov.pl/web/gdos/dostep-do-danych-geoprzestrzennych", "https://sdi.gdos.gov.pl/wfs"], "dane": ["form ochrony przyrody", "Natura 2000"], "inst": ["Ochrony Środowiska"]},
    "PL-ENV-018": {"pages": ["https://www9.bdl.lasy.gov.pl/portal/uslugi-ogc?v=1", "https://www.bdl.lasy.gov.pl/portal/regulamin"], "dane": ["Bank Danych o Lasach", "Lasy Państwowe"]},
    "PL-ENV-035": {"pages": ["https://www.gov.pl/web/zagospodarowanieprzestrzenne/rejestr-urbanistyczny-informacje"], "app": "https://rejestr-urbanistyczny.gov.pl/published", "dane": ["rejestr urbanistyczny", "plan ogólny"]},
    "PLS-016": {"pages": ["https://ezdrowie.gov.pl/portal/home/badania-i-dane", "https://ezdrowie.gov.pl/portal/artykul/e-zdrowie-w-liczbach-e-recepty"], "dane": ["Centrum e-Zdrowia", "e-recepty"], "inst": ["e-Zdrowia"]},
    "PLS-019": {"pages": ["https://wwwold.pzh.gov.pl/oldpage/epimeld/index_p.html", "https://www.pzh.gov.pl/"], "dane": ["choroby zakaźne", "zachorowania"], "inst": ["Zdrowia Publicznego"]},
    "PLS-024": {"pages": ["https://ela.nauka.gov.pl/pl"], "app": "https://ela.nauka.gov.pl/pl", "dane": ["absolwentów", "Ekonomiczne Losy"]},
    "PLS-025": {"pages": ["https://cke.gov.pl/egzamin-osmoklasisty/wyniki/", "https://mapa.wyniki.edu.pl/", "https://stat.gov.pl/obszary-tematyczne/edukacja/edukacja/srednie-wyniki-uczniow-na-egzaminie-osmoklasisty,18,1.html"],
                "dane": ["egzaminu ósmoklasisty", "wyniki egzaminu"], "inst": ["Komisja Egzaminacyjna"],
                "raw": ["https://stat.gov.pl/download/gfx/portalinformacyjny/pl/defaultaktualnosci/5488/18/1/1/srednie_wyniki_uczniow_na_egzaminie_osmoklasisty.csv"]},
    "PLS-030": {"pages": ["https://barometrzawodow.pl/", "https://barometrzawodow.pl/en/module/forecasts-on-maps", "https://barometrzawodow.pl/forecast-card-zip/2026/"], "app": "https://barometrzawodow.pl/en/module/forecasts-on-maps", "dane": ["barometr zawodów"]},
}


def main():
    res: dict[str, dict] = {}
    for eid, spec in ENTRIES.items():
        out: dict[str, object] = {}
        try:
            for u in spec.get("pages", []):
                out[tag(u)[:60]] = probe_page(u, svc=eid.startswith("PL-ENV"))
            if spec.get("app"):
                out["app"] = app_scan(spec["app"])
            for u in spec.get("raw", []):
                r = get(u, 2000000, 90)
                if "err" in r:
                    fail(u, r)
                    out["raw " + u[-60:]] = r["err"][:80]
                    continue
                b = r.pop("body")
                body = b.decode("utf-8", "replace")
                print(f"=== RAW {u}\nMETA {json.dumps({k: v for k, v in r.items() if v}, ensure_ascii=False)} read={len(b)}\nBODY {body[:6000]}\n=== END", flush=True)
                out["raw " + u[-60:]] = {"st": r.get("st"), "ct": r.get("ct"), "len": len(b), "lm": r.get("lm"),
                                          "hits": re.findall(r'number(?:Matched|OfFeatures)="([^"]+)"', body)[:2],
                                          "types": re.findall(r"<(?:wfs:)?Name>([^<]+)</", body)[:12], "head": re.sub(r"\s+", " ", body[:300])}
            dn = {}
            for q in spec.get("dane", []):
                dn[q] = dane(q)
            for q in spec.get("inst", []):
                dn["inst:" + q] = dane(q, "institutions")
            out["dane"] = dn
            top = []
            for q, v in dn.items():
                if isinstance(v, dict) and not q.startswith("inst:"):
                    top += [x["id"] for x in v.get("rows", [])[:2] if x.get("id")]
            out["dane_ds"] = {i: dane_ds(i) for i in list(dict.fromkeys(top))[:3]}
        except Exception as e:
            ERR.append(f"{eid}: {type(e).__name__}: {str(e)[:120]}")
            print(f"=== {eid} FAILED {type(e).__name__}: {e}", flush=True)
        res[eid] = out
    ids = list(res)
    groups = [ids[0:1], ids[1:2], ids[2:4], ids[4:6], ids[6:8], ids[8:9], ids[9:10], ids[10:11], ids[11:13]]
    for g in groups:
        emit("parked " + ",".join(g), {k: res[k] for k in g})
    emit("parked robots errors totals", {"robots": {h.split("//")[1]: t[:160] for h, t in ROBOTS_TXT.items()}, "requests": COUNT[0], "dead": DEAD, "errors": ERR[:25]})


def app_scan(url, bundles=4):
    """JavaScript application: address-like strings in its own bundles (API routes, download paths)."""
    got = page(url, text=300)
    if not got:
        return {"err": "failed"}
    t = got[2]
    host = urllib.parse.urlsplit(url).netloc
    srcs = [urllib.parse.urljoin(url, html.unescape(s)) for s in re.findall(r'(?is)<script\b[^>]*src=["\']([^"\']+)', t)]
    srcs = [s for s in srcs if urllib.parse.urlsplit(s).netloc == host]
    hints: list[str] = []
    for s in sorted(srcs, key=lambda x: (not re.search(r"main|app", x), x))[:bundles]:
        r = get(s, 15000000, 60)
        if "err" in r:
            continue
        b = r["body"].decode("utf-8", "replace")
        for m in re.findall(r'["\'`]((?:\.\./|\./|/)?(?:api|rest|download|export|pobierz|files?|dane|data|assets)/[A-Za-z0-9_\-/\.{}$?=&]*)["\'`]', b):
            if m not in hints:
                hints.append(m)
        for m in re.findall(r'https?://[A-Za-z0-9\.\-]+(?:/[A-Za-z0-9_\-/\.]*)?', b):
            if m not in hints and not re.search(r"w3\.org|reactjs|github|mozilla|googleapis|schema\.org", m):
                hints.append(m)
    print(f"=== APP {url} scripts={srcs}\nHINTS {hints[:200]}\n=== END", flush=True)
    return {"scripts": [s[-60:] for s in srcs[:6]], "hints": hints[:40]}


def two():
    res: dict[str, object] = {}
    res["dane_ds"] = {i: dane_ds(i) for i in ("2585", "2621", "1776", "608", "677", "1740", "175", "599")}
    res["dane_q"] = {q: dane(q) for q in ("Centralny Rejestr Form Ochrony Przyrody", "nadleśnictw", "Barometr", "Ekonomiczne Losy Absolwentów")}
    for u in ("https://barometrzawodow.pl/modul/prognozy-w-tabelach", "https://barometrzawodow.pl/en/module/forecasts-on-tables",
              "https://barometrzawodow.pl/modul/publikacje?publication=national&year=2025", "https://barometrzawodow.pl/modul/prognozy-na-plakatach",
              "https://www9.bdl.lasy.gov.pl/portal/uslugi-ogc?v=1"):
        got = page(u, text=20000)
        if not got:
            res[tag(u)[:60]] = "failed"
            continue
        p, ll, t = got
        fl = [(a, x) for a, x in ll if re.search(r"(?i)\.(xlsx?|csv|zip|pdf|ods)(\?|$)|export|pobierz|download|wfs|ogc|api", a + " " + x)]
        for a, x in fl[:60]:
            print(f"  FILE {a} | {x}", flush=True)
        forms = re.findall(r'(?is)<form\b[^>]*action=["\']([^"\']+)', t)[:5]
        ajax = re.findall(r'data-ajax-url=["\']([^"\']+)', t)[:8]
        print(f"  FORMS {forms} AJAX {ajax}", flush=True)
        end()
        res[tag(u)[:60]] = {"chars": len(p), "files": [f"{a[:25]}|{x[-80:]}" for a, x in fl[:8]], "forms": forms, "ajax": ajax,
                            "wfs": around(p, r"WFS|OGC API", 60, 260, 3)}
    res["ela_app"] = app_scan("https://ela.nauka.gov.pl/pl", 3)
    res["ela_uczen_app"] = app_scan("https://ela-uczen.nauka.gov.pl", 3)
    keys = list(res)
    emit("parked2 dane datasets", res["dane_ds"])
    emit("parked2 dane queries", res["dane_q"])
    emit("parked2 barometr and lasy", {k: res[k] for k in keys if k not in ("dane_ds", "dane_q", "ela_app", "ela_uczen_app")})
    emit("parked2 ela", {"ela": res["ela_app"], "uczen": res["ela_uczen_app"]})
    emit("parked2 robots errors", {"requests": COUNT[0], "dead": DEAD, "errors": ERR[:25]})


if __name__ == "__main__":
    import sys

    two() if sys.argv[1:] == ["two"] else main()
